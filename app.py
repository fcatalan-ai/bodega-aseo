"""
Sistema Comercializadora B&F SpA
Ventas, compras, cotizaciones (PDF) e inventario.
Flask + PostgreSQL (Render) — usa SQLite local si no hay DATABASE_URL.
"""
import os
import io
import re
import json
import hmac
import base64
from decimal import Decimal
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from flask import (Flask, request, jsonify, session, g, render_template,
                   send_file, redirect, url_for, Response)

TZ = ZoneInfo('America/Santiago')
IVA = 0.19

DATABASE_URL = os.environ.get('DATABASE_URL', '')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
USE_PG = DATABASE_URL.startswith('postgresql')
if USE_PG:
    import psycopg2
    import psycopg2.extras
else:
    import sqlite3
SQLITE_PATH = os.environ.get('SQLITE_PATH', 'bf_local.db')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'cambia-esta-clave-en-render')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024  # respaldos grandes
APP_PIN = os.environ.get('APP_PIN', '1234')

# Documentos que llevan IVA
DOCS_VENTA = ['Factura', 'Boleta', 'Factura exenta', 'Nota de venta']
DOCS_VENTA_IVA = {'Factura', 'Boleta', 'Nota de venta'}
DOCS_COMPRA = ['Factura', 'Factura exenta', 'Boleta', 'Otro']
DOCS_COMPRA_IVA = {'Factura'}  # solo factura afecta da crédito fiscal


# ─────────────────────────── utilidades ───────────────────────────
def hoy():
    return datetime.now(TZ).date().isoformat()


def ahora():
    return datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')


class ApiError(Exception):
    def __init__(self, msg, status=400, **extra):
        super().__init__(msg)
        self.msg, self.status, self.extra = msg, status, extra


@app.errorhandler(ApiError)
def _api_error(e):
    try:
        get_db().rollback()
    except Exception:
        pass
    return jsonify(error=e.msg, **e.extra), e.status


def to_int(v, default=0):
    if v is None or v == '':
        return default
    if isinstance(v, (int, float)):
        return int(round(v))
    s = str(v).strip().replace('$', '').replace(' ', '')
    # "12.500" (miles chilenos) → 12500 ; "12,5" → 12.5
    if s.count('.') >= 1 and ',' not in s and len(s.split('.')[-1]) == 3:
        s = s.replace('.', '')
    s = s.replace(',', '.')
    try:
        return int(round(float(s)))
    except ValueError:
        return default


def to_id(v):
    try:
        return int(v) if v not in (None, '', 0, '0') else None
    except (TypeError, ValueError):
        raise ApiError('Identificador inválido')


def to_float(v, default=0.0):
    if v is None or v == '':
        return default
    try:
        return float(str(v).replace(',', '.'))
    except ValueError:
        return default


def fecha_iso(v):
    """Acepta YYYY-MM-DD o DD-MM-YYYY / DD/MM/YYYY y devuelve YYYY-MM-DD."""
    if not v:
        return hoy()
    s = str(v).strip()[:10].replace('/', '-')
    for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    raise ApiError(f'Fecha inválida: {v}')


def fecha_cl(iso):
    try:
        return datetime.strptime(str(iso)[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        return str(iso or '')


def rut_normalizar(r):
    r = (r or '').upper().replace('.', '').replace(' ', '').strip()
    if not r:
        return ''
    if '-' not in r and len(r) > 1:
        r = r[:-1] + '-' + r[-1]
    cuerpo, dv = r.split('-', 1)
    if not cuerpo.isdigit() or len(dv) != 1:
        raise ApiError('RUT con formato inválido')
    s, m = 0, 2
    for c in reversed(cuerpo):
        s += int(c) * m
        m = 2 if m == 7 else m + 1
    res = 11 - s % 11
    dvc = '0' if res == 11 else 'K' if res == 10 else str(res)
    if dv != dvc:
        raise ApiError(f'RUT inválido (el dígito verificador debería ser {dvc})')
    return f"{int(cuerpo):,}".replace(',', '.') + '-' + dv


# ─────────────────────────── base de datos ───────────────────────────
def get_db():
    if 'db' not in g:
        if USE_PG:
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            g.db = sqlite3.connect(SQLITE_PATH)
            g.db.row_factory = sqlite3.Row
            g.db.execute('PRAGMA foreign_keys=ON')
    return g.db


@app.teardown_appcontext
def _close_db(exc):
    db = g.pop('db', None)
    if db is not None:
        try:
            if exc is not None:
                db.rollback()
        finally:
            db.close()


def _sql(s):
    # Nunca se escriben '%' literales en el SQL: los patrones LIKE van como parámetro
    return s.replace('?', '%s') if USE_PG else s


def _clean(v):
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    return v


def q(sql, params=(), one=False):
    cur = get_db().cursor()
    cur.execute(_sql(sql), tuple(params))
    rows = [{k: _clean(v) for k, v in dict(r).items()} for r in cur.fetchall()]
    if one:
        return rows[0] if rows else None
    return rows


def ex(sql, params=()):
    cur = get_db().cursor()
    cur.execute(_sql(sql), tuple(params))
    return cur


def insert(sql, params=()):
    if USE_PG:
        cur = ex(sql + ' RETURNING id', params)
        return cur.fetchone()['id']
    return ex(sql, params).lastrowid


def commit():
    get_db().commit()


PK = 'SERIAL PRIMARY KEY' if USE_PG else 'INTEGER PRIMARY KEY AUTOINCREMENT'
FLT = 'DOUBLE PRECISION' if USE_PG else 'REAL'

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS config (clave TEXT PRIMARY KEY, valor TEXT);
CREATE TABLE IF NOT EXISTS productos (
  id {PK}, codigo TEXT UNIQUE NOT NULL, nombre TEXT NOT NULL, categoria TEXT DEFAULT '',
  unidad TEXT DEFAULT 'Unidad', precio_venta BIGINT DEFAULT 0, costo_promedio BIGINT DEFAULT 0,
  stock {FLT} DEFAULT 0, stock_minimo {FLT} DEFAULT 0, activo INTEGER DEFAULT 1, creado TEXT);
CREATE TABLE IF NOT EXISTS clientes (
  id {PK}, rut TEXT DEFAULT '', razon_social TEXT NOT NULL, giro TEXT DEFAULT '', direccion TEXT DEFAULT '',
  comuna TEXT DEFAULT '', ciudad TEXT DEFAULT '', email TEXT DEFAULT '', telefono TEXT DEFAULT '',
  contacto TEXT DEFAULT '', activo INTEGER DEFAULT 1, creado TEXT);
CREATE TABLE IF NOT EXISTS proveedores (
  id {PK}, rut TEXT DEFAULT '', razon_social TEXT NOT NULL, giro TEXT DEFAULT '', direccion TEXT DEFAULT '',
  comuna TEXT DEFAULT '', ciudad TEXT DEFAULT '', email TEXT DEFAULT '', telefono TEXT DEFAULT '',
  contacto TEXT DEFAULT '', activo INTEGER DEFAULT 1, creado TEXT);
CREATE TABLE IF NOT EXISTS compras (
  id {PK}, fecha TEXT NOT NULL, proveedor_id INTEGER REFERENCES proveedores(id), tipo_doc TEXT, folio TEXT DEFAULT '',
  neto BIGINT DEFAULT 0, iva BIGINT DEFAULT 0, total BIGINT DEFAULT 0, obs TEXT DEFAULT '',
  estado TEXT DEFAULT 'Vigente', pagada INTEGER DEFAULT 0, adjunto TEXT, adjunto_nombre TEXT, creado TEXT);
CREATE TABLE IF NOT EXISTS compra_items (
  id {PK}, compra_id INTEGER REFERENCES compras(id) ON DELETE CASCADE, producto_id INTEGER REFERENCES productos(id),
  descripcion TEXT, cantidad {FLT}, precio_unit BIGINT, descuento_pct {FLT} DEFAULT 0, subtotal BIGINT);
CREATE TABLE IF NOT EXISTS ventas (
  id {PK}, fecha TEXT NOT NULL, cliente_id INTEGER REFERENCES clientes(id), tipo_doc TEXT, folio TEXT DEFAULT '',
  neto BIGINT DEFAULT 0, iva BIGINT DEFAULT 0, total BIGINT DEFAULT 0, costo_total BIGINT DEFAULT 0,
  forma_pago TEXT DEFAULT '', pagada INTEGER DEFAULT 0, obs TEXT DEFAULT '', estado TEXT DEFAULT 'Vigente',
  cotizacion_id INTEGER, creado TEXT);
CREATE TABLE IF NOT EXISTS venta_items (
  id {PK}, venta_id INTEGER REFERENCES ventas(id) ON DELETE CASCADE, producto_id INTEGER REFERENCES productos(id),
  descripcion TEXT, cantidad {FLT}, precio_unit BIGINT, descuento_pct {FLT} DEFAULT 0, subtotal BIGINT,
  costo_unit BIGINT DEFAULT 0);
CREATE TABLE IF NOT EXISTS cotizaciones (
  id {PK}, numero INTEGER UNIQUE, fecha TEXT NOT NULL, validez_dias INTEGER DEFAULT 15,
  cliente_id INTEGER REFERENCES clientes(id), estado TEXT DEFAULT 'Pendiente',
  neto BIGINT DEFAULT 0, iva BIGINT DEFAULT 0, total BIGINT DEFAULT 0, obs TEXT DEFAULT '',
  condiciones TEXT DEFAULT '', venta_id INTEGER, creado TEXT);
CREATE TABLE IF NOT EXISTS cotizacion_items (
  id {PK}, cotizacion_id INTEGER REFERENCES cotizaciones(id) ON DELETE CASCADE, producto_id INTEGER REFERENCES productos(id),
  descripcion TEXT, cantidad {FLT}, precio_unit BIGINT, descuento_pct {FLT} DEFAULT 0, subtotal BIGINT);
CREATE TABLE IF NOT EXISTS movimientos (
  id {PK}, fecha_hora TEXT, producto_id INTEGER REFERENCES productos(id), tipo TEXT, cantidad {FLT},
  costo_unit BIGINT DEFAULT 0, referencia TEXT DEFAULT '', stock_resultante {FLT});
CREATE INDEX IF NOT EXISTS ix_ventas_fecha ON ventas(fecha);
CREATE INDEX IF NOT EXISTS ix_compras_fecha ON compras(fecha);
CREATE INDEX IF NOT EXISTS ix_cot_fecha ON cotizaciones(fecha);
CREATE INDEX IF NOT EXISTS ix_vi_venta ON venta_items(venta_id);
CREATE INDEX IF NOT EXISTS ix_ci_compra ON compra_items(compra_id);
CREATE INDEX IF NOT EXISTS ix_qi_cot ON cotizacion_items(cotizacion_id);
CREATE INDEX IF NOT EXISTS ix_mov_prod ON movimientos(producto_id);
CREATE TABLE IF NOT EXISTS producto_proveedor (
  id {PK}, proveedor_id INTEGER REFERENCES proveedores(id), codigo_proveedor TEXT NOT NULL,
  descripcion_proveedor TEXT DEFAULT '', producto_id INTEGER REFERENCES productos(id), factor {FLT} DEFAULT 1,
  actualizado TEXT, UNIQUE (proveedor_id, codigo_proveedor));
CREATE INDEX IF NOT EXISTS ix_pp_prod ON producto_proveedor(producto_id);
CREATE TABLE IF NOT EXISTS fletes (
  id {PK}, compra_id INTEGER REFERENCES compras(id), doc_id INTEGER REFERENCES compras(id),
  monto BIGINT DEFAULT 0, criterio TEXT DEFAULT 'valor', estado TEXT DEFAULT 'Vigente', creado TEXT);
CREATE TABLE IF NOT EXISTS flete_items (
  id {PK}, flete_id INTEGER REFERENCES fletes(id), compra_item_id INTEGER REFERENCES compra_items(id),
  producto_id INTEGER REFERENCES productos(id), monto BIGINT DEFAULT 0, aplicado BIGINT DEFAULT 0);
CREATE INDEX IF NOT EXISTS ix_fletes_compra ON fletes(compra_id);
CREATE TABLE IF NOT EXISTS pagos (
  id {PK}, tipo TEXT NOT NULL, doc_id INTEGER NOT NULL, fecha TEXT, monto BIGINT DEFAULT 0, medio TEXT DEFAULT '',
  mov_id INTEGER, origen TEXT DEFAULT 'manual', nota TEXT DEFAULT '', creado TEXT);
CREATE INDEX IF NOT EXISTS ix_pagos_doc ON pagos(tipo, doc_id);
CREATE INDEX IF NOT EXISTS ix_pagos_mov ON pagos(mov_id);
CREATE TABLE IF NOT EXISTS cuentas_banco (
  id {PK}, banco TEXT DEFAULT '', numero TEXT UNIQUE, alias TEXT DEFAULT '', titular TEXT DEFAULT '', creado TEXT);
CREATE TABLE IF NOT EXISTS cartolas (
  id {PK}, cuenta_id INTEGER REFERENCES cuentas_banco(id), numero TEXT DEFAULT '', fecha_inicio TEXT, fecha_fin TEXT,
  saldo_inicial BIGINT, saldo_final BIGINT, archivo TEXT DEFAULT '', movimientos INTEGER DEFAULT 0,
  nuevos INTEGER DEFAULT 0, creado TEXT);
CREATE TABLE IF NOT EXISTS reglas_banco (
  id {PK}, patron TEXT NOT NULL, sentido TEXT DEFAULT 'ambos', categoria TEXT NOT NULL, usos INTEGER DEFAULT 0, creado TEXT);
CREATE TABLE IF NOT EXISTS mov_banco (
  id {PK}, cuenta_id INTEGER REFERENCES cuentas_banco(id), cartola_id INTEGER REFERENCES cartolas(id), fecha TEXT,
  descripcion TEXT DEFAULT '', n_operacion TEXT DEFAULT '', sucursal TEXT DEFAULT '', monto BIGINT DEFAULT 0, saldo BIGINT,
  rut TEXT DEFAULT '', nombre TEXT DEFAULT '', huella TEXT UNIQUE, estado TEXT DEFAULT 'pendiente', categoria TEXT,
  nota TEXT DEFAULT '', regla_id INTEGER, creado TEXT);
CREATE INDEX IF NOT EXISTS ix_mov_fecha ON mov_banco(cuenta_id, fecha);
CREATE TABLE IF NOT EXISTS producto_alias (
  id {PK}, clave TEXT UNIQUE NOT NULL, producto_id INTEGER REFERENCES productos(id), actualizado TEXT);
"""

CONFIG_DEFAULT = {
    'empresa_nombre': 'Comercializadora B&F SpA',
    'empresa_rut': '',
    'empresa_giro': 'Comercializadora',
    'empresa_direccion': '',
    'empresa_ciudad': 'Temuco',
    'empresa_telefono': '',
    'empresa_email': '',
    'empresa_logo': '',
    'datos_bancarios': '',
    'cot_validez_dias': '15',
    'cot_condiciones': 'Precios netos, no incluyen IVA salvo indicación.\nForma de pago: a convenir.\nPlazo de entrega: a convenir.',
    'cot_numero_inicial': '1',
}
CONFIG_KEYS = set(CONFIG_DEFAULT)


def _agregar_columna(tabla, col, ddl):
    """Migración simple: agrega columnas nuevas a bases ya creadas."""
    if USE_PG:
        ex(f'ALTER TABLE {tabla} ADD COLUMN IF NOT EXISTS {col} {ddl}')
    elif col not in [r['name'] for r in q(f'PRAGMA table_info({tabla})')]:
        ex(f'ALTER TABLE {tabla} ADD COLUMN {col} {ddl}')


def init_db():
    db = get_db()
    cur = db.cursor()
    for stmt in [s.strip() for s in SCHEMA.split(';') if s.strip()]:
        cur.execute(stmt)
    _agregar_columna('compras', 'descuento_global', 'BIGINT DEFAULT 0')
    _agregar_columna('compras', 'adjunto_id', 'TEXT')      # public_id en Cloudinary
    _agregar_columna('compras', 'adjunto_mime', 'TEXT')
    _agregar_columna('compras', 'compra_ref_id', 'INTEGER')           # documento de flete → compra original
    _agregar_columna('compra_items', 'costo_adicional', 'BIGINT DEFAULT 0')  # flete absorbido por la línea
    for col, ddl in [('adjunto', 'TEXT'), ('adjunto_id', 'TEXT'), ('adjunto_mime', 'TEXT'), ('adjunto_nombre', 'TEXT'),
                     ('descuento_global', 'BIGINT DEFAULT 0'), ('mueve_stock', 'INTEGER DEFAULT 1')]:
        _agregar_columna('ventas', col, ddl)
    _agregar_columna('ventas', 'pagado', 'BIGINT DEFAULT 0')
    _agregar_columna('compras', 'pagado', 'BIGINT DEFAULT 0')
    # documentos marcados "pagados" antes de existir el registro de pagos → un pago por el total
    for tipo, tabla, medio in (('venta', 'ventas', "COALESCE(d.forma_pago,'')"), ('compra', 'compras', "''")):
        ex(f"INSERT INTO pagos (tipo, doc_id, fecha, monto, medio, origen, creado) SELECT '{tipo}', d.id, d.fecha, d.total, "
           f"{medio}, 'migrado', ? FROM {tabla} d WHERE d.pagada=1 AND d.estado='Vigente' AND NOT EXISTS "
           f"(SELECT 1 FROM pagos p WHERE p.tipo='{tipo}' AND p.doc_id=d.id)", (ahora(),))
        ex(f"UPDATE {tabla} SET pagado=(SELECT COALESCE(SUM(monto),0) FROM pagos p WHERE p.tipo='{tipo}' AND p.doc_id={tabla}.id)")
    if not q("SELECT clave FROM config WHERE clave='_costos_por_fecha'", one=True):
        recalcular_costos()
        ex("INSERT INTO config (clave, valor) VALUES ('_costos_por_fecha', ?)", (ahora(),))
    for k, v in CONFIG_DEFAULT.items():
        if not q('SELECT clave FROM config WHERE clave=?', (k,), one=True):
            ex('INSERT INTO config (clave, valor) VALUES (?, ?)', (k, v))
    commit()


def get_config():
    cfg = dict(CONFIG_DEFAULT)
    for r in q('SELECT clave, valor FROM config'):
        cfg[r['clave']] = r['valor'] or ''
    return cfg




def _version_estaticos():
    """Huella de los archivos de static/: cambia con cada actualización, así el navegador no usa copias viejas."""
    import hashlib
    h = hashlib.md5()
    carpeta = os.path.join(app.root_path, 'static')
    for nombre in sorted(os.listdir(carpeta)) if os.path.isdir(carpeta) else []:
        with open(os.path.join(carpeta, nombre), 'rb') as f:
            h.update(f.read())
    return h.hexdigest()[:10]


ASSET_VER = _version_estaticos()


@app.context_processor
def _ctx_version():
    return dict(asset_ver=ASSET_VER)


# ─────────────────────────── acceso con PIN ───────────────────────────
PUBLIC = {'login', 'static', 'health'}


@app.before_request
def _auth():
    if request.endpoint in PUBLIC:
        return
    if not session.get('ok'):
        if request.path.startswith('/api/') or request.path.startswith('/exportar'):
            return jsonify(error='Sesión expirada. Vuelve a ingresar el PIN.'), 401
        return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = ''
    if request.method == 'POST':
        bloqueo = session.get('bloqueo_hasta', 0)
        if bloqueo and datetime.now().timestamp() < bloqueo:
            error = 'Demasiados intentos. Espera un minuto.'
        elif hmac.compare_digest(request.form.get('pin', '').strip(), APP_PIN):
            session.clear()
            session.permanent = True
            session['ok'] = True
            return redirect(url_for('index'))
        else:
            session['fallos'] = session.get('fallos', 0) + 1
            if session['fallos'] >= 5:
                session['bloqueo_hasta'] = datetime.now().timestamp() + 60
                session['fallos'] = 0
            error = 'PIN incorrecto'
    return render_template('login.html', error=error, empresa=get_config()['empresa_nombre'])


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/health')
def health():
    return 'ok'


@app.route('/')
def index():
    return render_template('index.html', docs_venta=DOCS_VENTA, docs_compra=DOCS_COMPRA,
                           docs_venta_iva=sorted(DOCS_VENTA_IVA), docs_compra_iva=sorted(DOCS_COMPRA_IVA))


# ─────────────────────────── configuración ───────────────────────────
@app.get('/api/config')
def api_config_get():
    return jsonify(get_config())


@app.put('/api/config')
def api_config_put():
    data = request.get_json(force=True) or {}
    for k, v in data.items():
        if k not in CONFIG_KEYS:
            continue
        v = '' if v is None else str(v)
        if k == 'empresa_logo' and v:
            if not v.startswith('data:image/'):
                raise ApiError('El logo debe ser una imagen (PNG o JPG)')
            if len(v) > 700_000:
                raise ApiError('El logo es muy pesado (máx. ~500 KB)')
        if k == 'empresa_rut' and v:
            v = rut_normalizar(v)
        ex('UPDATE config SET valor=? WHERE clave=?', (v, k))
    commit()
    return jsonify(get_config())


# ─────────────────────────── productos ───────────────────────────
def next_codigo():
    rows = q('SELECT codigo FROM productos WHERE codigo LIKE ?', ('P-%',))
    mx = 0
    for r in rows:
        try:
            mx = max(mx, int(r['codigo'][2:]))
        except ValueError:
            pass
    return f'P-{mx + 1:04d}'


def mover_stock(pid, cantidad, tipo, referencia, costo_unit=0):
    """cantidad con signo (+ entra, - sale). Devuelve stock resultante."""
    p = q('SELECT stock FROM productos WHERE id=?', (pid,), one=True)
    if not p:
        raise ApiError(f'Producto {pid} no existe')
    nuevo = round((p['stock'] or 0) + cantidad, 4)
    ex('UPDATE productos SET stock=? WHERE id=?', (nuevo, pid))
    ex('INSERT INTO movimientos (fecha_hora, producto_id, tipo, cantidad, costo_unit, referencia, stock_resultante) '
       'VALUES (?,?,?,?,?,?,?)', (ahora(), pid, tipo, cantidad, costo_unit, referencia, nuevo))
    return nuevo


@app.get('/api/productos')
def api_productos():
    rows = q('SELECT * FROM productos ORDER BY nombre')
    return jsonify(rows)


def _producto_campos(d):
    nombre = (d.get('nombre') or '').strip()
    if not nombre:
        raise ApiError('El nombre del producto es obligatorio')
    return dict(nombre=nombre, categoria=(d.get('categoria') or '').strip(),
                unidad=(d.get('unidad') or 'Unidad').strip(),
                precio_venta=to_int(d.get('precio_venta')),
                stock_minimo=to_float(d.get('stock_minimo')))


@app.post('/api/productos')
def api_producto_crear():
    d = request.get_json(force=True) or {}
    c = _producto_campos(d)
    codigo = (d.get('codigo') or '').strip().upper() or next_codigo()
    if q('SELECT id FROM productos WHERE codigo=?', (codigo,), one=True):
        raise ApiError(f'Ya existe un producto con código {codigo}')
    stock_ini = to_float(d.get('stock_inicial'))
    costo = to_int(d.get('costo_promedio'))
    pid = insert('INSERT INTO productos (codigo, nombre, categoria, unidad, precio_venta, costo_promedio, stock, '
                 'stock_minimo, activo, creado) VALUES (?,?,?,?,?,?,?,?,1,?)',
                 (codigo, c['nombre'], c['categoria'], c['unidad'], c['precio_venta'], costo, 0,
                  c['stock_minimo'], ahora()))
    if stock_ini:
        mover_stock(pid, stock_ini, 'Inventario inicial', 'Creación de producto', costo)
    commit()
    return jsonify(q('SELECT * FROM productos WHERE id=?', (pid,), one=True))


@app.put('/api/productos/<int:pid>')
def api_producto_editar(pid):
    d = request.get_json(force=True) or {}
    c = _producto_campos(d)
    codigo = (d.get('codigo') or '').strip().upper()
    if not codigo:
        raise ApiError('El código es obligatorio')
    otro = q('SELECT id FROM productos WHERE codigo=? AND id<>?', (codigo, pid), one=True)
    if otro:
        raise ApiError(f'Ya existe otro producto con código {codigo}')
    sets = 'codigo=?, nombre=?, categoria=?, unidad=?, precio_venta=?, stock_minimo=?, activo=?'
    params = [codigo, c['nombre'], c['categoria'], c['unidad'], c['precio_venta'], c['stock_minimo'],
              1 if d.get('activo', True) else 0]
    if 'costo_promedio' in d and d.get('costo_promedio') not in (None, ''):
        sets += ', costo_promedio=?'
        params.append(to_int(d['costo_promedio']))
    ex(f'UPDATE productos SET {sets} WHERE id=?', params + [pid])
    commit()
    return jsonify(q('SELECT * FROM productos WHERE id=?', (pid,), one=True))


@app.delete('/api/productos/<int:pid>')
def api_producto_borrar(pid):
    usado = q('SELECT 1 AS x FROM movimientos WHERE producto_id=? UNION SELECT 1 FROM cotizacion_items WHERE producto_id=?',
              (pid, pid))
    if usado:
        ex('UPDATE productos SET activo=0 WHERE id=?', (pid,))
        commit()
        return jsonify(ok=True, desactivado=True)
    ex('DELETE FROM productos WHERE id=?', (pid,))
    commit()
    return jsonify(ok=True)


@app.post('/api/productos/<int:pid>/ajuste')
def api_producto_ajuste(pid):
    d = request.get_json(force=True) or {}
    p = q('SELECT * FROM productos WHERE id=?', (pid,), one=True)
    if not p:
        raise ApiError('Producto no existe', 404)
    nuevo = to_float(d.get('stock_nuevo'), None)
    if nuevo is None:
        raise ApiError('Indica el stock contado')
    delta = round(nuevo - (p['stock'] or 0), 4)
    if delta == 0:
        return jsonify(p)
    motivo = (d.get('motivo') or 'Ajuste de inventario').strip()
    mover_stock(pid, delta, 'Ajuste', motivo, p['costo_promedio'])
    recalcular_costos([pid])
    commit()
    return jsonify(q('SELECT * FROM productos WHERE id=?', (pid,), one=True))


# ─────────────────────────── costo promedio cronológico (kárdex) ───────────────────────────
ORDEN_EVENTO = {'inicial': 0, 'compra': 1, 'ajuste': 2, 'venta': 3}


def kardex(pid):
    """Recorre las entradas y salidas del producto ORDENADAS POR FECHA (no por orden de ingreso) y calcula el costo
    promedio ponderado vigente en cada momento. Dentro de un mismo día, las compras van antes que las ventas."""
    ev = []
    for c in q("SELECT ci.id, ci.cantidad, ci.subtotal, COALESCE(ci.costo_adicional,0) AS adic, co.id AS doc, co.fecha, "
               "co.tipo_doc, co.folio, co.neto, COALESCE(co.descuento_global,0) AS dg FROM compra_items ci "
               "JOIN compras co ON co.id=ci.compra_id WHERE ci.producto_id=? AND co.estado='Vigente'", (pid,)):
        f = c['neto'] / (c['neto'] + c['dg']) if (c['neto'] + c['dg']) else 1   # descuento global repartido
        cu = (c['subtotal'] * f + c['adic']) / c['cantidad'] if c['cantidad'] else 0
        ev.append(dict(tipo='compra', fecha=c['fecha'], orden=ORDEN_EVENTO['compra'], id=c['id'], cantidad=c['cantidad'],
                       costo=cu, doc=f'Compra #{c["doc"]} · {c["tipo_doc"]} {c["folio"] or ""}'.strip(), doc_id=c['doc']))
    for v in q("SELECT vi.id, vi.cantidad, vi.costo_unit, v.id AS doc, v.fecha, v.tipo_doc, v.folio, "
               "COALESCE(v.mueve_stock,1) AS mueve FROM venta_items vi JOIN ventas v ON v.id=vi.venta_id "
               "WHERE vi.producto_id=? AND v.estado='Vigente'", (pid,)):
        ev.append(dict(tipo='venta', fecha=v['fecha'], orden=ORDEN_EVENTO['venta'], id=v['id'], cantidad=v['cantidad'],
                       mueve=v['mueve'], costo_previo=v['costo_unit'],
                       doc=f'Venta #{v["doc"]} · {v["tipo_doc"]} {v["folio"] or ""}'.strip(), doc_id=v['doc']))
    for m in q("SELECT id, fecha_hora, tipo, cantidad, costo_unit, referencia FROM movimientos WHERE producto_id=? "
               "AND tipo IN ('Inventario inicial','Ajuste')", (pid,)):
        ini = m['tipo'] == 'Inventario inicial'
        ev.append(dict(tipo='inicial' if ini else 'ajuste', fecha='0000-00-00' if ini else (m['fecha_hora'] or '')[:10],
                       orden=ORDEN_EVENTO['inicial' if ini else 'ajuste'], id=m['id'], cantidad=m['cantidad'],
                       costo=m['costo_unit'] or 0, doc=m['tipo'] if ini else f'Ajuste · {m["referencia"] or ""}'.strip(' ·')))
    ev.sort(key=lambda e: (e['fecha'], e['orden'], e['id']))
    stock, avg, filas = 0.0, None, []
    for e in ev:
        q_ = e['cantidad'] or 0
        if e['tipo'] in ('compra', 'inicial') and q_ > 0 and (e['tipo'] == 'compra' or e['costo'] > 0):
            st = max(stock, 0)
            avg = e['costo'] if (avg is None or st <= 0) else (st * avg + q_ * e['costo']) / (st + q_)
            stock += q_
            entrada, salida, cu = q_, 0, e['costo']
        elif e['tipo'] == 'venta':
            cu = avg
            if e['mueve']:
                stock -= q_
            entrada, salida = 0, (q_ if e['mueve'] else 0)
        else:   # ajuste o inventario inicial sin costo: cambia la cantidad, no el costo
            stock += q_
            entrada, salida, cu = max(q_, 0), max(-q_, 0), avg
        filas.append(dict(fecha=e['fecha'] if e['fecha'] != '0000-00-00' else '', tipo=e['tipo'], documento=e['doc'],
                          doc_id=e.get('doc_id'), item_id=e['id'], entrada=entrada, salida=salida,
                          historica=e['tipo'] == 'venta' and not e['mueve'], costo_unit=None if cu is None else round(cu),
                          costo_previo=e.get('costo_previo'), saldo=round(stock, 4),
                          costo_promedio=None if avg is None else round(avg),
                          valor_saldo=None if avg is None else round(max(stock, 0) * avg)))
    return filas, avg


def recalcular_costos(pids=None):
    """Recalcula el costo promedio de los productos y el costo de cada venta según la fecha de los documentos.
    Si un producto no tiene compras con costo (avg desconocido), se respeta el costo ingresado a mano en la venta."""
    if pids is None:
        pids = [r['id'] for r in q('SELECT id FROM productos')]
    ventas_tocadas = set()
    for pid in {p for p in pids if p}:
        filas, avg = kardex(pid)
        for f in filas:
            if f['tipo'] == 'venta' and f['costo_unit'] is not None and f['costo_unit'] != f['costo_previo']:
                ex('UPDATE venta_items SET costo_unit=? WHERE id=?', (f['costo_unit'], f['item_id']))
                ventas_tocadas.add(f['doc_id'])
        if avg is not None:
            ex('UPDATE productos SET costo_promedio=? WHERE id=?', (int(round(avg)), pid))
    for vid in ventas_tocadas:
        ex('UPDATE ventas SET costo_total=(SELECT COALESCE(SUM(ROUND(costo_unit*cantidad)),0) FROM venta_items '
           'WHERE venta_id=?) WHERE id=?', (vid, vid))
    return len(ventas_tocadas)


def _productos_de(tabla, doc_id):
    t = 'compra_items' if tabla == 'compras' else 'venta_items'
    col = 'compra_id' if tabla == 'compras' else 'venta_id'
    return [r['producto_id'] for r in q(f'SELECT DISTINCT producto_id FROM {t} WHERE {col}=? AND producto_id IS NOT NULL', (doc_id,))]


@app.get('/api/productos/<int:pid>/kardex')
def api_producto_kardex(pid):
    filas, avg = kardex(pid)
    return jsonify(filas=filas, costo_promedio=None if avg is None else round(avg))


@app.post('/api/productos/recalcular-costos')
def api_recalcular_costos():
    n = recalcular_costos()
    commit()
    return jsonify(ventas_actualizadas=n)


@app.get('/api/productos/<int:pid>/movimientos')
def api_producto_movs(pid):
    return jsonify(q('SELECT * FROM movimientos WHERE producto_id=? ORDER BY id DESC LIMIT 300', (pid,)))


# ─────────────────────────── clientes / proveedores ───────────────────────────
ENTIDADES = {'clientes', 'proveedores'}
ENT_CAMPOS = ['rut', 'razon_social', 'giro', 'direccion', 'comuna', 'ciudad', 'email', 'telefono', 'contacto']


def _ent(tabla):
    if tabla not in ENTIDADES:
        raise ApiError('Recurso no válido', 404)
    return tabla


def _ent_campos(tabla, d, eid=None):
    vals = {k: (d.get(k) or '').strip() for k in ENT_CAMPOS}
    if not vals['razon_social']:
        raise ApiError('La razón social / nombre es obligatorio')
    vals['rut'] = rut_normalizar(vals['rut'])
    if vals['rut']:
        dup = q(f'SELECT id FROM {tabla} WHERE rut=? AND id<>?', (vals['rut'], eid or 0), one=True)
        if dup:
            raise ApiError(f'Ya existe un registro con RUT {vals["rut"]}')
    return vals


@app.get('/api/<tabla>')
def api_ent_list(tabla):
    _ent(tabla)
    return jsonify(q(f'SELECT * FROM {tabla} ORDER BY razon_social'))


@app.post('/api/<tabla>')
def api_ent_crear(tabla):
    _ent(tabla)
    v = _ent_campos(tabla, request.get_json(force=True) or {})
    eid = insert(f'INSERT INTO {tabla} ({", ".join(ENT_CAMPOS)}, activo, creado) VALUES '
                 f'({", ".join("?" * len(ENT_CAMPOS))}, 1, ?)', [v[k] for k in ENT_CAMPOS] + [ahora()])
    commit()
    return jsonify(q(f'SELECT * FROM {tabla} WHERE id=?', (eid,), one=True))


@app.put('/api/<tabla>/<int:eid>')
def api_ent_editar(tabla, eid):
    _ent(tabla)
    d = request.get_json(force=True) or {}
    v = _ent_campos(tabla, d, eid)
    sets = ', '.join(f'{k}=?' for k in ENT_CAMPOS)
    ex(f'UPDATE {tabla} SET {sets}, activo=? WHERE id=?',
       [v[k] for k in ENT_CAMPOS] + [1 if d.get('activo', True) else 0, eid])
    commit()
    return jsonify(q(f'SELECT * FROM {tabla} WHERE id=?', (eid,), one=True))


@app.delete('/api/<tabla>/<int:eid>')
def api_ent_borrar(tabla, eid):
    _ent(tabla)
    col = 'cliente_id' if tabla == 'clientes' else 'proveedor_id'
    docs = ['ventas', 'cotizaciones'] if tabla == 'clientes' else ['compras']
    if any(q(f'SELECT id FROM {t} WHERE {col}=? LIMIT 1', (eid,)) for t in docs):
        ex(f'UPDATE {tabla} SET activo=0 WHERE id=?', (eid,))
        commit()
        return jsonify(ok=True, desactivado=True)
    ex(f'DELETE FROM {tabla} WHERE id=?', (eid,))
    commit()
    return jsonify(ok=True)


# ─────────────────────────── ítems comunes ───────────────────────────
def calc_items(items, subtotal_factura=False):
    """subtotal_factura=True: respeta el subtotal que trae la factura importada (evita diferencias
    de $1 por precios con decimales o por convertir paquetes a unidades)."""
    out, neto = [], 0
    for it in items or []:
        cant = to_float(it.get('cantidad'))
        if cant <= 0:
            continue
        precio = to_int(it.get('precio_unit'))
        if precio < 0:
            raise ApiError('Los precios no pueden ser negativos')
        desc = min(max(to_float(it.get('descuento_pct')), 0), 100)
        sub = int(round(cant * precio * (1 - desc / 100)))
        if subtotal_factura and it.get('subtotal') not in (None, ''):
            dado = to_int(it.get('subtotal'))
            if abs(dado - sub) <= max(2, cant * 0.5 + 1):
                sub = dado
        pid = to_id(it.get('producto_id'))
        descr = (it.get('descripcion') or '').strip()
        if pid:
            p = q('SELECT nombre FROM productos WHERE id=?', (pid,), one=True)
            if not p:
                raise ApiError(f'Producto {pid} no existe')
            descr = descr or p['nombre']
        if not descr:
            raise ApiError('Hay una línea sin producto ni descripción')
        out.append(dict(producto_id=pid, costo_extra=max(to_int(it.get('costo_extra')), 0),
                        costo_unit=None if it.get('costo_unit') in (None, '') else max(to_int(it.get('costo_unit')), 0),
                        descripcion=descr, cantidad=cant, precio_unit=precio,
                        descuento_pct=desc, subtotal=sub))
        neto += sub
    if not out:
        raise ApiError('Agrega al menos una línea con cantidad mayor a 0')
    return out, neto


def _rango():
    desde = request.args.get('desde')
    hasta = request.args.get('hasta')
    return (fecha_iso(desde) if desde else '0000-01-01', fecha_iso(hasta) if hasta else '9999-12-31')


def _faltantes(items, excluir_venta=None):
    need = {}
    for it in items:
        if it['producto_id']:
            need[it['producto_id']] = need.get(it['producto_id'], 0) + it['cantidad']
    falt = []
    for pid, c in need.items():
        p = q('SELECT codigo, nombre, stock FROM productos WHERE id=?', (pid,), one=True)
        if (p['stock'] or 0) < c:
            falt.append(dict(codigo=p['codigo'], nombre=p['nombre'], stock=p['stock'], requerido=c))
    return falt


# ─────────────────────────── pagos (cobros de ventas / pagos de compras) ───────────────────────────
TABLA_DOC = {'venta': 'ventas', 'compra': 'compras'}
MEDIOS_PAGO = ['Transferencia', 'Efectivo', 'Cheque', 'Débito', 'Crédito', 'Depósito', 'Otro']


def recalcular_pago(tipo, doc_id):
    t = TABLA_DOC[tipo]
    pagado = q("SELECT COALESCE(SUM(monto),0) AS s FROM pagos WHERE tipo=? AND doc_id=?", (tipo, doc_id), one=True)['s']
    d = q(f'SELECT total FROM {t} WHERE id=?', (doc_id,), one=True)
    if not d:
        return
    pagada = 1 if d['total'] > 0 and pagado >= d['total'] - 1 else 0
    ex(f'UPDATE {t} SET pagado=?, pagada=? WHERE id=?', (int(pagado), pagada, doc_id))


def registrar_pago(tipo, doc_id, monto, fecha=None, medio='', mov_id=None, origen='manual', nota=''):
    monto = to_int(monto)
    if monto <= 0:
        raise ApiError('El monto del pago debe ser mayor a 0')
    pid = insert('INSERT INTO pagos (tipo, doc_id, fecha, monto, medio, mov_id, origen, nota, creado) '
                 'VALUES (?,?,?,?,?,?,?,?,?)', (tipo, doc_id, fecha_iso(fecha), monto, (medio or '')[:30], mov_id,
                                                origen, (nota or '')[:200], ahora()))
    recalcular_pago(tipo, doc_id)
    return pid


def saldo_doc(tipo, doc_id):
    d = q(f'SELECT total, pagado FROM {TABLA_DOC[tipo]} WHERE id=?', (doc_id,), one=True)
    return (d['total'] - (d['pagado'] or 0)) if d else 0


def _pagos_doc(tipo, doc_id):
    return q('SELECT p.*, m.fecha AS mov_fecha, m.descripcion AS mov_desc FROM pagos p LEFT JOIN mov_banco m ON m.id=p.mov_id '
             'WHERE p.tipo=? AND p.doc_id=? ORDER BY p.fecha, p.id', (tipo, doc_id))


def liberar_pagos(tipo, doc_id):
    """Al anular un documento: se borran sus pagos y los movimientos bancarios vinculados vuelven a 'pendiente'."""
    movs = {p['mov_id'] for p in q('SELECT mov_id FROM pagos WHERE tipo=? AND doc_id=? AND mov_id IS NOT NULL', (tipo, doc_id))}
    ex('DELETE FROM pagos WHERE tipo=? AND doc_id=?', (tipo, doc_id))
    for mid in movs:
        if not q('SELECT id FROM pagos WHERE mov_id=?', (mid,)):
            ex("UPDATE mov_banco SET estado='pendiente' WHERE id=? AND estado='conciliado'", (mid,))
    recalcular_pago(tipo, doc_id)


def marcar_pagada(tipo, doc_id, pagada, medio=''):
    """Compatibilidad con el botón 'Marcar pagada / no pagada'."""
    if pagada:
        s = saldo_doc(tipo, doc_id)
        if s > 0:
            registrar_pago(tipo, doc_id, s, hoy(), medio)
    else:
        if q('SELECT id FROM pagos WHERE tipo=? AND doc_id=? AND mov_id IS NOT NULL', (tipo, doc_id)):
            raise ApiError('Tiene pagos conciliados con el banco. Desvincúlalos en la sección Banco.')
        ex('DELETE FROM pagos WHERE tipo=? AND doc_id=?', (tipo, doc_id))
        recalcular_pago(tipo, doc_id)


@app.post('/api/<tabla>/<int:doc_id>/pagos')
def api_pago_crear(tabla, doc_id):
    tipo = {v: k for k, v in TABLA_DOC.items()}.get(tabla)
    if not tipo:
        raise ApiError('Recurso no válido', 404)
    d = request.get_json(force=True) or {}
    doc = q(f'SELECT estado FROM {tabla} WHERE id=?', (doc_id,), one=True)
    if not doc or doc['estado'] != 'Vigente':
        raise ApiError('El documento no existe o está anulado')
    if to_int(d.get('monto')) > saldo_doc(tipo, doc_id) + 1:
        raise ApiError(f'El monto supera el saldo pendiente ({saldo_doc(tipo, doc_id):,})'.replace(',', '.'))
    registrar_pago(tipo, doc_id, d.get('monto'), d.get('fecha'), d.get('medio'), nota=d.get('nota'))
    commit()
    return jsonify(pagos=_pagos_doc(tipo, doc_id), saldo=saldo_doc(tipo, doc_id))


@app.delete('/api/pagos/<int:pid>')
def api_pago_borrar(pid):
    p = q('SELECT * FROM pagos WHERE id=?', (pid,), one=True)
    if not p:
        raise ApiError('Pago no existe', 404)
    if p['mov_id']:
        raise ApiError('Este pago está conciliado con un movimiento del banco. Desvincúlalo en la sección Banco.')
    ex('DELETE FROM pagos WHERE id=?', (pid,))
    recalcular_pago(p['tipo'], p['doc_id'])
    commit()
    return jsonify(ok=True)


# ─────────────────────────── ventas ───────────────────────────
def crear_venta(d, cotizacion_id=None, subtotal_factura=False):
    """d['mover_stock']=False registra una venta histórica sin descontar inventario (su stock nunca se cargó)."""
    items, suma = calc_items(d.get('items'), subtotal_factura)
    desc_global = min(max(to_int(d.get('descuento_global')), 0), suma)
    neto = suma - desc_global
    tipo = d.get('tipo_doc') or 'Factura'
    if tipo not in DOCS_VENTA:
        raise ApiError('Tipo de documento no válido')
    cliente_id = to_id(d.get('cliente_id'))
    if tipo == 'Factura' and not cliente_id:
        raise ApiError('Una factura requiere cliente')
    folio = (d.get('folio') or '').strip()
    if folio and q("SELECT id FROM ventas WHERE tipo_doc=? AND folio=? AND estado='Vigente'", (tipo, folio), one=True):
        raise ApiError(f'Ya existe una venta con {tipo} folio {folio}')
    mueve = d.get('mover_stock', True) is not False
    if mueve:
        falt = _faltantes(items)
        if falt and not d.get('forzar'):
            raise ApiError('Stock insuficiente', 409, faltantes=falt)
    iva = int(round(neto * IVA)) if tipo in DOCS_VENTA_IVA else 0
    if iva and d.get('iva') not in (None, ''):
        iva_doc = to_int(d.get('iva'))   # IVA tal como está en la factura emitida (débito fiscal real)
        if abs(iva_doc - iva) <= max(2, neto * 0.001):
            iva = iva_doc
    fecha = fecha_iso(d.get('fecha'))
    _validar_adjunto(d.get('adjunto'))
    vid = insert('INSERT INTO ventas (fecha, cliente_id, tipo_doc, folio, neto, iva, total, costo_total, forma_pago, '
                 'pagada, obs, estado, cotizacion_id, creado, descuento_global, mueve_stock) '
                 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                 (fecha, cliente_id, tipo, folio, neto, iva, neto + iva, 0,
                  (d.get('forma_pago') or '').strip(), 1 if d.get('pagada') else 0,
                  (d.get('obs') or '').strip(), 'Vigente', cotizacion_id, ahora(), desc_global, 1 if mueve else 0))
    costo_total = 0
    ref = f'Venta #{vid} {tipo} {folio}'.strip()
    for it in items:
        costo = 0
        if it['producto_id']:
            costo = it['costo_unit'] if it['costo_unit'] is not None else \
                (q('SELECT costo_promedio FROM productos WHERE id=?', (it['producto_id'],), one=True)['costo_promedio'] or 0)
            if mueve:
                mover_stock(it['producto_id'], -it['cantidad'], 'Venta', ref, costo)
        elif it['costo_unit']:
            costo = it['costo_unit']
        costo_total += int(round(costo * it['cantidad']))
        ex('INSERT INTO venta_items (venta_id, producto_id, descripcion, cantidad, precio_unit, descuento_pct, subtotal, '
           'costo_unit) VALUES (?,?,?,?,?,?,?,?)',
           (vid, it['producto_id'], it['descripcion'], it['cantidad'], it['precio_unit'], it['descuento_pct'],
            it['subtotal'], costo))
    ex('UPDATE ventas SET costo_total=? WHERE id=?', (costo_total, vid))
    recalcular_costos(_productos_de('ventas', vid))   # costo según la fecha de la venta, no según el orden de ingreso
    if d.get('pagada'):
        ex('UPDATE ventas SET pagada=0 WHERE id=?', (vid,))
        registrar_pago('venta', vid, neto + iva, fecha, d.get('forma_pago'))
    guardar_adjunto(vid, d.get('adjunto'), d.get('adjunto_nombre'), 'ventas')
    return vid


@app.get('/api/ventas')
def api_ventas():
    desde, hasta = _rango()
    rows = q('SELECT v.*, c.razon_social AS cliente, c.rut AS cliente_rut FROM ventas v '
             'LEFT JOIN clientes c ON c.id=v.cliente_id WHERE v.fecha>=? AND v.fecha<=? ORDER BY v.fecha DESC, v.id DESC',
             (desde, hasta))
    for r in rows:
        r.pop('adjunto', None)
    return jsonify(rows)


@app.get('/api/ventas/<int:vid>')
def api_venta(vid):
    v = q('SELECT v.*, c.razon_social AS cliente, c.rut AS cliente_rut FROM ventas v '
          'LEFT JOIN clientes c ON c.id=v.cliente_id WHERE v.id=?', (vid,), one=True)
    if not v:
        raise ApiError('Venta no existe', 404)
    v.pop('adjunto', None)
    v['items'] = q('SELECT vi.*, p.codigo FROM venta_items vi LEFT JOIN productos p ON p.id=vi.producto_id '
                   'WHERE venta_id=? ORDER BY vi.id', (vid,))
    v['pagos'] = _pagos_doc('venta', vid)
    v['saldo'] = v['total'] - (v['pagado'] or 0) if v['estado'] == 'Vigente' else 0
    return jsonify(v)


@app.post('/api/ventas')
def api_venta_crear():
    vid = crear_venta(request.get_json(force=True) or {})
    commit()
    return api_venta(vid)


@app.put('/api/ventas/<int:vid>')
def api_venta_editar(vid):
    """Solo datos de cabecera; para cambiar líneas se anula y se registra de nuevo."""
    d = request.get_json(force=True) or {}
    v = q('SELECT * FROM ventas WHERE id=?', (vid,), one=True)
    if not v:
        raise ApiError('Venta no existe', 404)
    ex('UPDATE ventas SET fecha=?, folio=?, forma_pago=?, obs=?, cliente_id=? WHERE id=?',
       (fecha_iso(d.get('fecha', v['fecha'])), (d.get('folio', v['folio']) or '').strip(),
        (d.get('forma_pago', v['forma_pago']) or '').strip(),
        (d.get('obs', v['obs']) or '').strip(), to_id(d.get('cliente_id', v['cliente_id'])), vid))
    if 'pagada' in d and bool(d['pagada']) != bool(v['pagada']) and v['estado'] == 'Vigente':
        marcar_pagada('venta', vid, d['pagada'], d.get('forma_pago', v['forma_pago']))
    if fecha_iso(d.get('fecha', v['fecha'])) != v['fecha']:
        recalcular_costos(_productos_de('ventas', vid))
    commit()
    return api_venta(vid)


@app.post('/api/ventas/<int:vid>/anular')
def api_venta_anular(vid):
    v = q('SELECT * FROM ventas WHERE id=?', (vid,), one=True)
    if not v:
        raise ApiError('Venta no existe', 404)
    if v['estado'] == 'Anulada':
        raise ApiError('La venta ya está anulada')
    for it in q('SELECT * FROM venta_items WHERE venta_id=?', (vid,)):
        if it['producto_id'] and v.get('mueve_stock', 1) != 0:   # una venta histórica no descontó stock: no se devuelve
            mover_stock(it['producto_id'], it['cantidad'], 'Anulación venta', f'Venta #{vid}', it['costo_unit'])
    liberar_pagos('venta', vid)
    ex("UPDATE ventas SET estado='Anulada', pagada=0 WHERE id=?", (vid,))
    if v['cotizacion_id']:
        ex("UPDATE cotizaciones SET estado='Aceptada', venta_id=NULL WHERE id=?", (v['cotizacion_id'],))
    recalcular_costos(_productos_de('ventas', vid))
    commit()
    return api_venta(vid)


# ─────────────────────────── compras ───────────────────────────
@app.get('/api/compras')
def api_compras():
    desde, hasta = _rango()
    rows = q('SELECT co.id, co.fecha, co.proveedor_id, co.tipo_doc, co.folio, co.neto, co.iva, co.total, co.obs, '
             'co.estado, co.pagada, co.pagado, co.adjunto_nombre, co.compra_ref_id, p.razon_social AS proveedor, '
             'p.rut AS proveedor_rut FROM compras co LEFT JOIN proveedores p ON p.id=co.proveedor_id '
             'WHERE co.fecha>=? AND co.fecha<=? ORDER BY co.fecha DESC, co.id DESC', (desde, hasta))
    return jsonify(rows)


@app.get('/api/compras/<int:cid>')
def api_compra(cid):
    c = q('SELECT co.id, co.fecha, co.proveedor_id, co.tipo_doc, co.folio, co.neto, co.iva, co.total, co.obs, '
          'co.estado, co.pagada, co.pagado, co.adjunto_nombre, co.descuento_global, co.compra_ref_id, p.razon_social AS proveedor, '
          'p.rut AS proveedor_rut FROM compras co LEFT JOIN proveedores p ON p.id=co.proveedor_id WHERE co.id=?', (cid,), one=True)
    if not c:
        raise ApiError('Compra no existe', 404)
    c['items'] = q('SELECT ci.*, p.codigo FROM compra_items ci LEFT JOIN productos p ON p.id=ci.producto_id '
                   'WHERE compra_id=? ORDER BY ci.id', (cid,))
    c['pagos'] = _pagos_doc('compra', cid)
    c['saldo'] = c['total'] - (c['pagado'] or 0) if c['estado'] == 'Vigente' else 0
    c['fletes'] = q('SELECT f.id, f.doc_id, f.monto, f.criterio, f.estado, d.fecha, d.tipo_doc, d.folio, d.total, '
                    'p.razon_social AS proveedor FROM fletes f JOIN compras d ON d.id=f.doc_id '
                    'LEFT JOIN proveedores p ON p.id=d.proveedor_id WHERE f.compra_id=? ORDER BY f.id', (cid,))
    return jsonify(c)


def _validar_adjunto(adj):
    if not adj:
        return None
    if not str(adj).startswith('data:'):
        raise ApiError('Adjunto inválido')
    if len(adj) > 8_000_000:
        raise ApiError('El adjunto es muy pesado (máx. ~6 MB)')
    return adj


# ── almacenamiento de documentos: Cloudinary (privado) o, si no está configurado, dentro de la base
CLOUDINARY_FOLDER = os.environ.get('CLOUDINARY_FOLDER', 'bf/compras')
EXT_MIME = {'application/pdf': '.pdf', 'text/xml': '.xml', 'application/xml': '.xml', 'image/jpeg': '.jpg',
            'image/png': '.png', 'image/webp': '.webp', 'image/heic': '.heic'}


def cloud_ok():
    e = os.environ
    return bool(e.get('CLOUDINARY_URL') or (e.get('CLOUDINARY_CLOUD_NAME') and e.get('CLOUDINARY_API_KEY')
                                            and e.get('CLOUDINARY_API_SECRET')))


def _cld():
    import cloudinary
    import cloudinary.uploader
    import cloudinary.utils
    e = os.environ
    if e.get('CLOUDINARY_CLOUD_NAME') and not e.get('CLOUDINARY_URL'):
        cloudinary.config(cloud_name=e['CLOUDINARY_CLOUD_NAME'], api_key=e.get('CLOUDINARY_API_KEY'),
                          api_secret=e.get('CLOUDINARY_API_SECRET'), secure=True)
    return cloudinary


def _cloud_subir(cid, data, nombre, mime, tabla='compras'):
    ext = os.path.splitext(nombre or '')[1].lower()
    if not re.fullmatch(r'\.[a-z0-9]{2,5}', ext or ''):
        ext = EXT_MIME.get(mime, '')
    import uuid
    carpeta = CLOUDINARY_FOLDER if tabla == 'compras' else os.environ.get('CLOUDINARY_FOLDER_VENTAS', 'bf/ventas')
    public_id = f'{carpeta}/{tabla[:-1]}_{cid}_{datetime.now(TZ).strftime("%Y%m%d")}_{uuid.uuid4().hex[:8]}{ext}'
    try:
        r = _cld().uploader.upload(io.BytesIO(data), resource_type='raw', type='private', public_id=public_id,
                                   overwrite=True)
    except Exception as e:
        raise ApiError(f'No se pudo subir el documento a Cloudinary ({e}). Revisa las credenciales.', 502)
    return r['public_id']


def _cloud_bajar(public_id):
    import time
    import urllib.request
    url = _cld().utils.private_download_url(public_id, '', resource_type='raw', type='private',
                                            expires_at=int(time.time()) + 300)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.read()
    except Exception as e:
        raise ApiError(f'No se pudo descargar el documento desde Cloudinary ({e})', 502)


def _cloud_borrar(public_id):
    try:
        _cld().uploader.destroy(public_id, resource_type='raw', type='private', invalidate=True)
    except Exception:
        pass  # si falla solo queda un archivo huérfano en Cloudinary


TABLAS_ADJ = ('compras', 'ventas')


def guardar_adjunto(cid, data_url, nombre, tabla='compras'):
    assert tabla in TABLAS_ADJ
    adj = _validar_adjunto(data_url)
    if not adj:
        return
    nombre = (nombre or '')[:120]
    head, b64 = adj.split(',', 1)
    mime = head[5:].split(';')[0] or 'application/octet-stream'
    viejo = q(f'SELECT adjunto_id FROM {tabla} WHERE id=?', (cid,), one=True)['adjunto_id']
    if cloud_ok():
        pid = _cloud_subir(cid, base64.b64decode(b64), nombre, mime, tabla)
        ex(f'UPDATE {tabla} SET adjunto=NULL, adjunto_id=?, adjunto_mime=?, adjunto_nombre=? WHERE id=?',
           (pid, mime, nombre, cid))
    else:
        ex(f'UPDATE {tabla} SET adjunto=?, adjunto_id=NULL, adjunto_mime=?, adjunto_nombre=? WHERE id=?',
           (adj, mime, nombre, cid))
    nuevo = q(f'SELECT adjunto_id FROM {tabla} WHERE id=?', (cid,), one=True)['adjunto_id']
    if viejo and viejo != nuevo and cloud_ok():
        _cloud_borrar(viejo)


def crear_compra(d, subtotal_factura=False):
    items, suma = calc_items(d.get('items'), subtotal_factura)
    desc_global = min(max(to_int(d.get('descuento_global')), 0), suma)
    neto = suma - desc_global
    factor_desc = neto / suma if suma else 1
    tipo = d.get('tipo_doc') or 'Factura'
    if tipo not in DOCS_COMPRA:
        raise ApiError('Tipo de documento no válido')
    iva = int(round(neto * IVA)) if tipo in DOCS_COMPRA_IVA else 0
    if iva and d.get('iva') not in (None, ''):
        # IVA tal como viene en la factura (cada proveedor redondea distinto; es el crédito fiscal real)
        iva_factura = to_int(d.get('iva'))
        if abs(iva_factura - iva) <= max(2, neto * 0.001):
            iva = iva_factura
    folio = (d.get('folio') or '').strip()
    prov = to_id(d.get('proveedor_id'))
    if prov and folio and not d.get('permitir_duplicado'):
        dup = next((x for x in buscar_duplicados('compra', folio, prov) if x['nivel'] == 'exacto'), None)
        if dup:
            raise ApiError(f'Ya registraste el folio {folio} de este proveedor (compra #{dup["id"]}, {dup["tipo_doc"]} '
                           f'del {fecha_cl(dup["fecha"])})', 409, duplicado=dup)
    _validar_adjunto(d.get('adjunto'))
    cid = insert('INSERT INTO compras (fecha, proveedor_id, tipo_doc, folio, neto, iva, total, obs, estado, pagada, '
                 'creado, descuento_global) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                 (fecha_iso(d.get('fecha')), prov, tipo, folio, neto, iva, neto + iva,
                  (d.get('obs') or '').strip(), 'Vigente', 1 if d.get('pagada') else 0, ahora(), desc_global))
    ref = f'Compra #{cid} {tipo} {folio}'.strip()
    for it in items:
        if it['producto_id']:
            # costo unitario real de la línea (con descuento) → costo promedio ponderado
            # (el descuento global de la factura se reparte en proporción a cada línea)
            # + flete de la misma factura repartido en esta línea (costo_extra)
            cu = int(round((it['subtotal'] + it['costo_extra']) * factor_desc / it['cantidad'])) if it['cantidad'] \
                else it['precio_unit']
            p = q('SELECT stock, costo_promedio FROM productos WHERE id=?', (it['producto_id'],), one=True)
            st = max(p['stock'] or 0, 0)
            nuevo_costo = cu if st <= 0 else int(round((st * (p['costo_promedio'] or 0) + it['cantidad'] * cu) /
                                                         (st + it['cantidad'])))
            ex('UPDATE productos SET costo_promedio=? WHERE id=?', (nuevo_costo, it['producto_id']))
            mover_stock(it['producto_id'], it['cantidad'], 'Compra', ref, cu)
        ex('INSERT INTO compra_items (compra_id, producto_id, descripcion, cantidad, precio_unit, descuento_pct, subtotal, '
           'costo_adicional) VALUES (?,?,?,?,?,?,?,?)',
           (cid, it['producto_id'], it['descripcion'], it['cantidad'], it['precio_unit'], it['descuento_pct'],
            it['subtotal'], int(round(it['costo_extra'] * factor_desc)) if it['producto_id'] else 0))
    recalcular_costos(_productos_de('compras', cid))   # compras con fecha anterior corrigen ventas ya ingresadas
    if d.get('pagada'):
        ex('UPDATE compras SET pagada=0 WHERE id=?', (cid,))
        registrar_pago('compra', cid, neto + iva, fecha_iso(d.get('fecha')), d.get('medio_pago'))
    guardar_adjunto(cid, d.get('adjunto'), d.get('adjunto_nombre'))  # al final: si algo falla antes, no se sube
    return cid


@app.post('/api/compras')
def api_compra_crear():
    cid = crear_compra(request.get_json(force=True) or {})
    commit()
    return api_compra(cid)


@app.put('/api/compras/<int:cid>')
def api_compra_editar(cid):
    d = request.get_json(force=True) or {}
    c = q('SELECT id, fecha, folio, obs, pagada, proveedor_id FROM compras WHERE id=?', (cid,), one=True)
    if not c:
        raise ApiError('Compra no existe', 404)
    ex('UPDATE compras SET fecha=?, folio=?, obs=?, proveedor_id=? WHERE id=?',
       (fecha_iso(d.get('fecha', c['fecha'])), (d.get('folio', c['folio']) or '').strip(),
        (d.get('obs', c['obs']) or '').strip(), to_id(d.get('proveedor_id', c['proveedor_id'])), cid))
    if 'pagada' in d and bool(d['pagada']) != bool(c['pagada']):
        marcar_pagada('compra', cid, d['pagada'])
    if d.get('adjunto'):
        guardar_adjunto(cid, d['adjunto'], d.get('adjunto_nombre'))
    if fecha_iso(d.get('fecha', c['fecha'])) != c['fecha']:
        ref = q('SELECT compra_ref_id FROM compras WHERE id=?', (cid,), one=True)['compra_ref_id']
        recalcular_costos(_productos_de('compras', ref or cid))
    commit()
    return api_compra(cid)


@app.post('/api/compras/<int:cid>/anular')
def api_compra_anular(cid):
    c = q('SELECT id, estado, compra_ref_id FROM compras WHERE id=?', (cid,), one=True)
    if not c:
        raise ApiError('Compra no existe', 404)
    if c['estado'] == 'Anulada':
        raise ApiError('La compra ya está anulada')
    if q("SELECT id FROM fletes WHERE compra_id=? AND estado='Vigente'", (cid,)):
        raise ApiError('Esta compra tiene un flete asociado. Anula primero el documento del flete.')
    if c['compra_ref_id']:
        revertir_flete(cid)
    for it in q('SELECT * FROM compra_items WHERE compra_id=?', (cid,)):
        if it['producto_id']:
            mover_stock(it['producto_id'], -it['cantidad'], 'Anulación compra', f'Compra #{cid}')
    liberar_pagos('compra', cid)
    ex("UPDATE compras SET estado='Anulada', pagada=0 WHERE id=?", (cid,))
    recalcular_costos(_productos_de('compras', c['compra_ref_id'] or cid))
    commit()
    return api_compra(cid)


@app.get('/api/<tabla>/<int:cid>/adjunto')
def api_compra_adjunto(tabla, cid):
    if tabla not in TABLAS_ADJ:
        raise ApiError('Recurso no válido', 404)
    c = q(f'SELECT adjunto, adjunto_id, adjunto_mime, adjunto_nombre FROM {tabla} WHERE id=?', (cid,), one=True)
    if not c or not (c['adjunto'] or c['adjunto_id']):
        raise ApiError('Sin adjunto', 404)
    if c['adjunto_id']:
        data, mime = _cloud_bajar(c['adjunto_id']), c['adjunto_mime'] or 'application/octet-stream'
    else:
        head, b64 = c['adjunto'].split(',', 1)
        data, mime = base64.b64decode(b64), head[5:].split(';')[0] or 'application/octet-stream'
    return send_file(io.BytesIO(data), mimetype=mime, download_name=c['adjunto_nombre'] or f'{tabla[:-1]}_{cid}',
                     as_attachment=request.args.get('descargar') == '1')


# ─────────────────────────── costos de flete ───────────────────────────
CRITERIOS_FLETE = {'valor': 'por valor', 'cantidad': 'por cantidad', 'producto': 'a un producto'}


def repartir(monto, bases, criterio='valor', destino=None):
    """Reparte un monto entero entre líneas. bases: [(valor, cantidad)]. Devuelve montos enteros que suman exacto."""
    n = len(bases)
    if not n or monto <= 0:
        return [0] * n
    if criterio == 'producto':
        if destino is None or not 0 <= destino < n:
            raise ApiError('Elige el producto al que corresponde el flete')
        pesos = [1 if i == destino else 0 for i in range(n)]
    elif criterio == 'cantidad':
        pesos = [max(b[1], 0) for b in bases]
    else:
        pesos = [max(b[0], 0) for b in bases]
    tot = sum(pesos)
    if tot <= 0:
        pesos, tot = [1] * n, n
    crudo = [monto * p / tot for p in pesos]
    res = [int(x) for x in crudo]
    for i in sorted(range(n), key=lambda i: crudo[i] - res[i], reverse=True)[:monto - sum(res)]:
        res[i] += 1  # método del resto mayor: la suma queda exacta
    return res


def aplicar_flete(compra_id, doc_id, monto, criterio, destino_item_id=None):
    """Suma un costo posterior (flete) al costo promedio de los productos de una compra."""
    lineas = q('SELECT id, producto_id, cantidad, subtotal FROM compra_items WHERE compra_id=? AND producto_id IS NOT NULL '
               'ORDER BY id', (compra_id,))
    if not lineas:
        raise ApiError('La compra no tiene productos de inventario donde repartir el flete')
    destino = next((i for i, l in enumerate(lineas) if l['id'] == to_id(destino_item_id)), None)
    montos = repartir(monto, [(l['subtotal'], l['cantidad']) for l in lineas], criterio, destino)
    fid = insert('INSERT INTO fletes (compra_id, doc_id, monto, criterio, estado, creado) VALUES (?,?,?,?,?,?)',
                 (compra_id, doc_id, monto, criterio, 'Vigente', ahora()))
    ref = f'Flete compra #{compra_id} (doc #{doc_id})'
    for l, a in zip(lineas, montos):
        if not a:
            continue
        p = q('SELECT stock, costo_promedio FROM productos WHERE id=?', (l['producto_id'],), one=True)
        stock = max(p['stock'] or 0, 0)
        # solo las unidades de esta compra que siguen en bodega absorben el flete; lo ya vendido no se recalcula
        aplicado = int(round(a / l['cantidad'] * min(stock, l['cantidad']))) if l['cantidad'] else 0
        if stock > 0 and aplicado:
            nuevo = int(round(((p['costo_promedio'] or 0) * stock + aplicado) / stock))
            ex('UPDATE productos SET costo_promedio=? WHERE id=?', (nuevo, l['producto_id']))
            ex('INSERT INTO movimientos (fecha_hora, producto_id, tipo, cantidad, costo_unit, referencia, stock_resultante) '
               'VALUES (?,?,?,?,?,?,?)', (ahora(), l['producto_id'], 'Costo flete', 0, nuevo, ref, p['stock']))
        ex('UPDATE compra_items SET costo_adicional=COALESCE(costo_adicional,0)+? WHERE id=?', (a, l['id']))
        ex('INSERT INTO flete_items (flete_id, compra_item_id, producto_id, monto, aplicado) VALUES (?,?,?,?,?)',
           (fid, l['id'], l['producto_id'], a, aplicado))
    return fid


def revertir_flete(doc_id):
    for f in q("SELECT id, compra_id FROM fletes WHERE doc_id=? AND estado='Vigente'", (doc_id,)):
        for fi in q('SELECT * FROM flete_items WHERE flete_id=?', (f['id'],)):
            p = q('SELECT stock, costo_promedio FROM productos WHERE id=?', (fi['producto_id'],), one=True)
            stock = max(p['stock'] or 0, 0)
            if stock > 0 and fi['aplicado']:
                nuevo = max(int(round(((p['costo_promedio'] or 0) * stock - fi['aplicado']) / stock)), 0)
                ex('UPDATE productos SET costo_promedio=? WHERE id=?', (nuevo, fi['producto_id']))
                ex('INSERT INTO movimientos (fecha_hora, producto_id, tipo, cantidad, costo_unit, referencia, '
                   'stock_resultante) VALUES (?,?,?,?,?,?,?)', (ahora(), fi['producto_id'], 'Anulación flete', 0, nuevo,
                                                                f'Doc #{doc_id}', p['stock']))
            ex('UPDATE compra_items SET costo_adicional=COALESCE(costo_adicional,0)-? WHERE id=?',
               (fi['monto'], fi['compra_item_id']))
        ex("UPDATE fletes SET estado='Anulado' WHERE id=?", (f['id'],))


@app.post('/api/compras/<int:cid>/flete')
def api_compra_flete(cid):
    """Flete en un documento aparte (transportista, boleta, o que llega después): se registra como compra propia,
    vinculada, y su costo se reparte en los productos de la compra original."""
    d = request.get_json(force=True) or {}
    c = q('SELECT id, estado, compra_ref_id, tipo_doc, folio FROM compras WHERE id=?', (cid,), one=True)
    if not c:
        raise ApiError('Compra no existe', 404)
    if c['estado'] != 'Vigente':
        raise ApiError('La compra está anulada')
    if c['compra_ref_id']:
        raise ApiError('Este documento ya es un flete')
    tipo = d.get('tipo_doc') or 'Factura'
    if tipo not in DOCS_COMPRA:
        raise ApiError('Tipo de documento no válido')
    monto = to_int(d.get('monto'))  # neto si es factura afecta; total si es boleta u otro
    if monto <= 0:
        raise ApiError('Indica el monto del flete')
    criterio = d.get('criterio') if d.get('criterio') in CRITERIOS_FLETE else 'valor'
    prov = to_id(d.get('proveedor_id'))
    doc_id = crear_compra(dict(fecha=d.get('fecha'), proveedor_id=prov, tipo_doc=tipo, folio=d.get('folio'),
                               obs=(d.get('obs') or '').strip() or f'Flete de la compra #{cid}', pagada=d.get('pagada'),
                               iva=d.get('iva'), adjunto=d.get('adjunto'), adjunto_nombre=d.get('adjunto_nombre'),
                               items=[dict(descripcion=(d.get('descripcion') or '').strip() or
                                           f'Flete compra #{cid} {c["tipo_doc"]} {c["folio"] or ""}'.strip(),
                                           cantidad=1, precio_unit=monto)]))
    ex('UPDATE compras SET compra_ref_id=? WHERE id=?', (cid, doc_id))
    # al costo va el neto (el IVA de una factura es crédito fiscal); en boleta el total, porque ese IVA no se recupera
    aplicar_flete(cid, doc_id, monto, criterio, d.get('destino_item_id'))
    recalcular_costos(_productos_de('compras', cid))
    commit()
    return api_compra(cid)


# ─────────────────────────── detección de documentos duplicados ───────────────────────────
def _folio_n(f):
    f = re.sub(r'\D', '', str(f or ''))
    return f.lstrip('0') or ('0' if f else '')


def buscar_duplicados(tipo, folio, entidad_id=None, rut='', nombre='', total=None):
    """Documentos vigentes con el mismo folio.
    nivel 'exacto': mismo proveedor/cliente (por id o RUT) — es el mismo documento.
    nivel 'probable': mismo folio y mismo total, o nombre muy parecido (p. ej. proveedor creado sin RUT)."""
    fn = _folio_n(folio)
    if not fn:
        return []
    t, ent, ent_t = ('compras', 'proveedor_id', 'proveedores') if tipo == 'compra' else ('ventas', 'cliente_id', 'clientes')
    filas = q(f"SELECT d.id, d.fecha, d.tipo_doc, d.folio, d.total, d.estado, d.{ent} AS ent_id, e.razon_social AS entidad, "
              f"e.rut FROM {t} d LEFT JOIN {ent_t} e ON e.id=d.{ent} WHERE d.estado='Vigente' AND d.folio<>'' "
              f"AND d.folio LIKE ?", ('%' + fn,))
    out = []
    for f in filas:
        if _folio_n(f['folio']) != fn:
            continue
        mismo = (entidad_id and f['ent_id'] == entidad_id) or (rut and f['rut'] and _rut_l(f['rut']) == _rut_l(rut))
        parecido = bool(nombre and f['entidad'] and sim_nombre(nombre, f['entidad']) >= 0.6)
        mismo_total = total is not None and total and abs((f['total'] or 0) - total) <= 2
        if mismo:
            nivel = 'exacto'
        elif parecido or mismo_total:
            nivel = 'probable'
        elif tipo == 'venta':
            nivel = 'exacto'   # el folio de venta es de la propia empresa: no se repite entre clientes
        else:
            continue           # otro proveedor con el mismo número de factura: es normal
        f['nivel'] = nivel
        f['motivo'] = ('mismo ' + ('proveedor' if tipo == 'compra' else 'cliente') if mismo else
                       'mismo folio y mismo total' if mismo_total else 'nombre parecido' if parecido else 'mismo folio')
        out.append(f)
    out.sort(key=lambda x: x['nivel'] != 'exacto')
    return out


@app.get('/api/<tabla>/buscar-folio')
def api_buscar_folio(tabla):
    tipo = {'compras': 'compra', 'ventas': 'venta'}.get(tabla)
    if not tipo:
        raise ApiError('Recurso no válido', 404)
    a = request.args
    ent = to_id(a.get('proveedor_id') or a.get('cliente_id'))
    e = q(f'SELECT rut, razon_social FROM {"proveedores" if tipo == "compra" else "clientes"} WHERE id=?', (ent,), one=True) if ent else None
    return jsonify(buscar_duplicados(tipo, a.get('folio'), ent, e['rut'] if e else '', e['razon_social'] if e else '',
                                     to_int(a.get('total')) if a.get('total') else None))


# ─────────────────────────── importar factura de compra ───────────────────────────
RUT_EMPRESA_DEFECTO = '77.344.248-7'


def _rut_empresa():
    return get_config().get('empresa_rut') or RUT_EMPRESA_DEFECTO


def _clave_proveedor(codigo, descripcion):
    """Código del proveedor; si la factura no trae código, se usa la descripción normalizada."""
    from lector_facturas import norm
    codigo = (codigo or '').strip().upper()
    return codigo if codigo else 'D:' + norm(descripcion)[:100]


def _similitud(a, b):
    from lector_facturas import norm
    ta = {w for w in re.findall(r'[a-z0-9]+', norm(a)) if len(w) >= 2}
    tb = {w for w in re.findall(r'[a-z0-9]+', norm(b)) if len(w) >= 2}
    return len(ta & tb) / len(ta | tb) if ta and tb else 0


def _buscar_proveedor_rut(rut):
    try:
        rut_n = rut_normalizar(rut)
    except ApiError:
        return None
    return q('SELECT * FROM proveedores WHERE rut=?', (rut_n,), one=True) if rut_n else None


@app.post('/api/compras/leer-factura')
def api_leer_factura():
    from lector_facturas import leer_factura, LecturaError
    d = request.get_json(force=True) or {}
    archivo = d.get('archivo') or ''
    if ',' not in archivo:
        raise ApiError('Adjunta el PDF o XML de la factura')
    try:
        data = base64.b64decode(archivo.split(',', 1)[1])
    except Exception:
        raise ApiError('No se pudo leer el archivo')
    try:
        r = leer_factura(data, d.get('nombre') or '', _rut_empresa())
    except LecturaError as e:
        raise ApiError(str(e), 422)
    prov = _buscar_proveedor_rut(r['emisor']['rut'])
    r['proveedor_id'] = prov['id'] if prov else None
    r['proveedor_nombre'] = prov['razon_social'] if prov else ''
    r['duplicados'] = buscar_duplicados('compra', r['folio'], prov['id'] if prov else None, r['emisor']['rut'],
                                        r['emisor']['razon_social'], r['total'])
    r['duplicada'] = any(x['nivel'] == 'exacto' for x in r['duplicados'])
    productos = q('SELECT id, codigo, nombre FROM productos WHERE activo=1')
    for it in r['items']:
        it['clave'] = _clave_proveedor(it['codigo'], it['descripcion'])
        sug = None
        if prov:
            m = q('SELECT pp.producto_id, pp.factor FROM producto_proveedor pp JOIN productos p ON p.id=pp.producto_id '
                  'WHERE pp.proveedor_id=? AND pp.codigo_proveedor=? AND p.activo=1', (prov['id'], it['clave']), one=True)
            if m:
                sug = dict(producto_id=m['producto_id'], factor=m['factor'] or 1, via='equivalencia', score=1)
        if not sug and it['codigo']:
            p = next((p for p in productos if p['codigo'].upper() == it['codigo'].upper()), None)
            if p:
                sug = dict(producto_id=p['id'], factor=1, via='codigo', score=1)
        if not sug and productos:
            mejor = max(productos, key=lambda p: _similitud(it['descripcion'], p['nombre']))
            sc = _similitud(it['descripcion'], mejor['nombre'])
            if sc >= 0.5:
                sug = dict(producto_id=mejor['id'], factor=1, via='nombre', score=round(sc, 2))
        it['sugerido'] = sug
    return jsonify(r)


@app.post('/api/compras/importar')
def api_importar_compra():
    d = request.get_json(force=True) or {}
    # proveedor: existente o se crea con los datos de la factura
    prov_id = to_id(d.get('proveedor_id'))
    if not prov_id:
        pdat = d.get('proveedor') or {}
        existente = _buscar_proveedor_rut(pdat.get('rut'))
        if existente:
            prov_id = existente['id']
        else:
            nombre = (pdat.get('razon_social') or '').strip()
            if not nombre:
                raise ApiError('Falta el nombre del proveedor')
            prov_id = insert('INSERT INTO proveedores (rut, razon_social, giro, direccion, comuna, ciudad, email, '
                             'telefono, contacto, activo, creado) VALUES (?,?,?,?,?,?,?,?,?,1,?)',
                             (rut_normalizar(pdat.get('rut')), nombre, (pdat.get('giro') or '')[:120], '', '', '',
                              '', '', '', ahora()))
    items, fletes, idx_map = [], [], {}
    for n_it, it in enumerate(d.get('items') or []):
        accion = it.get('accion') or 'libre'
        if accion == 'omitir':
            continue
        cant = to_float(it.get('cantidad'))
        if cant <= 0:
            continue
        factor = to_float(it.get('factor'), 1) or 1
        if factor <= 0:
            raise ApiError('Las unidades por paquete deben ser mayores a 0')
        descripcion = (it.get('descripcion') or '').strip()
        pid = None
        if accion == 'nuevo':
            nv = it.get('nuevo') or {}
            nombre = (nv.get('nombre') or descripcion).strip()
            if not nombre:
                raise ApiError('Un producto nuevo no tiene nombre')
            pid = insert('INSERT INTO productos (codigo, nombre, categoria, unidad, precio_venta, costo_promedio, stock, '
                         'stock_minimo, activo, creado) VALUES (?,?,?,?,?,?,?,?,1,?)',
                         (next_codigo(), nombre[:150], (nv.get('categoria') or '').strip(),
                          (nv.get('unidad') or 'Unidad').strip(), to_int(nv.get('precio_venta')), 0, 0,
                          to_float(nv.get('stock_minimo')), ahora()))
        elif accion == 'existente':
            pid = to_id(it.get('producto_id'))
            if not pid or not q('SELECT id FROM productos WHERE id=?', (pid,), one=True):
                raise ApiError(f'Elige el producto para: {descripcion[:60]}')
        if pid:
            clave = _clave_proveedor(it.get('codigo'), descripcion)
            prev = q('SELECT id FROM producto_proveedor WHERE proveedor_id=? AND codigo_proveedor=?', (prov_id, clave),
                     one=True)
            if prev:
                ex('UPDATE producto_proveedor SET producto_id=?, factor=?, descripcion_proveedor=?, actualizado=? '
                   'WHERE id=?', (pid, factor, descripcion[:200], ahora(), prev['id']))
            else:
                ex('INSERT INTO producto_proveedor (proveedor_id, codigo_proveedor, descripcion_proveedor, producto_id, '
                   'factor, actualizado) VALUES (?,?,?,?,?,?)', (prov_id, clave, descripcion[:200], pid, factor, ahora()))
        # conversión paquete → unidades de stock
        if pid and factor != 1:
            unidad = (it.get('unidad') or '').strip()
            nota = f' ({cant:g} {unidad} x {factor:g})'.replace('  ', ' ')
            cant_stock = cant * factor
            precio = to_float(it.get('precio_unit')) / factor
        else:
            nota, cant_stock, precio = '', cant, to_float(it.get('precio_unit'))
        idx_map[n_it] = len(items)
        if accion == 'flete':
            fletes.append(len(items))
            descripcion = descripcion or 'Flete'
        items.append(dict(producto_id=pid, descripcion=(descripcion + nota)[:250], cantidad=round(cant_stock, 4),
                          precio_unit=precio, subtotal=it.get('subtotal'),
                          descuento_pct=min(max(to_float(it.get('descuento_pct')), 0), 100)))
    if not items:
        raise ApiError('No hay líneas para registrar')
    if fletes:
        # el flete de la misma factura se reparte en el costo de los productos (la línea queda en la compra sin stock)
        monto_flete = sum(to_int(items[i]['subtotal']) if items[i]['subtotal'] not in (None, '')
                          else int(round(items[i]['cantidad'] * items[i]['precio_unit'])) for i in fletes)
        stock_idx = [i for i, x in enumerate(items) if x['producto_id']]
        if not stock_idx:
            raise ApiError('Hay un flete pero ningún producto de inventario donde repartirlo')
        destino = idx_map.get(to_int(d.get('flete_destino'), -1) if d.get('flete_destino') not in (None, '') else -1)
        destino = stock_idx.index(destino) if destino in stock_idx else None
        bases = [(to_int(items[i]['subtotal']) if items[i]['subtotal'] not in (None, '')
                  else items[i]['cantidad'] * items[i]['precio_unit'], items[i]['cantidad']) for i in stock_idx]
        crit = d.get('flete_criterio') if d.get('flete_criterio') in CRITERIOS_FLETE else 'valor'
        for i, a in zip(stock_idx, repartir(monto_flete, bases, crit, destino)):
            items[i]['costo_extra'] = a
    cid = crear_compra(dict(fecha=d.get('fecha'), proveedor_id=prov_id, tipo_doc=d.get('tipo_doc') or 'Factura',
                            permitir_duplicado=d.get('permitir_duplicado'),
                            folio=d.get('folio'), obs=d.get('obs'), pagada=d.get('pagada'),
                            descuento_global=d.get('descuento_global'), iva=d.get('iva'), adjunto=d.get('adjunto'),
                            adjunto_nombre=d.get('adjunto_nombre'), items=items), subtotal_factura=True)
    commit()
    return api_compra(cid)


@app.get('/api/proveedores/<int:pid>/equivalencias')
def api_equivalencias(pid):
    return jsonify(q('SELECT pp.*, p.codigo, p.nombre FROM producto_proveedor pp JOIN productos p ON p.id=pp.producto_id '
                     'WHERE pp.proveedor_id=? ORDER BY pp.descripcion_proveedor', (pid,)))


@app.delete('/api/equivalencias/<int:eid>')
def api_equivalencia_borrar(eid):
    ex('DELETE FROM producto_proveedor WHERE id=?', (eid,))
    commit()
    return jsonify(ok=True)


# ─────────────────────────── importar factura de venta ───────────────────────────
def _clave_venta(descripcion):
    from lector_facturas import norm
    return 'V:' + norm(descripcion)[:120]


def _leer_archivo_base64(d):
    archivo = d.get('archivo') or ''
    if ',' not in archivo:
        raise ApiError('Adjunta el PDF o XML de la factura')
    try:
        return base64.b64decode(archivo.split(',', 1)[1])
    except Exception:
        raise ApiError('No se pudo leer el archivo')


@app.post('/api/ventas/leer-factura')
def api_leer_factura_venta():
    from lector_facturas import leer_factura, LecturaError
    d = request.get_json(force=True) or {}
    data = _leer_archivo_base64(d)
    try:
        r = leer_factura(data, d.get('nombre') or '', _rut_empresa(), modo='venta')
    except LecturaError as e:
        raise ApiError(str(e), 422)
    cli = None
    try:
        rut_c = rut_normalizar(r['receptor'].get('rut'))
        cli = q('SELECT * FROM clientes WHERE rut=?', (rut_c,), one=True) if rut_c else None
    except ApiError:
        pass
    r['cliente_id'] = cli['id'] if cli else None
    r['cliente_nombre'] = cli['razon_social'] if cli else ''
    tipo = r['tipo_doc'] if r['tipo_doc'] in DOCS_VENTA else 'Factura'
    r['duplicados'] = [x for x in buscar_duplicados('venta', r['folio'], None, r['receptor'].get('rut'),
                                                    r['receptor'].get('razon_social'), r['total'])
                       if x['tipo_doc'] == tipo or x['nivel'] != 'exacto']
    r['duplicada'] = any(x['tipo_doc'] == tipo for x in r['duplicados'])
    productos = q('SELECT id, codigo, nombre, costo_promedio, stock FROM productos WHERE activo=1')
    for it in r['items']:
        sug = None
        m = q('SELECT a.producto_id FROM producto_alias a JOIN productos p ON p.id=a.producto_id '
              'WHERE a.clave=? AND p.activo=1', (_clave_venta(it['descripcion']),), one=True)
        if m:
            sug = dict(producto_id=m['producto_id'], via='equivalencia', score=1)
        if not sug and it['codigo']:
            p = next((p for p in productos if p['codigo'].upper() == it['codigo'].upper()), None)
            if p:
                sug = dict(producto_id=p['id'], via='codigo', score=1)
        if not sug and productos:
            mejor = max(productos, key=lambda p: _similitud(it['descripcion'], p['nombre']))
            sc = _similitud(it['descripcion'], mejor['nombre'])
            if sc >= 0.5:
                sug = dict(producto_id=mejor['id'], via='nombre', score=round(sc, 2))
        it['sugerido'] = sug
    return jsonify(r)


@app.post('/api/ventas/importar')
def api_importar_venta():
    d = request.get_json(force=True) or {}
    cli_id = to_id(d.get('cliente_id'))
    if not cli_id:
        cd = d.get('cliente') or {}
        rut_c = rut_normalizar(cd.get('rut'))
        existente = q('SELECT id FROM clientes WHERE rut=?', (rut_c,), one=True) if rut_c else None
        if existente:
            cli_id = existente['id']
        elif (cd.get('razon_social') or '').strip():
            cli_id = insert('INSERT INTO clientes (rut, razon_social, giro, direccion, comuna, ciudad, email, telefono, '
                            'contacto, activo, creado) VALUES (?,?,?,?,?,?,?,?,?,1,?)',
                            (rut_c, cd['razon_social'].strip()[:150], (cd.get('giro') or '')[:120],
                             (cd.get('direccion') or '')[:150], (cd.get('comuna') or '')[:60], (cd.get('ciudad') or '')[:60],
                             '', '', '', ahora()))
    items = []
    for it in d.get('items') or []:
        accion = it.get('accion') or 'libre'
        if accion == 'omitir' or to_float(it.get('cantidad')) <= 0:
            continue
        descripcion = (it.get('descripcion') or '').strip()
        pid = None
        if accion == 'nuevo':
            nv = it.get('nuevo') or {}
            nombre = (nv.get('nombre') or descripcion).strip()
            if not nombre:
                raise ApiError('Un producto nuevo no tiene nombre')
            pid = insert('INSERT INTO productos (codigo, nombre, categoria, unidad, precio_venta, costo_promedio, stock, '
                         'stock_minimo, activo, creado) VALUES (?,?,?,?,?,?,?,?,1,?)',
                         (next_codigo(), nombre[:150], (nv.get('categoria') or '').strip(), 'Unidad',
                          to_int(it.get('precio_unit')), max(to_int(it.get('costo_unit')), 0), 0, 0, ahora()))
        elif accion == 'existente':
            pid = to_id(it.get('producto_id'))
            if not pid or not q('SELECT id FROM productos WHERE id=?', (pid,), one=True):
                raise ApiError(f'Elige el producto para: {descripcion[:60]}')
        if pid and descripcion:
            clave = _clave_venta(descripcion)
            if q('SELECT id FROM producto_alias WHERE clave=?', (clave,), one=True):
                ex('UPDATE producto_alias SET producto_id=?, actualizado=? WHERE clave=?', (pid, ahora(), clave))
            else:
                ex('INSERT INTO producto_alias (clave, producto_id, actualizado) VALUES (?,?,?)', (clave, pid, ahora()))
        items.append(dict(producto_id=pid, descripcion=descripcion, cantidad=to_float(it.get('cantidad')),
                          precio_unit=to_float(it.get('precio_unit')), subtotal=it.get('subtotal'),
                          descuento_pct=min(max(to_float(it.get('descuento_pct')), 0), 100),
                          costo_unit=it.get('costo_unit')))
    if not items:
        raise ApiError('No hay líneas para registrar')
    vid = crear_venta(dict(fecha=d.get('fecha'), cliente_id=cli_id, tipo_doc=d.get('tipo_doc') or 'Factura',
                           folio=d.get('folio'), forma_pago=d.get('forma_pago'), pagada=d.get('pagada'), obs=d.get('obs'),
                           descuento_global=d.get('descuento_global'), iva=d.get('iva'),
                           mover_stock=d.get('mover_stock', True), forzar=d.get('forzar'), adjunto=d.get('adjunto'),
                           adjunto_nombre=d.get('adjunto_nombre'), items=items), subtotal_factura=True)
    commit()
    return api_venta(vid)


# ─────────────────────────── banco y conciliación ───────────────────────────
CATEGORIAS_BANCO = {
    'ingreso': ['Aporte de socios', 'Préstamo recibido', 'Devolución / reembolso', 'Otro ingreso'],
    'egreso': ['Retiro de socios', 'Remuneraciones y honorarios', 'Impuestos (F29 / SII)', 'Comisiones y gastos bancarios',
               'Gastos menores sin factura', 'Arriendo y servicios', 'Pago de préstamo', 'Otro egreso'],
    'ambos': ['Traspaso entre cuentas', 'Sin efecto (anulado / reversado)'],
}
TODAS_CATEGORIAS = [c for v in CATEGORIAS_BANCO.values() for c in v]


def _rut_l(r):
    return (r or '').upper().replace('.', '').replace(' ', '')


def _tokens(s):
    from lector_facturas import norm
    stop = {'de', 'del', 'la', 'el', 'y', 'los', 'las', 'spa', 'ltda', 'limitada', 'sa', 's.a', 'eirl', 'cl'}
    return [w for w in re.findall(r'[a-z0-9]+', norm(s)) if w not in stop]


def sim_nombre(glosa, razon):
    """Parecido entre el nombre truncado de la glosa ('PROVEEDORES INTE') y la razón social; la última palabra
    de la glosa puede venir cortada, así que se acepta como prefijo."""
    a, b = _tokens(glosa), _tokens(razon)
    if not a or not b:
        return 0
    hit = 0
    for i, w in enumerate(a):
        if w in b or (i == len(a) - 1 and len(w) >= 3 and any(x.startswith(w) for x in b)):
            hit += 1
    return hit / len(a)


def aplicar_reglas(mov_ids=None):
    reglas = q('SELECT * FROM reglas_banco ORDER BY LENGTH(patron) DESC')
    if not reglas:
        return 0
    if mov_ids is None:
        movs = q("SELECT id, descripcion, monto FROM mov_banco WHERE estado='pendiente'")
    else:
        movs = [m for m in (q('SELECT id, descripcion, monto, estado FROM mov_banco WHERE id=?', (i,), one=True) for i in mov_ids)
                if m and m['estado'] == 'pendiente']
    from lector_facturas import norm
    n = 0
    for m in movs:
        d = norm(m['descripcion'])
        for r in reglas:
            sentido_ok = r['sentido'] == 'ambos' or (r['sentido'] == 'ingreso') == (m['monto'] > 0)
            if sentido_ok and norm(r['patron']) in d:
                ex("UPDATE mov_banco SET estado='clasificado', categoria=?, regla_id=? WHERE id=?", (r['categoria'], r['id'], m['id']))
                ex('UPDATE reglas_banco SET usos=usos+1 WHERE id=?', (r['id'],))
                n += 1
                break
    return n


@app.get('/api/banco/cuentas')
def api_banco_cuentas():
    cuentas = q('SELECT * FROM cuentas_banco ORDER BY id')
    for c in cuentas:
        ult = q('SELECT fecha, saldo FROM mov_banco WHERE cuenta_id=? ORDER BY fecha DESC, id DESC LIMIT 1', (c['id'],), one=True)
        c['ultimo_saldo'] = ult['saldo'] if ult else None
        c['ultima_fecha'] = ult['fecha'] if ult else None
        c['cartolas'] = q('SELECT c.id, c.numero, c.fecha_inicio, c.fecha_fin, c.saldo_inicial, c.saldo_final, c.movimientos, '
                          'c.archivo, c.creado, (SELECT COUNT(*) FROM mov_banco m WHERE m.cartola_id=c.id) AS nuevos '
                          'FROM cartolas c WHERE c.cuenta_id=? ORDER BY c.fecha_fin DESC', (c['id'],))
    return jsonify(cuentas=cuentas, categorias=CATEGORIAS_BANCO)


@app.post('/api/banco/importar')
def api_banco_importar():
    from lector_cartola import leer_cartola, CartolaError
    d = request.get_json(force=True) or {}
    data = _leer_archivo_base64(d)
    try:
        r = leer_cartola(data, d.get('nombre') or '')
    except CartolaError as e:
        raise ApiError(str(e), 422)
    adv = list(r['advertencias'])
    if r['rut_titular'] and _rut_l(r['rut_titular']) != _rut_l(_rut_empresa()):
        adv.append(f'La cartola es de otro RUT ({r["rut_titular"]}), no de la empresa.')
    numero = r['cuenta'] or 'SIN-NUMERO'
    cta = q('SELECT * FROM cuentas_banco WHERE numero=?', (numero,), one=True)
    if not cta:
        cid = insert('INSERT INTO cuentas_banco (banco, numero, alias, titular, creado) VALUES (?,?,?,?,?)',
                     (r['banco'], numero, r['alias'], r['titular'], ahora()))
        cta = q('SELECT * FROM cuentas_banco WHERE id=?', (cid,), one=True)
    # continuidad con lo ya cargado: el saldo inicial debe calzar con el último saldo anterior
    prev = q('SELECT fecha, saldo FROM mov_banco WHERE cuenta_id=? AND fecha<? ORDER BY fecha DESC, id DESC LIMIT 1',
             (cta['id'], r['fecha_inicio']), one=True)
    if prev and prev['saldo'] is not None and r['saldo_inicial'] is not None and prev['saldo'] != r['saldo_inicial']:
        adv.append(f'El saldo inicial (${r["saldo_inicial"]:,}) no calza con el último saldo cargado '.replace(',', '.') +
                   f'(${prev["saldo"]:,} al {fecha_cl(prev["fecha"])}). Puede faltar una cartola intermedia.'.replace(',', '.'))
    car = insert('INSERT INTO cartolas (cuenta_id, numero, fecha_inicio, fecha_fin, saldo_inicial, saldo_final, archivo, '
                 'movimientos, nuevos, creado) VALUES (?,?,?,?,?,?,?,?,?,?)',
                 (cta['id'], r['numero_cartola'], r['fecha_inicio'], r['fecha_fin'], r['saldo_inicial'], r['saldo_final'],
                  (d.get('nombre') or '')[:120], len(r['movimientos']), 0, ahora()))
    nuevos = []
    for m in r['movimientos']:
        if q('SELECT id FROM mov_banco WHERE huella=?', (m['huella'],), one=True):
            continue
        nuevos.append(insert('INSERT INTO mov_banco (cuenta_id, cartola_id, fecha, descripcion, n_operacion, sucursal, monto, '
                             'saldo, rut, nombre, huella, estado, creado) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                             (cta['id'], car, m['fecha'], m['descripcion'][:200], m['n_operacion'][:30], m['sucursal'][:40],
                              m['monto'], m['saldo'], m['rut'], m['nombre'][:80], m['huella'], 'pendiente', ahora())))
    ex('UPDATE cartolas SET nuevos=? WHERE id=?', (len(nuevos), car))
    if not nuevos:
        ex('DELETE FROM cartolas WHERE id=?', (car,))
    clasif = aplicar_reglas(nuevos)
    commit()
    return jsonify(cuenta_id=cta['id'], cuenta=numero, banco=r['banco'], desde=r['fecha_inicio'], hasta=r['fecha_fin'],
                   total=len(r['movimientos']), nuevos=len(nuevos), repetidos=len(r['movimientos']) - len(nuevos),
                   clasificados=clasif, saldo_inicial=r['saldo_inicial'], saldo_final=r['saldo_final'], advertencias=adv)


def _docs_link(mov_id):
    return q("SELECT p.id AS pago_id, p.tipo, p.doc_id, p.monto, p.origen, COALESCE(v.tipo_doc, c.tipo_doc) AS tipo_doc, "
             "COALESCE(v.folio, c.folio) AS folio, COALESCE(cl.razon_social, pr.razon_social) AS entidad "
             "FROM pagos p LEFT JOIN ventas v ON p.tipo='venta' AND v.id=p.doc_id LEFT JOIN clientes cl ON cl.id=v.cliente_id "
             "LEFT JOIN compras c ON p.tipo='compra' AND c.id=p.doc_id LEFT JOIN proveedores pr ON pr.id=c.proveedor_id "
             "WHERE p.mov_id=? ORDER BY p.id", (mov_id,))


@app.get('/api/banco/movimientos')
def api_banco_movs():
    desde, hasta = _rango()
    params, filtro = [desde, hasta], ''
    if to_id(request.args.get('cuenta_id')):
        filtro, params = ' AND cuenta_id=?', params + [to_id(request.args.get('cuenta_id'))]
    movs = q(f'SELECT * FROM mov_banco WHERE fecha>=? AND fecha<=?{filtro} ORDER BY fecha, id', params)
    links = {}
    if movs:
        for p in q("SELECT p.mov_id, p.tipo, p.doc_id, p.monto, COALESCE(v.tipo_doc, c.tipo_doc) AS tipo_doc, "
                   "COALESCE(v.folio, c.folio) AS folio FROM pagos p LEFT JOIN ventas v ON p.tipo='venta' AND v.id=p.doc_id "
                   "LEFT JOIN compras c ON p.tipo='compra' AND c.id=p.doc_id WHERE p.mov_id IS NOT NULL"):
            links.setdefault(p['mov_id'], []).append(p)
    for m in movs:
        m['docs'] = links.get(m['id'], [])
    return jsonify(movs)


def _candidatos(tipo, mov):
    """Documentos que podría pagar el movimiento: con saldo pendiente, o marcados pagados sin movimiento bancario."""
    t, ent, ent_t = ('ventas', 'cliente_id', 'clientes') if tipo == 'venta' else ('compras', 'proveedor_id', 'proveedores')
    lim_ini = (date.fromisoformat(mov['fecha']) - timedelta(days=400)).isoformat()
    lim_fin = (date.fromisoformat(mov['fecha']) + timedelta(days=7)).isoformat()
    docs = q(f"SELECT d.id, d.fecha, d.tipo_doc, d.folio, d.total, COALESCE(d.pagado,0) AS pagado, d.{ent} AS ent_id, "
             f"e.razon_social AS entidad, e.rut FROM {t} d LEFT JOIN {ent_t} e ON e.id=d.{ent} "
             f"WHERE d.estado='Vigente' AND d.fecha>=? AND d.fecha<=?", (lim_ini, lim_fin))
    sueltos = {}
    for p in q("SELECT id, doc_id, monto FROM pagos WHERE tipo=? AND mov_id IS NULL AND medio<>'Efectivo'", (tipo,)):
        sueltos.setdefault(p['doc_id'], []).append(p)
    out = []
    for dd in docs:
        dd['saldo'] = dd['total'] - dd['pagado']
        dd['pago_suelto'] = next((p for p in sueltos.get(dd['id'], []) if abs(p['monto'] - abs(mov['monto'])) <= 1), None)
        if dd['saldo'] > 0 or dd['pago_suelto']:
            out.append(dd)
    return out


def sugerencias(mov):
    tipo = 'venta' if mov['monto'] > 0 else 'compra'
    imp = abs(mov['monto'])
    res = []
    docs = _candidatos(tipo, mov)
    por_ent = {}
    for d in docs:
        motivos, sc = [], 0
        rut_ok = bool(mov['rut']) and _rut_l(mov['rut']) == _rut_l(d['rut'])
        sn = sim_nombre(mov['nombre'], d['entidad'] or '') if mov['nombre'] else 0
        if rut_ok:
            sc += 40
            motivos.append('mismo RUT')
        elif sn >= 0.6:
            sc += int(35 * sn)
            motivos.append(f'nombre parecido ({int(sn * 100)}%)')
        objetivo = d['pago_suelto']['monto'] if d['pago_suelto'] else d['saldo']
        if abs(objetivo - imp) <= 1:
            sc += 50
            motivos.append('monto exacto' + (' (pago ya registrado)' if d['pago_suelto'] else ''))
            monto_asig = objetivo
        elif imp < d['saldo'] and (rut_ok or sn >= 0.6):
            sc += 10
            motivos.append('abono parcial')
            monto_asig = imp
        else:
            monto_asig = min(imp, d['saldo'])
        if abs((date.fromisoformat(mov['fecha']) - date.fromisoformat(d['fecha'])).days) <= 45:
            sc += 5
        if rut_ok or sn >= 0.6:
            por_ent.setdefault(d['ent_id'], []).append(d)
        if sc >= 40:
            res.append(dict(score=min(sc, 100), motivos=motivos,
                            docs=[dict(tipo=tipo, id=d['id'], monto=monto_asig, tipo_doc=d['tipo_doc'], folio=d['folio'],
                                       fecha=d['fecha'], entidad=d['entidad'], total=d['total'], saldo=d['saldo'])]))
    # un solo pago que cubre varias facturas del mismo cliente/proveedor
    from itertools import combinations
    for ent, ds in por_ent.items():
        ds = [d for d in ds if d['saldo'] > 0][:12]
        hecho = False
        for k in range(2, min(len(ds), 5) + 1):
            for comb in combinations(ds, k):
                if abs(sum(d['saldo'] for d in comb) - imp) <= 1:
                    res.append(dict(score=95, motivos=[f'{k} documentos del mismo {"cliente" if tipo == "venta" else "proveedor"} suman el monto exacto'],
                                    docs=[dict(tipo=tipo, id=d['id'], monto=d['saldo'], tipo_doc=d['tipo_doc'], folio=d['folio'],
                                               fecha=d['fecha'], entidad=d['entidad'], total=d['total'], saldo=d['saldo'])
                                          for d in comb]))
                    hecho = True
                    break
            if hecho:
                break
    res.sort(key=lambda r: -r['score'])
    return res[:8]


def _mov(mid):
    m = q('SELECT * FROM mov_banco WHERE id=?', (mid,), one=True)
    if not m:
        raise ApiError('Movimiento no existe', 404)
    return m


@app.get('/api/banco/movimientos/<int:mid>')
def api_banco_mov(mid):
    m = _mov(mid)
    m['docs'] = _docs_link(mid)
    m['sugerencias'] = sugerencias(m) if m['estado'] == 'pendiente' else []
    m['regla'] = q('SELECT * FROM reglas_banco WHERE id=?', (m['regla_id'],), one=True) if m['regla_id'] else None
    return jsonify(m)


@app.get('/api/banco/documentos')
def api_banco_docs():
    """Documentos con saldo pendiente para conciliar a mano."""
    tipo = 'venta' if request.args.get('tipo') == 'venta' else 'compra'
    mid = to_id(request.args.get('mov_id'))
    mov = _mov(mid) if mid else dict(fecha=hoy(), monto=1 if tipo == 'venta' else -1)
    docs = _candidatos(tipo, mov)
    docs.sort(key=lambda d: d['fecha'], reverse=True)
    return jsonify(docs)


def conciliar_mov(m, asignaciones):
    tipo_ok = 'venta' if m['monto'] > 0 else 'compra'
    total = 0
    for a in asignaciones:
        if a.get('tipo') != tipo_ok:
            raise ApiError('Un abono se concilia con ventas y un cargo con compras')
        total += to_int(a.get('monto'))
    if abs(total - abs(m['monto'])) > 10:
        raise ApiError(f'Lo asignado (${total:,}) debe sumar el monto del movimiento (${abs(m["monto"]):,})'.replace(',', '.'))
    for a in asignaciones:
        did, monto = to_id(a.get('id')), to_int(a.get('monto'))
        if monto <= 0:
            continue
        suelto = q("SELECT id FROM pagos WHERE tipo=? AND doc_id=? AND mov_id IS NULL AND monto BETWEEN ? AND ? ORDER BY id LIMIT 1",
                   (tipo_ok, did, monto - 1, monto + 1), one=True)
        if suelto:   # el pago ya estaba registrado (p. ej. marcado pagado a mano): solo se vincula
            ex('UPDATE pagos SET mov_id=? WHERE id=?', (m['id'], suelto['id']))
            continue
        if monto > saldo_doc(tipo_ok, did) + 1:
            raise ApiError(f'El monto asignado supera el saldo pendiente del documento #{did}')
        registrar_pago(tipo_ok, did, monto, m['fecha'], 'Transferencia', mov_id=m['id'], origen='banco')
    ex("UPDATE mov_banco SET estado='conciliado', categoria=NULL, regla_id=NULL WHERE id=?", (m['id'],))


@app.post('/api/banco/movimientos/<int:mid>/conciliar')
def api_banco_conciliar(mid):
    m = _mov(mid)
    if m['estado'] != 'pendiente':
        raise ApiError('El movimiento ya está conciliado o clasificado; deshazlo primero')
    conciliar_mov(m, (request.get_json(force=True) or {}).get('asignaciones') or [])
    commit()
    return api_banco_mov(mid)


@app.post('/api/banco/movimientos/<int:mid>/clasificar')
def api_banco_clasificar(mid):
    d = request.get_json(force=True) or {}
    m = _mov(mid)
    if m['estado'] == 'conciliado':
        raise ApiError('El movimiento está conciliado con documentos; deshazlo primero')
    cat = (d.get('categoria') or '').strip()
    if cat not in TODAS_CATEGORIAS:
        raise ApiError('Elige una categoría')
    regla_id, extra = None, 0
    patron = (d.get('patron') or '').strip()
    if d.get('crear_regla') and patron:
        if len(patron) < 4:
            raise ApiError('El texto de la regla es muy corto (mínimo 4 caracteres)')
        sentido = 'ambos' if cat in CATEGORIAS_BANCO['ambos'] else ('ingreso' if m['monto'] > 0 else 'egreso')
        regla_id = insert('INSERT INTO reglas_banco (patron, sentido, categoria, usos, creado) VALUES (?,?,?,?,?)',
                          (patron.upper()[:80], sentido, cat, 1, ahora()))
    ex("UPDATE mov_banco SET estado='clasificado', categoria=?, nota=?, regla_id=? WHERE id=?",
       (cat, (d.get('nota') or '').strip()[:200], regla_id, mid))
    if regla_id:
        extra = aplicar_reglas()
    commit()
    r = api_banco_mov(mid).get_json()
    r['aplicados'] = extra
    return jsonify(r)


@app.post('/api/banco/movimientos/<int:mid>/deshacer')
def api_banco_deshacer(mid):
    m = _mov(mid)
    for p in q('SELECT * FROM pagos WHERE mov_id=?', (mid,)):
        if p['origen'] == 'banco':
            ex('DELETE FROM pagos WHERE id=?', (p['id'],))
        else:
            ex('UPDATE pagos SET mov_id=NULL WHERE id=?', (p['id'],))
        recalcular_pago(p['tipo'], p['doc_id'])
    ex("UPDATE mov_banco SET estado='pendiente', categoria=NULL, regla_id=NULL WHERE id=?", (mid,))
    commit()
    return api_banco_mov(mid)


@app.post('/api/banco/auto')
def api_banco_auto():
    """Clasifica con las reglas y concilia solo lo inequívoco: una única sugerencia, con monto exacto y RUT o nombre."""
    d = request.get_json(force=True) or {}
    desde, hasta = fecha_iso(d.get('desde') or '2000-01-01'), fecha_iso(d.get('hasta') or '2099-12-31')
    clasif = aplicar_reglas()
    commit()
    conc = 0
    for m in q("SELECT * FROM mov_banco WHERE estado='pendiente' AND fecha>=? AND fecha<=? ORDER BY fecha, id", (desde, hasta)):
        sug = [s for s in sugerencias(m) if s['score'] >= 85]
        if len(sug) == 1 or (len(sug) > 1 and sug[0]['score'] - sug[1]['score'] >= 20):
            s0 = sug[0]
            if any('monto exacto' in x or 'suman el monto exacto' in x for x in s0['motivos']):
                try:
                    conciliar_mov(m, [dict(tipo=x['tipo'], id=x['id'], monto=x['monto']) for x in s0['docs']])
                    conc += 1
                except ApiError:
                    get_db().rollback()
                    continue
                commit()
    commit()
    return jsonify(clasificados=clasif, conciliados=conc)


def eliminar_movimientos(ids):
    """Borra movimientos bancarios. Si estaban conciliados, los pagos creados desde el banco se eliminan y los
    pagos registrados a mano solo se desvinculan; los documentos recuperan su saldo."""
    n = conc = 0
    for mid in ids:
        m = q('SELECT id, estado FROM mov_banco WHERE id=?', (to_id(mid),), one=True)
        if not m:
            continue
        for p in q('SELECT * FROM pagos WHERE mov_id=?', (m['id'],)):
            if p['origen'] == 'banco':
                ex('DELETE FROM pagos WHERE id=?', (p['id'],))
            else:
                ex('UPDATE pagos SET mov_id=NULL WHERE id=?', (p['id'],))
            recalcular_pago(p['tipo'], p['doc_id'])
        conc += m['estado'] == 'conciliado'
        ex('DELETE FROM mov_banco WHERE id=?', (m['id'],))
        n += 1
    # limpieza: cartolas sin movimientos y cuentas vacías
    ex('DELETE FROM cartolas WHERE NOT EXISTS (SELECT 1 FROM mov_banco m WHERE m.cartola_id=cartolas.id)')
    ex('DELETE FROM cuentas_banco WHERE NOT EXISTS (SELECT 1 FROM mov_banco m WHERE m.cuenta_id=cuentas_banco.id)')
    return n, conc


@app.delete('/api/banco/cartolas/<int:car_id>')
def api_banco_cartola_borrar(car_id):
    if not q('SELECT id FROM cartolas WHERE id=?', (car_id,), one=True):
        raise ApiError('Cartola no existe', 404)
    ids = [r['id'] for r in q('SELECT id FROM mov_banco WHERE cartola_id=?', (car_id,))]
    n, conc = eliminar_movimientos(ids)
    ex('DELETE FROM cartolas WHERE id=?', (car_id,))
    commit()
    return jsonify(eliminados=n, conciliados=conc)


@app.post('/api/banco/movimientos/lote')
def api_banco_lote():
    """Acciones sobre varios movimientos: eliminar, clasificar o deshacer."""
    d = request.get_json(force=True) or {}
    ids = [to_id(i) for i in d.get('ids') or [] if to_id(i)]
    if not ids:
        raise ApiError('No hay movimientos seleccionados')
    accion = d.get('accion')
    if accion == 'eliminar':
        n, conc = eliminar_movimientos(ids)
        commit()
        return jsonify(afectados=n, conciliados=conc)
    if accion == 'clasificar':
        cat = (d.get('categoria') or '').strip()
        if cat not in TODAS_CATEGORIAS:
            raise ApiError('Elige una categoría')
        n = omit = 0
        for mid in ids:
            m = q('SELECT id, estado, monto FROM mov_banco WHERE id=?', (mid,), one=True)
            if not m or m['estado'] == 'conciliado':
                omit += 1
                continue
            valida = cat in CATEGORIAS_BANCO['ambos'] or cat in CATEGORIAS_BANCO['ingreso' if m['monto'] > 0 else 'egreso']
            if not valida:
                omit += 1
                continue
            ex("UPDATE mov_banco SET estado='clasificado', categoria=?, regla_id=NULL WHERE id=?", (cat, mid))
            n += 1
        commit()
        return jsonify(afectados=n, omitidos=omit)
    if accion == 'deshacer':
        n = 0
        for mid in ids:
            m = q('SELECT id, estado FROM mov_banco WHERE id=?', (mid,), one=True)
            if not m or m['estado'] == 'pendiente':
                continue
            for p in q('SELECT * FROM pagos WHERE mov_id=?', (mid,)):
                if p['origen'] == 'banco':
                    ex('DELETE FROM pagos WHERE id=?', (p['id'],))
                else:
                    ex('UPDATE pagos SET mov_id=NULL WHERE id=?', (p['id'],))
                recalcular_pago(p['tipo'], p['doc_id'])
            ex("UPDATE mov_banco SET estado='pendiente', categoria=NULL, regla_id=NULL WHERE id=?", (mid,))
            n += 1
        commit()
        return jsonify(afectados=n)
    raise ApiError('Acción no válida')


@app.get('/api/banco/reglas')
def api_banco_reglas():
    return jsonify(q('SELECT * FROM reglas_banco ORDER BY categoria, patron'))


@app.delete('/api/banco/reglas/<int:rid>')
def api_banco_regla_borrar(rid):
    ex('UPDATE mov_banco SET regla_id=NULL WHERE regla_id=?', (rid,))
    ex('DELETE FROM reglas_banco WHERE id=?', (rid,))
    commit()
    return jsonify(ok=True)


def informe_conciliacion(cuenta_id, desde, hasta):
    movs = q('SELECT * FROM mov_banco WHERE cuenta_id=? AND fecha>=? AND fecha<=? ORDER BY fecha, id', (cuenta_id, desde, hasta))
    saldo_ini = (movs[0]['saldo'] - movs[0]['monto']) if movs and movs[0]['saldo'] is not None else None
    saldo_fin = movs[-1]['saldo'] if movs else None
    if not movs:
        prev = q('SELECT saldo FROM mov_banco WHERE cuenta_id=? AND fecha<? ORDER BY fecha DESC, id DESC LIMIT 1',
                 (cuenta_id, desde), one=True)
        saldo_ini = saldo_fin = prev['saldo'] if prev else None
    est = {k: dict(n=0, abonos=0, cargos=0) for k in ('conciliado', 'clasificado', 'pendiente')}
    flujo = {}
    for m in movs:
        e = est[m['estado']]
        e['n'] += 1
        e['abonos' if m['monto'] > 0 else 'cargos'] += abs(m['monto'])
        if m['estado'] == 'conciliado':
            k = 'Cobros de ventas' if m['monto'] > 0 else 'Pagos a proveedores'
        elif m['estado'] == 'clasificado':
            k = m['categoria']
        else:
            k = 'Sin explicar (pendiente)'
        f = flujo.setdefault(k, dict(categoria=k, ingresos=0, egresos=0, n=0))
        f['ingresos' if m['monto'] > 0 else 'egresos'] += abs(m['monto'])
        f['n'] += 1
    # partidas en tránsito: pagos registrados en el sistema (no en efectivo) que el banco aún no muestra
    transito = q("SELECT p.*, COALESCE(v.tipo_doc, c.tipo_doc) AS tipo_doc, COALESCE(v.folio, c.folio) AS folio, "
                 "COALESCE(cl.razon_social, pr.razon_social) AS entidad FROM pagos p "
                 "LEFT JOIN ventas v ON p.tipo='venta' AND v.id=p.doc_id LEFT JOIN clientes cl ON cl.id=v.cliente_id "
                 "LEFT JOIN compras c ON p.tipo='compra' AND c.id=p.doc_id LEFT JOIN proveedores pr ON pr.id=c.proveedor_id "
                 "WHERE p.mov_id IS NULL AND p.medio NOT IN ('Efectivo','Crédito','Débito') AND p.origen<>'migrado' "
                 "AND p.fecha>=? AND p.fecha<=? ORDER BY p.fecha", (desde, hasta))
    dep_transito = sum(p['monto'] for p in transito if p['tipo'] == 'venta')
    pag_transito = sum(p['monto'] for p in transito if p['tipo'] == 'compra')
    return dict(desde=desde, hasta=hasta, saldo_inicial=saldo_ini, saldo_final=saldo_fin,
                abonos=sum(m['monto'] for m in movs if m['monto'] > 0), cargos=sum(-m['monto'] for m in movs if m['monto'] < 0),
                n=len(movs), estados=est, flujo=sorted(flujo.values(), key=lambda f: -(f['ingresos'] + f['egresos'])),
                pendientes=[m for m in movs if m['estado'] == 'pendiente'], transito=transito,
                dep_transito=dep_transito, pag_transito=pag_transito,
                saldo_ajustado=None if saldo_fin is None else saldo_fin + dep_transito - pag_transito)


@app.get('/api/banco/informe')
def api_banco_informe():
    desde, hasta = _rango()
    cid = to_id(request.args.get('cuenta_id'))
    if not cid:
        raise ApiError('Elige una cuenta')
    return jsonify(informe_conciliacion(cid, desde, hasta))


# ─────────────────────────── cotizaciones ───────────────────────────
def _cot_estado(c):
    if c['estado'] == 'Pendiente':
        try:
            vence = date.fromisoformat(c['fecha']) + timedelta(days=int(c['validez_dias'] or 0))
            if vence < date.fromisoformat(hoy()):
                return 'Vencida'
        except Exception:
            pass
    return c['estado']


def _cot_get(cid):
    c = q('SELECT co.*, cl.razon_social AS cliente, cl.rut AS cliente_rut FROM cotizaciones co '
          'LEFT JOIN clientes cl ON cl.id=co.cliente_id WHERE co.id=?', (cid,), one=True)
    if not c:
        raise ApiError('Cotización no existe', 404)
    c['estado_actual'] = _cot_estado(c)
    c['items'] = q('SELECT qi.*, p.codigo FROM cotizacion_items qi LEFT JOIN productos p ON p.id=qi.producto_id '
                   'WHERE cotizacion_id=? ORDER BY qi.id', (cid,))
    return c


@app.get('/api/cotizaciones')
def api_cots():
    desde, hasta = _rango()
    rows = q('SELECT co.*, cl.razon_social AS cliente FROM cotizaciones co LEFT JOIN clientes cl ON cl.id=co.cliente_id '
             'WHERE co.fecha>=? AND co.fecha<=? ORDER BY co.numero DESC', (desde, hasta))
    for r in rows:
        r['estado_actual'] = _cot_estado(r)
    return jsonify(rows)


@app.get('/api/cotizaciones/<int:cid>')
def api_cot(cid):
    return jsonify(_cot_get(cid))


def _cot_guardar_items(cid, items):
    ex('DELETE FROM cotizacion_items WHERE cotizacion_id=?', (cid,))
    for it in items:
        ex('INSERT INTO cotizacion_items (cotizacion_id, producto_id, descripcion, cantidad, precio_unit, descuento_pct, '
           'subtotal) VALUES (?,?,?,?,?,?,?)', (cid, it['producto_id'], it['descripcion'], it['cantidad'],
                                                it['precio_unit'], it['descuento_pct'], it['subtotal']))


def _siguiente_numero():
    mx = q('SELECT MAX(numero) AS m FROM cotizaciones', one=True)['m'] or 0
    ini = to_int(get_config().get('cot_numero_inicial'), 1)
    return max(mx + 1, ini)


@app.post('/api/cotizaciones')
def api_cot_crear():
    d = request.get_json(force=True) or {}
    items, neto = calc_items(d.get('items'))
    if not d.get('cliente_id'):
        raise ApiError('Selecciona un cliente para la cotización')
    iva = 0 if d.get('exenta') else int(round(neto * IVA))
    cfg = get_config()
    cid = insert('INSERT INTO cotizaciones (numero, fecha, validez_dias, cliente_id, estado, neto, iva, total, obs, '
                 'condiciones, creado) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                 (_siguiente_numero(), fecha_iso(d.get('fecha')),
                  to_int(d.get('validez_dias'), to_int(cfg['cot_validez_dias'], 15)), to_id(d['cliente_id']), 'Pendiente',
                  neto, iva, neto + iva, (d.get('obs') or '').strip(),
                  d.get('condiciones') if d.get('condiciones') is not None else cfg['cot_condiciones'], ahora()))
    _cot_guardar_items(cid, items)
    commit()
    return jsonify(_cot_get(cid))


@app.put('/api/cotizaciones/<int:cid>')
def api_cot_editar(cid):
    d = request.get_json(force=True) or {}
    c = _cot_get(cid)
    if c['estado'] == 'Convertida':
        raise ApiError('La cotización ya se convirtió en venta; duplícala para hacer cambios')
    items, neto = calc_items(d.get('items'))
    if not d.get('cliente_id'):
        raise ApiError('Selecciona un cliente para la cotización')
    iva = 0 if d.get('exenta') else int(round(neto * IVA))
    ex('UPDATE cotizaciones SET fecha=?, validez_dias=?, cliente_id=?, neto=?, iva=?, total=?, obs=?, condiciones=? '
       'WHERE id=?', (fecha_iso(d.get('fecha')), to_int(d.get('validez_dias'), 15), to_id(d['cliente_id']), neto, iva,
                      neto + iva, (d.get('obs') or '').strip(), d.get('condiciones') or '', cid))
    _cot_guardar_items(cid, items)
    commit()
    return jsonify(_cot_get(cid))


@app.post('/api/cotizaciones/<int:cid>/estado')
def api_cot_estado(cid):
    estado = (request.get_json(force=True) or {}).get('estado')
    if estado not in ('Pendiente', 'Aceptada', 'Rechazada'):
        raise ApiError('Estado no válido')
    c = _cot_get(cid)
    if c['estado'] == 'Convertida':
        raise ApiError('La cotización ya está convertida en venta')
    ex('UPDATE cotizaciones SET estado=? WHERE id=?', (estado, cid))
    commit()
    return jsonify(_cot_get(cid))


@app.post('/api/cotizaciones/<int:cid>/duplicar')
def api_cot_duplicar(cid):
    c = _cot_get(cid)
    nid = insert('INSERT INTO cotizaciones (numero, fecha, validez_dias, cliente_id, estado, neto, iva, total, obs, '
                 'condiciones, creado) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                 (_siguiente_numero(), hoy(), c['validez_dias'], c['cliente_id'], 'Pendiente', c['neto'], c['iva'],
                  c['total'], c['obs'], c['condiciones'], ahora()))
    _cot_guardar_items(nid, c['items'])
    commit()
    return jsonify(_cot_get(nid))


@app.post('/api/cotizaciones/<int:cid>/convertir')
def api_cot_convertir(cid):
    d = request.get_json(force=True) or {}
    c = _cot_get(cid)
    if c['estado'] == 'Convertida':
        raise ApiError('Esta cotización ya se convirtió en venta')
    tipo = d.get('tipo_doc') or 'Factura'
    if c['iva'] == 0 and tipo in DOCS_VENTA_IVA:
        tipo = 'Factura exenta'
    vid = crear_venta(dict(fecha=d.get('fecha') or hoy(), cliente_id=c['cliente_id'], tipo_doc=tipo,
                           folio=d.get('folio'), forma_pago=d.get('forma_pago'), pagada=d.get('pagada'),
                           obs=f'Desde cotización N° {c["numero"]}', items=c['items'], forzar=d.get('forzar')),
                      cotizacion_id=cid)
    ex("UPDATE cotizaciones SET estado='Convertida', venta_id=? WHERE id=?", (vid, cid))
    commit()
    return jsonify(venta_id=vid, cotizacion=_cot_get(cid))


@app.delete('/api/cotizaciones/<int:cid>')
def api_cot_borrar(cid):
    c = _cot_get(cid)
    if c['estado'] == 'Convertida':
        raise ApiError('No se puede eliminar una cotización convertida en venta')
    ex('DELETE FROM cotizacion_items WHERE cotizacion_id=?', (cid,))
    ex('DELETE FROM cotizaciones WHERE id=?', (cid,))
    commit()
    return jsonify(ok=True)


@app.get('/api/cotizaciones/<int:cid>/pdf')
def api_cot_pdf(cid):
    from pdf_cotizacion import generar_pdf
    c = _cot_get(cid)
    cliente = q('SELECT * FROM clientes WHERE id=?', (c['cliente_id'],), one=True) or {}
    pdf = generar_pdf(c, cliente, get_config(), fecha_cl)
    nombre = f'Cotizacion_{c["numero"]:06d}.pdf'
    return send_file(io.BytesIO(pdf), mimetype='application/pdf', download_name=nombre,
                     as_attachment=request.args.get('descargar') == '1')


# ─────────────────────────── resumen ───────────────────────────
def _mes_rango(mes):
    y, m = (int(x) for x in mes.split('-'))
    ini = date(y, m, 1)
    fin = date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)
    return ini.isoformat(), fin.isoformat()


@app.get('/api/resumen')
def api_resumen():
    mes = request.args.get('mes') or hoy()[:7]
    try:
        ini, fin = _mes_rango(mes)
    except Exception:
        raise ApiError('Mes inválido (usa AAAA-MM)')
    v = q("SELECT COUNT(*) AS n, COALESCE(SUM(neto),0) AS neto, COALESCE(SUM(iva),0) AS iva, "
          "COALESCE(SUM(total),0) AS total, COALESCE(SUM(costo_total),0) AS costo FROM ventas "
          "WHERE estado='Vigente' AND fecha>=? AND fecha<=?", (ini, fin), one=True)
    c = q("SELECT COUNT(*) AS n, COALESCE(SUM(neto),0) AS neto, COALESCE(SUM(iva),0) AS iva, "
          "COALESCE(SUM(total),0) AS total FROM compras WHERE estado='Vigente' AND fecha>=? AND fecha<=?",
          (ini, fin), one=True)
    por_cobrar = q("SELECT COUNT(*) AS n, COALESCE(SUM(total - COALESCE(pagado,0)),0) AS total FROM ventas "
                   "WHERE estado='Vigente' AND pagada=0", one=True)
    por_pagar = q("SELECT COUNT(*) AS n, COALESCE(SUM(total - COALESCE(pagado,0)),0) AS total FROM compras "
                  "WHERE estado='Vigente' AND pagada=0", one=True)
    cots = [r for r in q("SELECT * FROM cotizaciones WHERE estado IN ('Pendiente','Aceptada')")
            if _cot_estado(r) != 'Vencida']
    # últimos 12 meses
    y, m = (int(x) for x in mes.split('-'))
    meses = []
    for i in range(11, -1, -1):
        mm, yy = m - i, y
        while mm <= 0:
            mm += 12
            yy -= 1
        meses.append(f'{yy:04d}-{mm:02d}')
    serie = {k: dict(mes=k, ventas=0, costo=0, compras=0) for k in meses}
    desde12 = meses[0] + '-01'
    for r in q("SELECT fecha, neto, costo_total FROM ventas WHERE estado='Vigente' AND fecha>=? AND fecha<=?",
               (desde12, fin)):
        k = r['fecha'][:7]
        if k in serie:
            serie[k]['ventas'] += r['neto']
            serie[k]['costo'] += r['costo_total']
    for r in q("SELECT fecha, neto FROM compras WHERE estado='Vigente' AND fecha>=? AND fecha<=?", (desde12, fin)):
        k = r['fecha'][:7]
        if k in serie:
            serie[k]['compras'] += r['neto']
    top = q("SELECT COALESCE(p.codigo,'') AS codigo, vi.descripcion, SUM(vi.cantidad) AS cantidad, "
            "SUM(vi.subtotal) AS neto, SUM(vi.subtotal - vi.costo_unit*vi.cantidad) AS margen "
            "FROM venta_items vi JOIN ventas v ON v.id=vi.venta_id LEFT JOIN productos p ON p.id=vi.producto_id "
            "WHERE v.estado='Vigente' AND v.fecha>=? AND v.fecha<=? "
            "GROUP BY p.codigo, vi.descripcion ORDER BY SUM(vi.subtotal) DESC LIMIT 8", (ini, fin))
    stock_bajo = q("SELECT id, codigo, nombre, stock, stock_minimo, unidad FROM productos WHERE activo=1 AND "
                   "(stock < 0 OR (stock_minimo > 0 AND stock <= stock_minimo)) ORDER BY stock - stock_minimo")
    inv = q("SELECT COALESCE(SUM(CASE WHEN stock>0 THEN stock*costo_promedio ELSE 0 END),0) AS valor, "
            "COUNT(*) AS n FROM productos WHERE activo=1", one=True)
    return jsonify(mes=mes, ventas=v, compras=c, margen=v['neto'] - v['costo'],
                   iva_debito=v['iva'], iva_credito=c['iva'], iva_a_pagar=v['iva'] - c['iva'],
                   por_cobrar=por_cobrar, por_pagar=por_pagar,
                   cotizaciones=dict(n=len(cots), total=sum(r['neto'] for r in cots)),
                   serie=list(serie.values()), top=top, stock_bajo=stock_bajo,
                   inventario=dict(valor=int(round(inv['valor'] or 0)), n=inv['n']))


# ─────────────────────────── exportaciones ───────────────────────────
def _xlsx(nombre_hoja, headers, filas, money_cols=(), titulo=None):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    wb = Workbook()
    ws = wb.active
    ws.title = nombre_hoja[:31]
    r0 = 1
    if titulo:
        ws.cell(1, 1, titulo).font = Font(bold=True, size=13)
        r0 = 3
    for j, h in enumerate(headers, 1):
        c = ws.cell(r0, j, h)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = PatternFill('solid', fgColor='1F3A5F')
        c.alignment = Alignment(horizontal='center')
    for i, fila in enumerate(filas, r0 + 1):
        for j, val in enumerate(fila, 1):
            c = ws.cell(i, j, val)
            if headers[j - 1] in money_cols:
                c.number_format = '#,##0'
    if filas:
        n = r0 + len(filas) + 1
        ws.cell(n, 1, 'TOTAL').font = Font(bold=True)
        for j, h in enumerate(headers, 1):
            if h in money_cols:
                col = get_column_letter(j)
                c = ws.cell(n, j, f'=SUM({col}{r0 + 1}:{col}{n - 1})')
                c.font = Font(bold=True)
                c.number_format = '#,##0'
    for j, h in enumerate(headers, 1):
        largo = max([len(str(h))] + [len(str(f[j - 1] or '')) for f in filas[:300]])
        ws.column_dimensions[get_column_letter(j)].width = min(max(10, largo + 2), 50)
    ws.freeze_panes = ws.cell(r0 + 1, 1)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


@app.get('/exportar/<tipo>')
def exportar(tipo):
    desde, hasta = _rango()
    per = f'{fecha_cl(desde)} al {fecha_cl(hasta)}' if request.args.get('desde') else 'todo el período'
    emp = get_config()['empresa_nombre']
    if tipo == 'ventas':
        rows = q("SELECT v.*, c.razon_social AS cliente, c.rut AS cliente_rut FROM ventas v LEFT JOIN clientes c "
                 "ON c.id=v.cliente_id WHERE v.fecha>=? AND v.fecha<=? ORDER BY v.fecha, v.id", (desde, hasta))
        h = ['N°', 'Fecha', 'Documento', 'Folio', 'RUT', 'Cliente', 'Neto', 'IVA', 'Total', 'Costo', 'Margen',
             'Pagada', 'Estado']
        f = [[r['id'], fecha_cl(r['fecha']), r['tipo_doc'], r['folio'], r['cliente_rut'] or '', r['cliente'] or '',
              *((r['neto'], r['iva'], r['total'], r['costo_total'], r['neto'] - r['costo_total'])
                if r['estado'] == 'Vigente' else (0, 0, 0, 0, 0)),
              'Sí' if r['pagada'] else 'No', r['estado']] for r in rows]
        buf = _xlsx('Ventas', h, f, {'Neto', 'IVA', 'Total', 'Costo', 'Margen'}, f'Libro de ventas — {emp} — {per}')
    elif tipo == 'ventas_detalle':
        rows = q("SELECT v.id, v.fecha, v.tipo_doc, v.folio, c.razon_social AS cliente, p.codigo, vi.descripcion, "
                 "vi.cantidad, vi.precio_unit, vi.descuento_pct, vi.subtotal, vi.costo_unit FROM venta_items vi "
                 "JOIN ventas v ON v.id=vi.venta_id LEFT JOIN clientes c ON c.id=v.cliente_id "
                 "LEFT JOIN productos p ON p.id=vi.producto_id WHERE v.estado='Vigente' AND v.fecha>=? AND v.fecha<=? "
                 "ORDER BY v.fecha, v.id", (desde, hasta))
        h = ['Venta N°', 'Fecha', 'Documento', 'Folio', 'Cliente', 'Código', 'Descripción', 'Cantidad',
             'Precio unit.', 'Desc. %', 'Subtotal', 'Costo', 'Margen']
        f = [[r['id'], fecha_cl(r['fecha']), r['tipo_doc'], r['folio'], r['cliente'] or '', r['codigo'] or '',
              r['descripcion'], r['cantidad'], r['precio_unit'], r['descuento_pct'], r['subtotal'],
              int(round(r['costo_unit'] * r['cantidad'])),
              r['subtotal'] - int(round(r['costo_unit'] * r['cantidad']))] for r in rows]
        buf = _xlsx('Detalle ventas', h, f, {'Subtotal', 'Costo', 'Margen'}, f'Detalle de ventas — {emp} — {per}')
    elif tipo == 'compras':
        rows = q("SELECT co.id, co.fecha, co.tipo_doc, co.folio, co.neto, co.iva, co.total, co.estado, co.pagada, "
                 "p.razon_social AS proveedor, p.rut AS proveedor_rut FROM compras co LEFT JOIN proveedores p "
                 "ON p.id=co.proveedor_id WHERE co.fecha>=? AND co.fecha<=? ORDER BY co.fecha, co.id", (desde, hasta))
        h = ['N°', 'Fecha', 'Documento', 'Folio', 'RUT', 'Proveedor', 'Neto', 'IVA', 'Total', 'Pagada', 'Estado']
        f = [[r['id'], fecha_cl(r['fecha']), r['tipo_doc'], r['folio'], r['proveedor_rut'] or '',
              r['proveedor'] or '',
              *((r['neto'], r['iva'], r['total']) if r['estado'] == 'Vigente' else (0, 0, 0)),
              'Sí' if r['pagada'] else 'No', r['estado']] for r in rows]
        buf = _xlsx('Compras', h, f, {'Neto', 'IVA', 'Total'}, f'Libro de compras — {emp} — {per}')
    elif tipo == 'inventario':
        rows = q('SELECT * FROM productos WHERE activo=1 ORDER BY codigo')
        h = ['Código', 'Producto', 'Categoría', 'Unidad', 'Stock', 'Stock mínimo', 'Costo promedio',
             'Precio venta', 'Valorizado']
        f = [[r['codigo'], r['nombre'], r['categoria'], r['unidad'], r['stock'], r['stock_minimo'],
              r['costo_promedio'], r['precio_venta'], int(round(max(r['stock'], 0) * r['costo_promedio']))]
             for r in rows]
        buf = _xlsx('Inventario', h, f, {'Costo promedio', 'Precio venta', 'Valorizado'},
                    f'Inventario valorizado — {emp} — al {fecha_cl(hoy())}')
    else:
        raise ApiError('Exportación no válida', 404)
    return send_file(buf, as_attachment=True, download_name=f'BF_{tipo}_{hoy()}.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


TABLAS_RESPALDO = ['config', 'productos', 'clientes', 'proveedores', 'compras', 'compra_items', 'ventas', 'venta_items',
                   'cotizaciones', 'cotizacion_items', 'movimientos', 'producto_proveedor', 'fletes', 'flete_items',
                   'producto_alias', 'cuentas_banco', 'cartolas', 'reglas_banco', 'mov_banco', 'pagos']


def _columnas(tabla):
    if USE_PG:
        return [r['column_name'] for r in q('SELECT column_name FROM information_schema.columns '
                                            'WHERE table_schema=current_schema() AND table_name=?', (tabla,))]
    return [r['name'] for r in q(f'PRAGMA table_info({tabla})')]


@app.get('/api/sistema')
def api_sistema():
    from urllib.parse import urlparse
    host = urlparse(DATABASE_URL).hostname or '' if USE_PG else ''
    proveedor = ('Neon' if 'neon.tech' in host else 'Supabase' if 'supabase' in host
                 else 'Render' if ('render.com' in host or host.startswith('dpg-')) else ('PostgreSQL' if USE_PG else 'SQLite local'))
    if USE_PG:
        tam = q('SELECT pg_database_size(current_database()) AS t', one=True)['t']
    else:
        tam = os.path.getsize(SQLITE_PATH) if os.path.exists(SQLITE_PATH) else 0
    en_bd = sum(q(f"SELECT COUNT(*) AS n FROM {t} WHERE adjunto IS NOT NULL AND adjunto<>''", one=True)['n'] for t in TABLAS_ADJ)
    en_nube = sum(q(f"SELECT COUNT(*) AS n FROM {t} WHERE adjunto_id IS NOT NULL AND adjunto_id<>''", one=True)['n']
                  for t in TABLAS_ADJ)
    return jsonify(base=proveedor, tam_bytes=int(tam or 0), cloudinary=cloud_ok(), carpeta=CLOUDINARY_FOLDER,
                   adjuntos_bd=en_bd, adjuntos_nube=en_nube)


@app.post('/api/adjuntos/migrar')
def api_migrar_adjuntos():
    """Mueve a Cloudinary los adjuntos guardados en la base (de a 5 por llamada)."""
    if not cloud_ok():
        raise ApiError('Cloudinary no está configurado en Render (falta CLOUDINARY_URL)')
    movidos = 0
    for t in TABLAS_ADJ:
        for r in q(f"SELECT id, adjunto, adjunto_nombre FROM {t} WHERE adjunto IS NOT NULL AND adjunto<>'' "
                   "ORDER BY id LIMIT 5"):
            guardar_adjunto(r['id'], r['adjunto'], r['adjunto_nombre'], t)
            commit()
            movidos += 1
        if movidos:
            break
    pend = sum(q(f"SELECT COUNT(*) AS n FROM {t} WHERE adjunto IS NOT NULL AND adjunto<>''", one=True)['n'] for t in TABLAS_ADJ)
    return jsonify(movidos=movidos, pendientes=pend)


@app.post('/api/restaurar')
def api_restaurar():
    """Carga un respaldo JSON en una base vacía (sirve para cambiar de proveedor de base de datos)."""
    f = request.files.get('archivo')
    if not f:
        raise ApiError('Adjunta el archivo de respaldo (.json)')
    try:
        data = json.load(f)
    except Exception:
        raise ApiError('El archivo no es un respaldo válido')
    if not isinstance(data, dict) or 'productos' not in data:
        raise ApiError('El archivo no parece un respaldo del sistema B&F')
    for t in ['productos', 'clientes', 'proveedores', 'compras', 'ventas', 'cotizaciones']:
        if q(f'SELECT id FROM {t} LIMIT 1'):
            raise ApiError('Esta base ya tiene datos. El respaldo solo se puede restaurar en una base nueva y vacía.', 409)
    resumen = {}
    for t in TABLAS_RESPALDO:
        filas = data.get(t) or []
        if not filas:
            continue
        cols_bd = set(_columnas(t))
        if t == 'config':
            for r in filas:
                if r.get('clave') in CONFIG_KEYS:
                    ex('UPDATE config SET valor=? WHERE clave=?', (r.get('valor') or '', r['clave']))
            continue
        for r in filas:
            cols = [k for k in r if k in cols_bd]
            ex(f'INSERT INTO {t} ({", ".join(cols)}) VALUES ({", ".join("?" * len(cols))})', [r[k] for k in cols])
        resumen[t] = len(filas)
        if USE_PG and 'id' in cols_bd:
            ex(f"SELECT setval(pg_get_serial_sequence('{t}', 'id'), (SELECT COALESCE(MAX(id), 1) FROM {t}), "
               f"(SELECT COUNT(*) > 0 FROM {t}))")
    recalcular_costos()
    commit()
    return jsonify(ok=True, restaurado=resumen, generado=data.get('_generado'))


@app.get('/exportar/conciliacion')
def exportar_conciliacion():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
    desde, hasta = _rango()
    cid = to_id(request.args.get('cuenta_id'))
    cta = q('SELECT * FROM cuentas_banco WHERE id=?', (cid,), one=True)
    if not cta:
        raise ApiError('Cuenta no existe', 404)
    inf = informe_conciliacion(cid, desde, hasta)
    wb = Workbook()
    head = lambda ws, r, vals: [setattr(ws.cell(r, j, v), 'font', Font(bold=True, color='FFFFFF')) or
                                setattr(ws.cell(r, j), 'fill', PatternFill('solid', fgColor='1F3A5F')) for j, v in enumerate(vals, 1)]
    ws = wb.active
    ws.title = 'Conciliación'
    emp = get_config()['empresa_nombre']
    filas = [(f'Conciliación bancaria — {emp}', None), (f'{cta["banco"]} cuenta {cta["numero"]} · {fecha_cl(desde)} al {fecha_cl(hasta)}', None),
             (None, None), ('Saldo inicial según banco', inf['saldo_inicial']), ('+ Abonos del período', inf['abonos']),
             ('− Cargos del período', inf['cargos']), ('Saldo final según banco', inf['saldo_final']), (None, None),
             ('+ Cobros registrados aún no vistos en el banco (depósitos en tránsito)', inf['dep_transito']),
             ('− Pagos registrados aún no cobrados (cheques / transferencias en tránsito)', inf['pag_transito']),
             ('Saldo conciliado', inf['saldo_ajustado']), (None, None),
             ('Movimientos conciliados con documentos', inf['estados']['conciliado']['n']),
             ('Movimientos clasificados por categoría', inf['estados']['clasificado']['n']),
             ('Movimientos pendientes (sin explicar)', inf['estados']['pendiente']['n'])]
    for i, (k, v) in enumerate(filas, 1):
        if k:
            ws.cell(i, 1, k).font = Font(bold=i in (1, 7, 11), size=13 if i == 1 else 11)
        if v is not None:
            c = ws.cell(i, 2, v)
            c.number_format = '#,##0'
            c.font = Font(bold=i in (7, 11))
    ws.column_dimensions['A'].width = 72
    ws.column_dimensions['B'].width = 16
    ws2 = wb.create_sheet('Flujo por categoría')
    head(ws2, 1, ['Categoría', 'Movimientos', 'Ingresos', 'Egresos', 'Neto'])
    for i, f in enumerate(inf['flujo'], 2):
        for j, v in enumerate([f['categoria'], f['n'], f['ingresos'], f['egresos'], f['ingresos'] - f['egresos']], 1):
            ws2.cell(i, j, v).number_format = '#,##0'
    movs = q('SELECT * FROM mov_banco WHERE cuenta_id=? AND fecha>=? AND fecha<=? ORDER BY fecha, id', (cid, desde, hasta))
    ws3 = wb.create_sheet('Movimientos')
    head(ws3, 1, ['Fecha', 'Descripción', 'N° operación', 'Cargo', 'Abono', 'Saldo', 'Estado', 'Categoría / documento', 'Nota'])
    for i, m in enumerate(movs, 2):
        docs = ', '.join(f'{d["tipo_doc"]} {d["folio"] or ""} ({d["entidad"] or ""})' for d in _docs_link(m['id']))
        vals = [fecha_cl(m['fecha']), m['descripcion'], m['n_operacion'], -m['monto'] if m['monto'] < 0 else None,
                m['monto'] if m['monto'] > 0 else None, m['saldo'], m['estado'].capitalize(), docs or m['categoria'] or '', m['nota']]
        for j, v in enumerate(vals, 1):
            ws3.cell(i, j, v).number_format = '#,##0'
    ws4 = wb.create_sheet('Partidas en tránsito')
    head(ws4, 1, ['Fecha', 'Tipo', 'Documento', 'Cliente / proveedor', 'Medio', 'Monto'])
    for i, p in enumerate(inf['transito'], 2):
        for j, v in enumerate([fecha_cl(p['fecha']), 'Cobro' if p['tipo'] == 'venta' else 'Pago', f'{p["tipo_doc"]} {p["folio"] or ""}',
                               p['entidad'] or '', p['medio'], p['monto']], 1):
            ws4.cell(i, j, v).number_format = '#,##0'
    for w in (ws2, ws3, ws4):
        for j in range(1, w.max_column + 1):
            w.column_dimensions[get_column_letter(j)].width = 48 if w.cell(1, j).value in ('Descripción', 'Categoría / documento', 'Cliente / proveedor', 'Categoría') else 15
        w.freeze_panes = 'A2'
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f'BF_conciliacion_{cta["numero"]}_{desde}_{hasta}.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.get('/exportar/respaldo')
def exportar_respaldo():
    data = {t: q(f'SELECT * FROM {t}') for t in TABLAS_RESPALDO}
    data['_generado'] = ahora()
    return Response(json.dumps(data, ensure_ascii=False, default=str), mimetype='application/json',
                    headers={'Content-Disposition': f'attachment; filename=BF_respaldo_{hoy()}.json'})


# la base se crea/actualiza al final, cuando todas las funciones (incluido el recálculo de costos) ya existen
with app.app_context():
    init_db()


if __name__ == '__main__':
    app.run(debug=True, port=int(os.environ.get('PORT', 5000)))
