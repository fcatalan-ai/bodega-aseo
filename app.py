from flask import Flask, render_template, request, jsonify, redirect, session, send_file
import os, io, base64
from datetime import datetime
from functools import wraps

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bodega-aseo-2025-secret')
DATABASE_URL = os.environ.get('DATABASE_URL', '')

CATEGORIAS = ['Papel','Bolsas','Líquidos Limpieza','Desinfectantes','Paños','Guantes','Varios']
EDIFICIOS  = ['Básica','Media','Parvularia','Administración']
MESES      = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
               'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']

def get_db():
    if DATABASE_URL:
        import psycopg2, psycopg2.extras
        return psycopg2.connect(DATABASE_URL), 'pg'
    import sqlite3
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)),'bodega.db'))
    conn.row_factory = sqlite3.Row
    return conn, 'sqlite'

def db_fetchall(sql, params=()):
    conn, mode = get_db()
    if mode == 'pg':
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql.replace('?','%s'), params)
    else:
        cur = conn.cursor()
        cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def db_fetchone(sql, params=()):
    conn, mode = get_db()
    if mode == 'pg':
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql.replace('?','%s'), params)
    else:
        cur = conn.cursor()
        cur.execute(sql, params)
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def db_run(sql, params=()):
    conn, mode = get_db()
    cur = conn.cursor()
    cur.execute(sql.replace('?','%s') if mode=='pg' else sql, params)
    conn.commit()
    conn.close()

def init_db():
    conn, mode = get_db()
    cur = conn.cursor()
    PK = 'SERIAL PRIMARY KEY' if mode=='pg' else 'INTEGER PRIMARY KEY AUTOINCREMENT'
    TS = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' if mode=='pg' else 'TEXT DEFAULT CURRENT_TIMESTAMP'
    cur.execute(f'''CREATE TABLE IF NOT EXISTS productos (
        id {PK}, nombre TEXT NOT NULL, categoria TEXT,
        unidad TEXT DEFAULT 'unidades', stock_actual INTEGER DEFAULT 0,
        stock_minimo INTEGER DEFAULT 0, activo BOOLEAN DEFAULT TRUE
    )''')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS movimientos (
        id {PK}, producto_id INTEGER NOT NULL,
        tipo TEXT NOT NULL, cantidad INTEGER NOT NULL,
        edificio TEXT, usuario TEXT, observacion TEXT,
        fecha TEXT, created_at {TS}
    )''')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS usuarios (
        id {PK}, nombre TEXT, email TEXT UNIQUE,
        password TEXT, rol TEXT DEFAULT 'edificio', edificio TEXT
    )''')
    if mode == 'pg':
        admin_email = os.environ.get('ADMIN_EMAIL','bodega@colegio.cl')
        admin_pass  = os.environ.get('ADMIN_PASS','bodega2025')
        cur.execute("INSERT INTO usuarios (nombre,email,password,rol,edificio) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (email) DO NOTHING",
            ('Bodega', admin_email, admin_pass, 'admin', ''))
        cur.execute("UPDATE usuarios SET password=%s, email=%s WHERE rol='admin'",
            (admin_pass, admin_email))
    else:
        admin_email = os.environ.get('ADMIN_EMAIL','bodega@colegio.cl')
        admin_pass  = os.environ.get('ADMIN_PASS','bodega2025')
        cur.execute("INSERT OR IGNORE INTO usuarios (nombre,email,password,rol,edificio) VALUES (?,?,?,?,?)",
            ('Bodega', admin_email, admin_pass, 'admin', ''))
        cur.execute("UPDATE usuarios SET password=?, email=? WHERE rol='admin'",
            (admin_pass, admin_email))
    conn.commit()
    conn.close()

with app.app_context():
    init_db()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect('/login')
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect('/login')
        if session.get('rol') != 'admin': return jsonify({'error':'Sin permisos'}), 403
        return f(*args, **kwargs)
    return decorated

def operador_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session: return redirect('/login')
        if session.get('rol') not in ('admin','operador'): return jsonify({'error':'Sin permisos'}), 403
        return f(*args, **kwargs)
    return decorated

# ── AUTH ─────────────────────────────────────────────────────────────────────
@app.route('/admin/usuarios')
@admin_required
def admin_usuarios():
    usuarios = db_fetchall("SELECT id, nombre, email, rol, edificio FROM usuarios ORDER BY rol, nombre")
    return render_template('admin_usuarios.html', usuarios=usuarios,
                          edificios=EDIFICIOS, user=session['user'], rol=session['rol'])

@app.route('/admin/usuarios/crear', methods=['POST'])
@admin_required
def crear_usuario():
    data = request.json
    nombre   = data.get('nombre','').strip()
    email    = data.get('email','').strip()
    password = data.get('password','').strip()
    rol      = data.get('rol','operador')
    edificio = data.get('edificio','')
    if not nombre or not email or not password:
        return jsonify({'error':'Todos los campos son requeridos'}), 400
    try:
        db_run("INSERT INTO usuarios (nombre,email,password,rol,edificio) VALUES (?,?,?,?,?)",
                  (nombre, email, password, rol, edificio))
        return jsonify({'ok': True})
    except Exception as e:
        if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
            return jsonify({'error': 'El email ya está registrado'}), 400
        return jsonify({'error': str(e)}), 500

@app.route('/admin/usuarios/<int:uid>', methods=['DELETE'])
@admin_required
def eliminar_usuario(uid):
    u = db_fetchone("SELECT rol FROM usuarios WHERE id=?", (uid,))
    if u and u['rol'] == 'admin':
        return jsonify({'error': 'No se puede eliminar al administrador'}), 400
    db_run("DELETE FROM usuarios WHERE id=?", (uid,))
    return jsonify({'ok': True})

@app.route('/admin/usuarios/<int:uid>/password', methods=['PUT'])
@admin_required
def cambiar_password(uid):
    data = request.json
    password = data.get('password','').strip()
    if not password:
        return jsonify({'error':'Password requerido'}), 400
    db_run("UPDATE usuarios SET password=? WHERE id=?", (password, uid))
    return jsonify({'ok': True})

@app.route('/login', methods=['GET','POST'])
def login():
    error = ''
    if request.method == 'POST':
        email    = request.form.get('email','').strip()
        password = request.form.get('password','')
        u = db_fetchone("SELECT * FROM usuarios WHERE email=? AND password=?", (email, password))
        if u:
            session['user']     = u['nombre']
            session['rol']      = u['rol']
            session['email']    = u['email']
            session['edificio'] = u.get('edificio','')
            return redirect('/')
        error = 'Email o contraseña incorrectos'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

# ── MAIN ─────────────────────────────────────────────────────────────────────
@app.route('/')
@login_required
def index():
    return render_template('index.html',
        user=session['user'], rol=session['rol'],
        edificio=session.get('edificio',''),
        categorias=CATEGORIAS, edificios=EDIFICIOS)

# ── PRODUCTOS ─────────────────────────────────────────────────────────────────
@app.route('/api/productos', methods=['GET'])
@login_required
def get_productos():
    cat = request.args.get('categoria','')
    q   = request.args.get('q','')
    sql = "SELECT * FROM productos WHERE activo=TRUE"
    params = []
    if cat:
        sql += " AND categoria=?"; params.append(cat)
    if q:
        sql += " AND nombre LIKE ?"; params.append(f'%{q}%')
    sql += " ORDER BY categoria, nombre"
    return jsonify(db_fetchall(sql, params))

@app.route('/api/productos/<int:pid>', methods=['GET'])
@login_required
def get_producto(pid):
    p = db_fetchone("SELECT * FROM productos WHERE id=?", (pid,))
    if not p: return jsonify({'error':'No encontrado'}), 404
    movs = db_fetchall(
        "SELECT * FROM movimientos WHERE producto_id=? ORDER BY created_at DESC LIMIT 50", (pid,))
    return jsonify({'producto': p, 'movimientos': movs})

@app.route('/api/productos', methods=['POST'])
@admin_required
def crear_producto():
    d = request.json
    conn, mode = get_db()
    cur = conn.cursor()
    if mode == 'pg':
        cur.execute(
            "INSERT INTO productos (nombre,categoria,unidad,stock_actual,stock_minimo) VALUES (%s,%s,%s,%s,%s) RETURNING id",
            (d['nombre'], d.get('categoria','Varios'), d.get('unidad','unidades'),
             int(d.get('stock_actual',0)), int(d.get('stock_minimo',0))))
        pid = cur.fetchone()[0]
    else:
        cur.execute(
            "INSERT INTO productos (nombre,categoria,unidad,stock_actual,stock_minimo) VALUES (?,?,?,?,?)",
            (d['nombre'], d.get('categoria','Varios'), d.get('unidad','unidades'),
             int(d.get('stock_actual',0)), int(d.get('stock_minimo',0))))
        pid = cur.lastrowid
    conn.commit(); conn.close()
    return jsonify({'ok':True,'id':pid})

@app.route('/api/productos/<int:pid>', methods=['PUT'])
@admin_required
def editar_producto(pid):
    d = request.json
    campos = ['nombre','categoria','unidad','stock_minimo']
    updates = {c:d[c] for c in campos if c in d}
    if updates:
        sets = ', '.join(f"{c}=?" for c in updates)
        db_run(f"UPDATE productos SET {sets} WHERE id=?", list(updates.values())+[pid])
    return jsonify({'ok':True})

@app.route('/api/productos/<int:pid>', methods=['DELETE'])
@admin_required
def eliminar_producto(pid):
    db_run("UPDATE productos SET activo=FALSE WHERE id=?", (pid,))
    return jsonify({'ok':True})

# ── PAGINA PUBLICA POR QR (sin login) ────────────────────────────────────────
@app.route('/producto/<int:pid>/publico')
def producto_publico(pid):
    p = db_fetchone("SELECT * FROM productos WHERE id=? AND activo=TRUE", (pid,))
    if not p:
        return "Producto no encontrado", 404
    movs = db_fetchall(
        """SELECT tipo, cantidad, edificio, usuario, observacion, fecha
           FROM movimientos WHERE producto_id=?
           ORDER BY created_at DESC LIMIT 10""", (pid,))
    return render_template('producto_publico.html',
        producto=p, movimientos=movs, edificios=EDIFICIOS)

@app.route('/api/productos/<int:pid>/salida-publica', methods=['POST'])
def salida_publica(pid):
    """Registra salida rapida desde la pagina publica (sin login, con PIN)."""
    PIN_BODEGA = os.environ.get('PIN_BODEGA', '')
    d = request.json
    pin      = str(d.get('pin', '')).strip()
    nombre   = str(d.get('nombre', '')).strip()
    edificio = str(d.get('edificio', '')).strip()
    cant     = int(d.get('cantidad', 0))

    if not PIN_BODEGA:
        return jsonify({'error': 'PIN_BODEGA no configurado en el servidor'}), 500
    if pin != PIN_BODEGA:
        return jsonify({'error': 'PIN incorrecto'}), 403
    if not nombre:
        return jsonify({'error': 'El nombre es obligatorio'}), 400
    if not edificio:
        return jsonify({'error': 'Selecciona el edificio'}), 400
    if cant <= 0:
        return jsonify({'error': 'Cantidad invalida'}), 400

    p = db_fetchone("SELECT * FROM productos WHERE id=? AND activo=TRUE", (pid,))
    if not p:
        return jsonify({'error': 'Producto no encontrado'}), 404
    if p['stock_actual'] < cant:
        return jsonify({'error': f'Stock insuficiente. Disponible: {p["stock_actual"]} {p["unidad"]}'}), 400

    fecha = datetime.now().strftime('%d-%m-%Y')
    conn, mode = get_db()
    cur = conn.cursor()
    if mode == 'pg':
        cur.execute(
            "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (pid, 'salida', cant, edificio, nombre, '[Salida QR]', fecha))
        cur.execute("UPDATE productos SET stock_actual=stock_actual-%s WHERE id=%s", (cant, pid))
    else:
        cur.execute(
            "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (?,?,?,?,?,?,?)",
            (pid, 'salida', cant, edificio, nombre, '[Salida QR]', fecha))
        cur.execute("UPDATE productos SET stock_actual=stock_actual-? WHERE id=?", (cant, pid))
    conn.commit(); conn.close()

    nuevo_stock = db_fetchone("SELECT stock_actual FROM productos WHERE id=?", (pid,))
    return jsonify({'ok': True, 'stock_nuevo': nuevo_stock['stock_actual'] if nuevo_stock else 0})

@app.route('/api/productos/<int:pid>/qr')
def get_qr(pid):
    """Genera imagen QR con la URL publica del producto."""
    import qrcode
    p = db_fetchone("SELECT nombre FROM productos WHERE id=?", (pid,))
    if not p:
        return jsonify({'error': 'No encontrado'}), 404

    base = request.host_url.rstrip('/')
    url = f"{base}/producto/{pid}/publico"

    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L,
                        box_size=8, border=3)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#0D4F3C", back_color="white")

    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return send_file(buf, mimetype='image/png',
                     download_name=f'qr_{p["nombre"].replace(" ","_")}.png')

# ── PDF GUIA DE SALIDA ─────────────────────────────────────────────────────────
@app.route('/api/guias/pdf', methods=['POST'])
@login_required
def generar_pdf_guia():
    """Genera PDF de guia de salida con firma digital."""
    try:
        from fpdf import FPDF
    except ImportError:
        return jsonify({'error': 'fpdf2 no instalado'}), 500

    d = request.json
    responsable = d.get('responsable', '')
    edificio    = d.get('edificio', '')
    fecha       = d.get('fecha', '')
    obs         = d.get('obs', '')
    items       = d.get('items', [])
    guia_id     = d.get('guia_id', '')
    firma_b64   = d.get('firma', '')  # data:image/png;base64,...

    def safe(txt):
        repl = {'á':'a','é':'e','í':'i','ó':'o','ú':'u',
                '\xe1':'a','\xe9':'e','\xed':'i','\xf3':'o','\xfa':'u',
                '\xc1':'A','\xc9':'E','\xcd':'I','\xd3':'O','\xda':'U',
                '\xf1':'n','\xd1':'N','\xfc':'u','\xdc':'U',
                '—':'-','–':'-','“':'"','”':'"',
                '\xbf':'?','\xa1':'!'}
        out = ''
        for ch in str(txt):
            out += repl.get(ch, ch if ord(ch) < 256 else '?')
        return out

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_margins(15, 15, 15)

    # Encabezado verde
    pdf.set_fill_color(13, 79, 60)
    pdf.rect(0, 0, 210, 28, 'F')
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_xy(15, 6)
    pdf.cell(0, 8, safe('Colegio Centenario de Temuco'), ln=True)
    pdf.set_font('Helvetica', '', 10)
    pdf.set_xy(15, 15)
    pdf.cell(0, 7, safe('Bodega de Aseo - Guia de Salida'), ln=True)

    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_xy(140, 8)
    pdf.cell(55, 8, safe(guia_id), align='R')

    pdf.set_text_color(0, 0, 0)

    # Datos guia
    pdf.set_xy(15, 33)
    pdf.set_fill_color(240, 247, 244)
    pdf.rect(15, 33, 180, 26, 'F')
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_xy(18, 36)
    pdf.cell(45, 6, safe('Responsable del retiro:'))
    pdf.set_font('Helvetica', '', 9)
    pdf.cell(0, 6, safe(responsable), ln=True)

    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_x(18)
    pdf.cell(45, 6, safe('Edificio de destino:'))
    pdf.set_font('Helvetica', '', 9)
    pdf.cell(60, 6, safe(edificio))
    pdf.set_font('Helvetica', 'B', 9)
    pdf.cell(20, 6, 'Fecha:')
    pdf.set_font('Helvetica', '', 9)
    pdf.cell(0, 6, safe(fecha), ln=True)

    if obs:
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_x(18)
        pdf.cell(45, 6, 'Observaciones:')
        pdf.set_font('Helvetica', '', 9)
        pdf.cell(0, 6, safe(obs), ln=True)

    # Tabla de productos
    pdf.set_xy(15, 64)
    pdf.set_fill_color(26, 107, 82)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 9)
    pdf.cell(10, 7, '#', fill=True, border=0, align='C')
    pdf.cell(105, 7, 'Producto', fill=True, border=0)
    pdf.cell(30, 7, 'Cantidad', fill=True, border=0, align='C')
    pdf.cell(35, 7, 'Unidad', fill=True, border=0, align='C')
    pdf.ln()

    pdf.set_text_color(0, 0, 0)
    pdf.set_font('Helvetica', '', 9)
    for i, it in enumerate(items, 1):
        if i % 2 == 0:
            pdf.set_fill_color(245, 250, 248)
        else:
            pdf.set_fill_color(255, 255, 255)
        pdf.cell(10, 7, str(i), fill=True, border=0, align='C')
        pdf.cell(105, 7, safe(it.get('nombre', '')), fill=True, border=0)
        pdf.cell(30, 7, str(it.get('cantidad', '')), fill=True, border=0, align='C')
        pdf.cell(35, 7, safe(it.get('unidad', '')), fill=True, border=0, align='C')
        pdf.ln()

    pdf.set_fill_color(224, 235, 224)
    pdf.set_font('Helvetica', 'B', 9)
    total_cant = sum(int(it.get('cantidad', 0)) for it in items)
    pdf.cell(10, 7, '', fill=True, border=0)
    pdf.cell(105, 7, safe(f'Total: {len(items)} producto(s)'), fill=True, border=0)
    pdf.cell(30, 7, str(total_cant), fill=True, border=0, align='C')
    pdf.cell(35, 7, 'unidades', fill=True, border=0, align='C')
    pdf.ln(14)

    # Firma digital
    firma_insertada = False
    if firma_b64 and ',' in firma_b64:
        try:
            from PIL import Image as PILImage
            firma_data = base64.b64decode(firma_b64.split(',', 1)[1])
            img_firma = PILImage.open(io.BytesIO(firma_data)).convert('RGBA')
            fondo = PILImage.new('RGB', img_firma.size, (255, 255, 255))
            fondo.paste(img_firma, mask=img_firma.split()[3])
            buf_firma = io.BytesIO()
            fondo.save(buf_firma, format='PNG')
            buf_firma.seek(0)

            pdf.set_font('Helvetica', 'B', 9)
            pdf.cell(0, 6, safe('Firma del responsable:'), ln=True)
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                tmp.write(buf_firma.read())
                tmp_path = tmp.name
            pdf.image(tmp_path, x=15, y=pdf.get_y(), w=70, h=28)
            os.unlink(tmp_path)
            pdf.set_y(pdf.get_y() + 32)
            firma_insertada = True
        except Exception:
            pass

    if not firma_insertada:
        pdf.set_font('Helvetica', '', 9)
        pdf.cell(0, 6, '_' * 45 + '   ' + '_' * 30, ln=True)
        pdf.set_font('Helvetica', '', 8)
        pdf.cell(0, 5, safe('     Firma del responsable                    RUT'), ln=True)

    pdf.ln(4)
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(150, 150, 150)
    pdf.set_x(15)
    pdf.cell(0, 5, safe(f'Generado el {datetime.now().strftime("%d-%m-%Y %H:%M")} - Sistema Bodega Aseo - Colegio Centenario de Temuco'), align='C')

    buf = io.BytesIO()
    pdf.output(buf)
    buf.seek(0)
    pdf_b64 = base64.b64encode(buf.read()).decode('utf-8')
    return jsonify({'ok': True, 'pdf_b64': pdf_b64, 'nombre': f'{guia_id}.pdf'})


# ── MOVIMIENTOS ───────────────────────────────────────────────────────────────
@app.route('/api/movimientos', methods=['POST'])
@login_required
def registrar_movimiento():
    d       = request.json
    pid     = int(d['producto_id'])
    tipo    = d['tipo']
    cant    = int(d['cantidad'])
    edificio= d.get('edificio', session.get('edificio',''))
    obs     = d.get('observacion','')
    fecha   = d.get('fecha', datetime.now().strftime('%d-%m-%Y'))
    usuario = session['user']

    if tipo == 'salida':
        p = db_fetchone("SELECT stock_actual FROM productos WHERE id=?", (pid,))
        if not p: return jsonify({'error':'Producto no encontrado'}), 404
        if p['stock_actual'] < cant:
            return jsonify({'error':f'Stock insuficiente. Disponible: {p["stock_actual"]}'}), 400

    conn, mode = get_db()
    cur = conn.cursor()
    delta = cant if tipo == 'entrada' else -cant
    if mode == 'pg':
        cur.execute(
            "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (pid, tipo, cant, edificio, usuario, obs, fecha))
        cur.execute("UPDATE productos SET stock_actual = stock_actual + %s WHERE id=%s", (delta, pid))
    else:
        cur.execute(
            "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (?,?,?,?,?,?,?)",
            (pid, tipo, cant, edificio, usuario, obs, fecha))
        cur.execute("UPDATE productos SET stock_actual = stock_actual + ? WHERE id=?", (delta, pid))
    conn.commit(); conn.close()
    return jsonify({'ok':True})

@app.route('/api/movimientos', methods=['GET'])
@login_required
def get_movimientos():
    tipo     = request.args.get('tipo','')
    edificio = request.args.get('edificio','')
    mes      = request.args.get('mes','')
    pid      = request.args.get('producto_id','')
    sql = '''SELECT m.*, p.nombre as producto_nombre, p.categoria, p.unidad
             FROM movimientos m JOIN productos p ON m.producto_id=p.id WHERE 1=1'''
    params = []
    if tipo:     sql += " AND m.tipo=?";        params.append(tipo)
    if edificio: sql += " AND m.edificio=?";    params.append(edificio)
    if pid:      sql += " AND m.producto_id=?"; params.append(int(pid))
    if mes:      sql += " AND m.fecha LIKE ?";  params.append(f'%-{mes}-%')
    sql += " ORDER BY m.created_at DESC LIMIT 200"
    return jsonify(db_fetchall(sql, params))

# ── STATS / DASHBOARD ─────────────────────────────────────────────────────────
@app.route('/api/stats')
@login_required
def stats():
    total_prod  = db_fetchone("SELECT COUNT(*) as n FROM productos WHERE activo=TRUE")['n']
    bajo_minimo = db_fetchone(
        "SELECT COUNT(*) as n FROM productos WHERE activo=TRUE AND stock_actual <= stock_minimo AND stock_minimo > 0")['n']
    total_ent   = db_fetchone(
        "SELECT COALESCE(SUM(cantidad),0) as s FROM movimientos WHERE tipo='entrada'")['s']
    total_sal   = db_fetchone(
        "SELECT COALESCE(SUM(cantidad),0) as s FROM movimientos WHERE tipo='salida'")['s']
    return jsonify({'total_productos':total_prod,'bajo_minimo':bajo_minimo,
                    'total_entradas':total_ent,'total_salidas':total_sal})

@app.route('/api/kpis')
@login_required
def kpis():
    por_cat = db_fetchall(
        "SELECT categoria, COUNT(*) as productos, SUM(stock_actual) as stock_total FROM productos WHERE activo=TRUE GROUP BY categoria ORDER BY stock_total DESC")
    alertas = db_fetchall(
        "SELECT * FROM productos WHERE activo=TRUE AND stock_actual <= stock_minimo AND stock_minimo > 0 ORDER BY stock_actual ASC")
    por_edificio = db_fetchall(
        "SELECT edificio, SUM(cantidad) as total FROM movimientos WHERE tipo='salida' AND edificio!='' GROUP BY edificio ORDER BY total DESC")
    consumo_mes = db_fetchall(
        """SELECT fecha, SUM(CASE WHEN tipo='entrada' THEN cantidad ELSE 0 END) as entradas,
                  SUM(CASE WHEN tipo='salida' THEN cantidad ELSE 0 END) as salidas
           FROM movimientos GROUP BY fecha ORDER BY fecha DESC LIMIT 100""")
    mes_dict = {}
    for r in consumo_mes:
        f = str(r.get('fecha',''))
        if len(f) >= 7:
            parts = f.split('-')
            if len(parts) == 3:
                mes_key = f"{parts[1]}-{parts[2]}" if len(parts[2])==4 else f"{parts[2]}-{parts[1]}"
                if mes_key not in mes_dict:
                    mes_dict[mes_key] = {'entradas':0,'salidas':0}
                mes_dict[mes_key]['entradas'] += int(r['entradas'] or 0)
                mes_dict[mes_key]['salidas']  += int(r['salidas'] or 0)
    consumo_mensual = [{'mes':k,'entradas':v['entradas'],'salidas':v['salidas']}
                       for k,v in sorted(mes_dict.items())[-12:]]
    mas_consumidos = db_fetchall(
        """SELECT p.nombre, p.categoria, p.unidad, p.stock_actual,
                  COALESCE(SUM(CASE WHEN m.tipo='salida' THEN m.cantidad ELSE 0 END),0) as total_salidas
           FROM productos p LEFT JOIN movimientos m ON p.id=m.producto_id
           WHERE p.activo=TRUE GROUP BY p.id, p.nombre, p.categoria, p.unidad, p.stock_actual
           ORDER BY total_salidas DESC LIMIT 10""")
    consumo_ed_prod = db_fetchall(
        """SELECT m.edificio, p.nombre, SUM(m.cantidad) as total
           FROM movimientos m JOIN productos p ON m.producto_id=p.id
           WHERE m.tipo='salida' AND m.edificio!=''
           GROUP BY m.edificio, p.nombre ORDER BY total DESC LIMIT 30""")
    return jsonify({
        'por_categoria':    por_cat,
        'alertas':          alertas,
        'por_edificio':     por_edificio,
        'consumo_mensual':  consumo_mensual,
        'mas_consumidos':   mas_consumidos,
        'consumo_ed_prod':  consumo_ed_prod,
    })

@app.route('/dashboard')
@admin_required
def dashboard():
    return render_template('dashboard.html', user=session['user'], rol=session['rol'])

# ── EXPORT ────────────────────────────────────────────────────────────────────
@app.route('/api/export/stock')
@login_required
def export_stock():
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    rows = db_fetchall("SELECT * FROM productos WHERE activo=TRUE ORDER BY categoria, nombre")
    wb = openpyxl.Workbook(); ws = wb.active; ws.title="Stock Actual"
    headers = ['ID','Nombre','Categoria','Unidad','Stock Actual','Stock Minimo','Estado']
    for col,h in enumerate(headers,1):
        cell = ws.cell(row=1,column=col,value=h)
        cell.font = Font(bold=True,color='FFFFFF')
        cell.fill = PatternFill('solid',start_color='1F3864',fgColor='1F3864')
        cell.alignment = Alignment(horizontal='center')
        ws.column_dimensions[ws.cell(row=1,column=col).column_letter].width = 18
    for ri,r in enumerate(rows,2):
        estado = 'BAJO MINIMO' if r['stock_actual'] <= r['stock_minimo'] and r['stock_minimo']>0 else 'OK'
        for col,val in enumerate([r['id'],r['nombre'],r['categoria'],r['unidad'],r['stock_actual'],r['stock_minimo'],estado],1):
            ws.cell(row=ri,column=col,value=val)
    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return send_file(buf,download_name='stock_bodega.xlsx',as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/api/export/movimientos')
@login_required
def export_movimientos():
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    rows = db_fetchall(
        """SELECT m.fecha, m.tipo, p.nombre, p.unidad, m.cantidad,
                  m.edificio, m.usuario, m.observacion
           FROM movimientos m JOIN productos p ON m.producto_id=p.id
           ORDER BY m.created_at DESC""")
    wb = openpyxl.Workbook(); ws = wb.active; ws.title="Movimientos"
    headers = ['Fecha','Tipo','Producto','Unidad','Cantidad','Edificio','Usuario','Observacion']
    for col,h in enumerate(headers,1):
        cell = ws.cell(row=1,column=col,value=h)
        cell.font = Font(bold=True,color='FFFFFF')
        cell.fill = PatternFill('solid',start_color='1F3864',fgColor='1F3864')
        cell.alignment = Alignment(horizontal='center')
        ws.column_dimensions[ws.cell(row=1,column=col).column_letter].width = 18
    for ri,r in enumerate(rows,2):
        for col,key in enumerate(['fecha','tipo','nombre','unidad','cantidad','edificio','usuario','observacion'],1):
            ws.cell(row=ri,column=col,value=r.get(key,''))
    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return send_file(buf,download_name='movimientos_bodega.xlsx',as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/api/importar', methods=['POST'])
@login_required
def importar_excel():
    if 'archivo' not in request.files:
        return jsonify({'error':'No se envio archivo'}), 400
    file = request.files['archivo']
    if not file.filename.lower().endswith(('.xlsx','.xls')):
        return jsonify({'error':'Solo se aceptan archivos Excel (.xlsx)'}), 400
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file.read()), data_only=True)
        ws = None
        for name in wb.sheetnames:
            if 'PROD' in name.upper():
                ws = wb[name]; break
        if ws is None: ws = wb.active
        creados = 0
        errores = []
        for row_num in range(4, ws.max_row + 1):
            def gv(col):
                v = ws.cell(row=row_num, column=col).value
                if v is None: return ''
                if hasattr(v, 'strftime'): return v.strftime('%d-%m-%Y')
                return str(v).strip()
            nombre   = gv(1)
            categoria= gv(2)
            unidad   = gv(3)
            if not nombre or not categoria: continue
            try:
                stock   = int(float(gv(4))) if gv(4) else 0
                minimo  = int(float(gv(5))) if gv(5) else 0
                conn2, mode2 = get_db()
                cur2 = conn2.cursor()
                if mode2 == 'pg':
                    cur2.execute(
                        "INSERT INTO productos (nombre,categoria,unidad,stock_actual,stock_minimo) VALUES (%s,%s,%s,%s,%s)",
                        (nombre, categoria, unidad or 'unidades', stock, minimo))
                else:
                    cur2.execute(
                        "INSERT INTO productos (nombre,categoria,unidad,stock_actual,stock_minimo) VALUES (?,?,?,?,?)",
                        (nombre, categoria, unidad or 'unidades', stock, minimo))
                conn2.commit(); conn2.close()
                creados += 1
            except Exception as e:
                errores.append(f"Fila {row_num}: {str(e)}")
        return jsonify({'ok':True,'creados':creados,'errores':errores})
    except Exception as e:
        return jsonify({'error':str(e)}), 500

@app.route('/api/kpis/filtrado')
@login_required
def kpis_filtrado():
    fecha_desde = request.args.get('desde','')
    fecha_hasta = request.args.get('hasta','')
    mes         = request.args.get('mes','')
    filtro_sql = "WHERE tipo='salida'"
    params = []
    if mes:
        filtro_sql += " AND fecha LIKE ?"
        params.append(f"%-{mes}")
    elif fecha_desde and fecha_hasta:
        filtro_sql += " AND fecha >= ? AND fecha <= ?"
        params += [fecha_desde, fecha_hasta]
    elif fecha_desde:
        filtro_sql += " AND fecha >= ?"
        params.append(fecha_desde)
    filtro_ent = filtro_sql.replace("tipo='salida'","tipo='entrada'")
    total_sal = db_fetchone(
        f"SELECT COALESCE(SUM(cantidad),0) as s FROM movimientos {filtro_sql}", params)
    total_ent = db_fetchone(
        f"SELECT COALESCE(SUM(cantidad),0) as s FROM movimientos {filtro_ent}", params)
    por_edificio = db_fetchall(
        f"""SELECT edificio, SUM(cantidad) as total
            FROM movimientos {filtro_sql} AND edificio!=''
            GROUP BY edificio ORDER BY total DESC""", params)
    por_producto = db_fetchall(
        f"""SELECT p.nombre, p.categoria, p.unidad, SUM(m.cantidad) as total
            FROM movimientos m JOIN productos p ON m.producto_id=p.id
            {filtro_sql}
            GROUP BY p.id, p.nombre, p.categoria, p.unidad
            ORDER BY total DESC LIMIT 15""", params)
    detalle = db_fetchall(
        f"""SELECT m.fecha, m.tipo, p.nombre, p.unidad, m.cantidad, m.edificio, m.usuario
            FROM movimientos m JOIN productos p ON m.producto_id=p.id
            {filtro_sql}
            ORDER BY m.created_at DESC LIMIT 100""", params)
    return jsonify({
        'total_salidas':  total_sal['s'] if total_sal else 0,
        'total_entradas': total_ent['s'] if total_ent else 0,
        'por_edificio':   por_edificio,
        'por_producto':   por_producto,
        'detalle':        detalle,
    })

@app.route('/api/movimientos/historial')
@login_required
def get_historial():
    tipo     = request.args.get('tipo','')
    edificio = request.args.get('edificio','')
    cat      = request.args.get('categoria_filtro','')
    q        = request.args.get('q','')
    desde    = request.args.get('desde','')
    hasta    = request.args.get('hasta','')
    sql = """SELECT m.id, m.fecha, m.tipo, m.cantidad, m.edificio,
                    m.usuario, m.observacion, m.producto_id,
                    p.nombre as producto_nombre, p.categoria, p.unidad
             FROM movimientos m JOIN productos p ON m.producto_id=p.id
             WHERE 1=1"""
    params = []
    if tipo:     sql += " AND m.tipo=?";          params.append(tipo)
    if edificio: sql += " AND m.edificio=?";      params.append(edificio)
    if cat:      sql += " AND p.categoria=?";     params.append(cat)
    if q:        sql += " AND p.nombre LIKE ?";   params.append(f'%{q}%')
    if desde:
        parts = desde.split('-')
        if len(parts)==3:
            yyyy,mm,dd = parts
            desde_num = yyyy+mm+dd
            sql += (" AND (CASE WHEN SUBSTR(m.fecha,3,1)='-'"
                    " THEN SUBSTR(m.fecha,7,4)||SUBSTR(m.fecha,4,2)||SUBSTR(m.fecha,1,2)"
                    " ELSE REPLACE(SUBSTR(m.fecha,1,10),'-','')"
                    " END) >= ?")
            params.append(desde_num)
    if hasta:
        parts = hasta.split('-')
        if len(parts)==3:
            yyyy,mm,dd = parts
            hasta_num = yyyy+mm+dd
            sql += (" AND (CASE WHEN SUBSTR(m.fecha,3,1)='-'"
                    " THEN SUBSTR(m.fecha,7,4)||SUBSTR(m.fecha,4,2)||SUBSTR(m.fecha,1,2)"
                    " ELSE REPLACE(SUBSTR(m.fecha,1,10),'-','')"
                    " END) <= ?")
            params.append(hasta_num)
    sql += " ORDER BY m.created_at DESC LIMIT 500"
    return jsonify(db_fetchall(sql, params))

@app.route('/movimientos')
@login_required
def movimientos_page():
    return render_template('movimientos.html',
        user=session['user'], rol=session['rol'],
        categorias=CATEGORIAS, edificios=EDIFICIOS)


@app.route('/api/movimientos/<int:mid>', methods=['PUT'])
@login_required
def editar_movimiento(mid):
    d = request.json
    mov = db_fetchone("SELECT * FROM movimientos WHERE id=?", (mid,))
    if not mov: return jsonify({'error':'No encontrado'}), 404
    nueva_cant = int(d.get('cantidad', mov['cantidad']))
    dif = nueva_cant - mov['cantidad']
    conn2, mode2 = get_db()
    cur2 = conn2.cursor()
    if mode2 == 'pg':
        cur2.execute(
            "UPDATE movimientos SET cantidad=%s,edificio=%s,observacion=%s,fecha=%s WHERE id=%s",
            (nueva_cant, d.get('edificio', mov['edificio']),
             d.get('observacion', mov['observacion']),
             d.get('fecha', mov['fecha']), mid))
        if mov['tipo'] == 'salida':
            cur2.execute("UPDATE productos SET stock_actual=stock_actual-%s WHERE id=%s", (dif, mov['producto_id']))
        else:
            cur2.execute("UPDATE productos SET stock_actual=stock_actual+%s WHERE id=%s", (dif, mov['producto_id']))
    else:
        cur2.execute(
            "UPDATE movimientos SET cantidad=?,edificio=?,observacion=?,fecha=? WHERE id=?",
            (nueva_cant, d.get('edificio', mov['edificio']),
             d.get('observacion', mov['observacion']),
             d.get('fecha', mov['fecha']), mid))
        if mov['tipo'] == 'salida':
            cur2.execute("UPDATE productos SET stock_actual=stock_actual-? WHERE id=?", (dif, mov['producto_id']))
        else:
            cur2.execute("UPDATE productos SET stock_actual=stock_actual+? WHERE id=?", (dif, mov['producto_id']))
    conn2.commit()
    conn2.close()
    return jsonify({'ok': True})

@app.route('/api/movimientos/<int:mid>', methods=['DELETE'])
@login_required
def eliminar_movimiento(mid):
    mov = db_fetchone("SELECT * FROM movimientos WHERE id=?", (mid,))
    if not mov: return jsonify({'error':'No encontrado'}), 404
    conn2, mode2 = get_db()
    cur2 = conn2.cursor()
    delta = mov['cantidad'] if mov['tipo'] == 'salida' else -mov['cantidad']
    if mode2 == 'pg':
        cur2.execute("UPDATE productos SET stock_actual=stock_actual+%s WHERE id=%s", (delta, mov['producto_id']))
        cur2.execute("DELETE FROM movimientos WHERE id=%s", (mid,))
    else:
        cur2.execute("UPDATE productos SET stock_actual=stock_actual+? WHERE id=?", (delta, mov['producto_id']))
        cur2.execute("DELETE FROM movimientos WHERE id=?", (mid,))
    conn2.commit()
    conn2.close()
    return jsonify({'ok': True})

@app.route('/api/importar/movimientos', methods=['POST'])
@login_required
def importar_movimientos():
    if 'archivo' not in request.files:
        return jsonify({'error':'No se envio archivo'}), 400
    file = request.files['archivo']
    if not file.filename.lower().endswith(('.xlsx','.xls')):
        return jsonify({'error':'Solo se aceptan archivos Excel (.xlsx)'}), 400
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file.read()), data_only=True)
        ws = None
        for name in wb.sheetnames:
            if 'MOV' in name.upper():
                ws = wb[name]; break
        if ws is None: ws = wb.active
        creados = 0
        errores = []
        for row_num in range(4, ws.max_row + 1):
            def gv(col):
                v = ws.cell(row=row_num, column=col).value
                if v is None: return ''
                return str(v).strip()
            def gv_fecha(col):
                v = ws.cell(row=row_num, column=col).value
                if v is None: return ''
                if hasattr(v, 'strftime'):
                    return v.strftime('%d-%m-%Y')
                s = str(v).strip()
                import re
                if re.match(r'^\d{4}-\d{2}-\d{2}', s):
                    parts = s[:10].split('-')
                    return f"{parts[2]}-{parts[1]}-{parts[0]}"
                m = re.match(r'^(\d{1,2})\s+[\d:]+[-](\d{2})[-](\d{4})', s)
                if m:
                    return f"{m.group(1).zfill(2)}-{m.group(2)}-{m.group(3)}"
                return s
            nombre   = gv(1)
            tipo     = gv(2).lower()
            cant_raw = gv(3)
            edificio = gv(4)
            fecha    = gv_fecha(5)
            obs      = gv(6)
            if not nombre or not tipo or not cant_raw: continue
            if tipo not in ('entrada','salida'): continue
            try:
                cant = int(float(cant_raw))
                if cant <= 0: continue
                prod = db_fetchone(
                    "SELECT id, stock_actual FROM productos WHERE LOWER(nombre)=LOWER(?)", (nombre,))
                if not prod:
                    errores.append(f"Fila {row_num}: producto '{nombre}' no encontrado")
                    continue
                if tipo == 'salida' and prod['stock_actual'] < cant:
                    errores.append(f"Fila {row_num}: stock insuficiente para '{nombre}' (hay {prod['stock_actual']})")
                    continue
                import re
                fecha_norm = fecha
                if fecha:
                    if re.match(r'\d{4}-\d{2}-\d{2}', str(fecha)):
                        parts = str(fecha).split('-')
                        fecha_norm = f"{parts[2]}-{parts[1]}-{parts[0]}"
                    elif hasattr(fecha, 'strftime'):
                        fecha_norm = fecha.strftime('%d-%m-%Y')
                conn2, mode2 = get_db()
                cur2 = conn2.cursor()
                delta = cant if tipo == 'entrada' else -cant
                usu = session['user']
                if mode2 == 'pg':
                    cur2.execute(
                        "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (prod['id'], tipo, cant, edificio, usu, obs, fecha_norm))
                    cur2.execute(
                        "UPDATE productos SET stock_actual=stock_actual+%s WHERE id=%s",
                        (delta, prod['id']))
                else:
                    cur2.execute(
                        "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (?,?,?,?,?,?,?)",
                        (prod['id'], tipo, cant, edificio, usu, obs, fecha_norm))
                    cur2.execute(
                        "UPDATE productos SET stock_actual=stock_actual+? WHERE id=?",
                        (delta, prod['id']))
                conn2.commit()
                conn2.close()
                creados += 1
            except Exception as e:
                errores.append(f"Fila {row_num}: {str(e)}")
        return jsonify({'ok':True,'creados':creados,'errores':errores})
    except Exception as e:
        return jsonify({'error':str(e)}), 500

@app.route('/api/resumen-semanal')
@login_required
def resumen_semanal():
    from datetime import datetime, timedelta
    hoy = datetime.now()
    lunes_actual = hoy - timedelta(days=hoy.weekday())
    lunes_anterior = lunes_actual - timedelta(days=7)
    domingo_anterior = lunes_actual - timedelta(days=1)
    def fmt(d): return d.strftime('%d-%m-%Y')
    def consumo_semana(desde, hasta):
        desde_num = desde.strftime('%Y%m%d')
        hasta_num = hasta.strftime('%Y%m%d')
        sql = """SELECT m.edificio, p.nombre, p.unidad, SUM(m.cantidad) as total
                 FROM movimientos m JOIN productos p ON m.producto_id=p.id
                 WHERE m.tipo='salida'
                 AND (CASE WHEN SUBSTR(m.fecha,3,1)='-'
                      THEN SUBSTR(m.fecha,7,4)||SUBSTR(m.fecha,4,2)||SUBSTR(m.fecha,1,2)
                      ELSE REPLACE(SUBSTR(m.fecha,1,10),'-','')
                      END) BETWEEN ? AND ?
                 GROUP BY m.edificio, p.nombre, p.unidad
                 ORDER BY m.edificio, total DESC"""
        return db_fetchall(sql, (desde_num, hasta_num))
    actual  = consumo_semana(lunes_actual, hoy)
    anterior= consumo_semana(lunes_anterior, domingo_anterior)
    def por_edificio(rows):
        result = {}
        for r in rows:
            ed = r['edificio'] or 'Sin edificio'
            if ed not in result:
                result[ed] = {'total': 0, 'productos': []}
            result[ed]['total'] += r['total']
            result[ed]['productos'].append({'nombre': r['nombre'],'total': r['total'],'unidad': r['unidad']})
        return result
    return jsonify({
        'semana_actual':   {'desde': fmt(lunes_actual),'hasta': fmt(hoy),'por_edificio': por_edificio(actual),'total': sum(r['total'] for r in actual)},
        'semana_anterior': {'desde': fmt(lunes_anterior),'hasta': fmt(domingo_anterior),'por_edificio': por_edificio(anterior),'total': sum(r['total'] for r in anterior)}
    })

@app.route('/reposicion')
@admin_required
def reposicion_page():
    return render_template('reposicion.html', user=session['user'], rol=session['rol'])

@app.route('/guia')
@login_required
def guia_page():
    return render_template('guia.html', user=session['user'], rol=session['rol'],
        edificios=EDIFICIOS)

@app.route('/guia_entrada')
@operador_required
def guia_entrada_page():
    return render_template('guia_entrada.html', user=session['user'], rol=session['rol'])

@app.route('/api/reposicion')
@login_required
def get_reposicion():
    bajo = db_fetchall(
        """SELECT p.*,
                  COALESCE((SELECT SUM(cantidad) FROM movimientos
                            WHERE producto_id=p.id AND tipo='salida'), 0) as total_salidas,
                  COALESCE((SELECT AVG(mes_cant) FROM (
                      SELECT SUM(cantidad) as mes_cant
                      FROM movimientos
                      WHERE producto_id=p.id AND tipo='salida'
                      GROUP BY SUBSTR(fecha,4,2)||SUBSTR(fecha,7,4)
                  ) t), 0) as promedio_mensual
           FROM productos p
           WHERE p.activo=TRUE AND p.stock_minimo > 0
             AND p.stock_actual <= p.stock_minimo
           ORDER BY (p.stock_actual * 1.0 / NULLIF(p.stock_minimo,0)) ASC""")
    for p in bajo:
        sugerido = max(0, round((p['promedio_mensual'] or p['stock_minimo']) - p['stock_actual']))
        if sugerido == 0:
            sugerido = p['stock_minimo'] - p['stock_actual']
        p['cantidad_sugerida'] = max(sugerido, p['stock_minimo'])
    return jsonify(bajo)

@app.route('/api/export/reposicion')
@login_required
def export_reposicion():
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    bajo = db_fetchall(
        """SELECT p.nombre, p.categoria, p.unidad, p.stock_actual, p.stock_minimo,
                  COALESCE((SELECT AVG(mes_cant) FROM (
                      SELECT SUM(cantidad) as mes_cant
                      FROM movimientos
                      WHERE producto_id=p.id AND tipo='salida'
                      GROUP BY SUBSTR(fecha,4,2)||SUBSTR(fecha,7,4)
                  ) t), 0) as promedio_mensual
           FROM productos p
           WHERE p.activo=TRUE AND p.stock_minimo > 0
             AND p.stock_actual <= p.stock_minimo
           ORDER BY p.categoria, p.nombre""")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Orden de Reposicion"
    ws.sheet_view.showGridLines = False
    ws.merge_cells("A1:G1")
    c = ws["A1"]
    c.value = f"ORDEN DE REPOSICION - Bodega de Aseo - {datetime.now().strftime('%d-%m-%Y')}"
    c.font = Font(bold=True, size=13, color="FFFFFF")
    c.fill = PatternFill("solid", start_color="0D4F3C", fgColor="0D4F3C")
    c.alignment = Alignment(horizontal="center")
    ws.row_dimensions[1].height = 28
    headers = ["Producto","Categoria","Unidad","Stock actual","Stock minimo","Prom. mensual","Cantidad a pedir"]
    for col,h in enumerate(headers,1):
        cell = ws.cell(row=2,column=col,value=h)
        cell.font = Font(bold=True,color="FFFFFF")
        cell.fill = PatternFill("solid",start_color="1a6b52",fgColor="1a6b52")
        cell.alignment = Alignment(horizontal="center")
        ws.column_dimensions[ws.cell(row=2,column=col).column_letter].width = 18
    for ri,p in enumerate(bajo,3):
        prom = round(p['promedio_mensual'] or 0)
        sugerido = max(p['stock_minimo'] - p['stock_actual'], prom)
        bg = "FCEBEB" if p['stock_actual'] == 0 else "FFF2CC"
        for col,val in enumerate([p['nombre'],p['categoria'],p['unidad'],
                                   p['stock_actual'],p['stock_minimo'],prom,sugerido],1):
            cell = ws.cell(row=ri,column=col,value=val)
            cell.fill = PatternFill("solid",start_color=bg,fgColor=bg)
            if col==7:
                cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center" if col>1 else "left")
    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    return send_file(buf, download_name=f'orden_reposicion_{datetime.now().strftime("%d%m%Y")}.xlsx',
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# ── FACTURAS ──────────────────────────────────────────────────────────────────
@app.route('/facturas')
@admin_required
def facturas_page():
    return render_template('facturas.html', user=session['user'], rol=session['rol'])

@app.route('/api/facturas/parsear', methods=['POST'])
@admin_required
def parsear_factura():
    if 'archivo' not in request.files:
        return jsonify({'error': 'No se envió archivo'}), 400
    file = request.files['archivo']
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Solo se aceptan archivos PDF'}), 400
    try:
        try:
            import pdfplumber
        except ImportError:
            import subprocess, sys
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'pdfplumber', '--break-system-packages', '-q'])
            import pdfplumber

        pdf_bytes = file.read()
        rows_parsed = []
        meta = {'numero': '', 'proveedor': '', 'fecha': '', 'neto': 0, 'iva': 0, 'total': 0}

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page = pdf.pages[0]
            text = page.extract_text() or ''
            lines = [l.strip() for l in text.split('\n') if l.strip()]

            import re
            # N° factura
            m = re.search(r'N[°oº]?\s*(\d+)', text, re.IGNORECASE)
            if m: meta['numero'] = m.group(1)

            # Fecha (08 de Junio del 2026)
            meses_map = {'enero':'01','febrero':'02','marzo':'03','abril':'04','mayo':'05','junio':'06',
                         'julio':'07','agosto':'08','septiembre':'09','octubre':'10','noviembre':'11','diciembre':'12'}
            m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+del?\s+(\d{4})', text, re.IGNORECASE)
            if m:
                d2, mon, y2 = m.group(1), m.group(2).lower(), m.group(3)
                meta['fecha'] = f"{d2.zfill(2)}-{meses_map.get(mon,'01')}-{y2}"
            else:
                # fallback: dd-mm-yyyy
                m = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', text)
                if m: meta['fecha'] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

            # Proveedor: primera linea que no sea RUT ni vacia
            skip_kw = ['R.U.T','RUT:','SEÑOR','GIRO','DIRECC','COMUN','TIPO','FACTURA','SII']
            for ln in lines[:15]:
                if len(ln) > 5 and not any(kw in ln.upper() for kw in skip_kw):
                    meta['proveedor'] = ln
                    break

            # Totales desde texto
            def parse_monto(pattern):
                m2 = re.search(pattern, text, re.IGNORECASE)
                if m2:
                    v = m2.group(1).replace('.','').replace(',','').strip()
                    try: return int(v)
                    except: return 0
                return 0
            meta['neto']  = parse_monto(r'MONTO NETO\s*\$?\s*([\d.,]+)')
            meta['iva']   = parse_monto(r'I\.?V\.?A\.?[^$\n]*\$?\s*([\d.,]+)')
            meta['total'] = parse_monto(r'TOTAL\s*\$?\s*([\d.,]+)')

            # Extraer productos: escaneo derecha-izquierda buscando 3 números al final
            def extraer_numero_cl(tok):
                cleaned = tok.replace('.', '').replace(',', '')
                return int(cleaned) if cleaned.isdigit() else None

            SKIP_PROD_KW = [
                'DESCRIPCION','DESCRIPCIÓN','CANTIDAD','PRECIO','VALOR','CODIGO','CÓDIGO',
                'MONTO NETO','MONTO','I.V.A','IVA','TOTAL','FORMA DE PAGO','FORMA','TIMBRE',
                'SEÑOR','SEÑORES','R.U.T','RUT','GIRO','DIRECC','COMUN','CIUDAD','TIPO',
                'FACTURA','ELECTRONICA','ELECTRÓNICA','S.I.I','SII','IMPUESTO','ADICIONAL',
                'COMERCIALIZADORA','CORPORACION','CORPORACIÓN','COLEGIO','VENTA','ARRIEROS',
                'EMAIL','TELEFONO','TELÉFONO','PEDRO','RESOLUCION','RESOLUCIÓN','VERIFIQUE',
                'FECHA','FOLIO','VENDEDOR','BODEGA','CONDICION','CONDICIÓN','NETO',
                'SUBTOTAL','SUB TOTAL','DESCUENTO','% IMPTO','%IMPTO',
            ]

            for ln in lines:
                ln_s = ln.strip()
                if ln_s.startswith('-'): ln_s = ln_s[1:].strip()
                if not ln_s or len(ln_s) < 5: continue
                if any(kw in ln_s.upper() for kw in SKIP_PROD_KW): continue
                tokens = ln_s.split()
                if len(tokens) < 4: continue
                # Scan right-to-left collecting trailing integers
                trailing = []; text_end = len(tokens)
                for i in range(len(tokens) - 1, -1, -1):
                    n = extraer_numero_cl(tokens[i])
                    if n is not None:
                        trailing.insert(0, n)
                        text_end = i
                        if len(trailing) == 3:
                            break
                    else:
                        if trailing:
                            break  # non-number interrupts sequence
                if len(trailing) != 3:
                    continue
                nombre = ' '.join(tokens[:text_end]).strip()
                if len(nombre) < 3 or not any(c.isalpha() for c in nombre): continue
                cant, precio, valor = trailing[0], trailing[1], trailing[2]
                if cant <= 0 or cant > 9999 or valor <= 0: continue
                rows_parsed.append({
                    'nombre_factura': nombre,
                    'cantidad': cant,
                    'precio_unit': precio,
                    'valor': valor,
                })

        # Obtener productos de bodega para matching
        productos = db_fetchall(
            "SELECT id, nombre, categoria, unidad FROM productos WHERE activo=TRUE ORDER BY nombre")

        def normalizar(s):
            import unicodedata
            s = s.lower().strip()
            s = unicodedata.normalize('NFD', s)
            s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
            return s

        for row in rows_parsed:
            nf = normalizar(row['nombre_factura'])
            best_match = None
            best_score = 0.0
            for p in productos:
                np = normalizar(p['nombre'])
                if nf == np:
                    score = 1.0
                elif nf in np or np in nf:
                    score = 0.85
                else:
                    wf = set(nf.split())
                    wp = set(np.split())
                    overlap = len(wf & wp)
                    score = (overlap / max(len(wf), len(wp))) * 0.7 if overlap else 0
                if score > best_score:
                    best_score = score
                    best_match = p
            if best_match and best_score >= 0.3:
                row['producto_id'] = best_match['id']
                row['producto_nombre'] = best_match['nombre']
                row['match_score'] = round(best_score, 2)
            else:
                row['producto_id'] = None
                row['producto_nombre'] = ''
                row['match_score'] = 0.0

        return jsonify({'ok': True, 'meta': meta, 'rows': rows_parsed, 'productos': productos})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'detalle': traceback.format_exc()}), 500

@app.route('/api/facturas/procesar', methods=['POST'])
@admin_required
def procesar_factura():
    d = request.json
    meta = d.get('meta', {})
    rows = d.get('rows', [])
    fecha_mov = meta.get('fecha') or datetime.now().strftime('%d-%m-%Y')
    num = meta.get('numero', '?')
    proveedor = meta.get('proveedor', '')
    obs_base = f"[Factura N°{num} | {proveedor}]"

    procesados = 0
    errores = []
    conn2, mode2 = get_db()
    cur2 = conn2.cursor()
    try:
        for row in rows:
            pid  = row.get('producto_id')
            cant = int(row.get('cantidad', 0))
            if not pid or cant <= 0:
                continue
            try:
                if mode2 == 'pg':
                    cur2.execute(
                        "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (pid, 'entrada', cant, '', session['user'], obs_base, fecha_mov))
                    cur2.execute(
                        "UPDATE productos SET stock_actual=stock_actual+%s WHERE id=%s", (cant, pid))
                else:
                    cur2.execute(
                        "INSERT INTO movimientos (producto_id,tipo,cantidad,edificio,usuario,observacion,fecha) VALUES (?,?,?,?,?,?,?)",
                        (pid, 'entrada', cant, '', session['user'], obs_base, fecha_mov))
                    cur2.execute(
                        "UPDATE productos SET stock_actual=stock_actual+? WHERE id=?", (cant, pid))
                procesados += 1
            except Exception as e2:
                errores.append(str(e2))

        conn2.commit()
    finally:
        conn2.close()

    return jsonify({'ok': True, 'procesados': procesados, 'errores': errores})


if __name__=='__main__':
    init_db()
    app.run(debug=False,host='0.0.0.0',port=int(os.environ.get('PORT',5000)))
