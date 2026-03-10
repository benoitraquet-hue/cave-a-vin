from flask import Flask, render_template, request, jsonify
import json, os, re, uuid
from datetime import datetime

app = Flask(__name__)

# ── DÉTECTION DB : PostgreSQL en prod, SQLite en local ─────────────────────
DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras
    # Railway fournit postgres:// mais psycopg2 veut postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    def get_db():
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn
    def fetchone(cur): return cur.fetchone()
    def fetchall(cur): return cur.fetchall()
    def lastid(cur): cur.execute('SELECT lastval()'); return cur.fetchone()[0]
    PH = '%s'   # placeholder PostgreSQL
else:
    import sqlite3
    DB = os.path.join(os.path.dirname(__file__), 'cave.db')
    def get_db():
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        return conn
    def fetchone(cur): return cur.fetchone()
    def fetchall(cur): return cur.fetchall()
    def lastid(cur): return cur.lastrowid
    PH = '?'    # placeholder SQLite

def q(sql):
    """Remplace ? par %s si PostgreSQL."""
    return sql.replace('?', PH) if USE_PG else sql

def row2dict(row):
    if row is None: return None
    if USE_PG: return dict(row)
    return dict(row)

def rows2list(rows):
    return [dict(r) for r in rows] if rows else []

def ex(conn, sql, params=()):
    """Execute avec placeholders adaptés."""
    cur = conn.cursor()
    cur.execute(q(sql), params)
    return cur

# ── INIT DB ────────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
    cur = conn.cursor()

    if USE_PG:
        cur.execute('''CREATE TABLE IF NOT EXISTS vins (
            id SERIAL PRIMARY KEY,
            loge_id TEXT, mur INTEGER DEFAULT 0, col_beton INTEGER DEFAULT 0,
            bloc INTEGER DEFAULT 0, loge INTEGER DEFAULT 0,
            domaine TEXT NOT NULL DEFAULT '', appellation TEXT DEFAULT '',
            cuvee TEXT DEFAULT '', millesime TEXT DEFAULT '',
            boire_a_partir INTEGER, boire_avant INTEGER,
            couleur TEXT DEFAULT '', prix_unit REAL DEFAULT 0,
            quantite INTEGER DEFAULT 0, bottles_haut INTEGER DEFAULT 0,
            bottles_bas INTEGER DEFAULT 0, notes TEXT DEFAULT '',
            groupe_id TEXT, loge_slot INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS frigo (
            id SERIAL PRIMARY KEY,
            rangee INTEGER NOT NULL, position INTEGER NOT NULL,
            domaine TEXT DEFAULT '', appellation TEXT DEFAULT '',
            cuvee TEXT DEFAULT '', millesime TEXT DEFAULT '',
            couleur TEXT DEFAULT '', boire_a_partir INTEGER,
            boire_avant INTEGER, prix_unit REAL DEFAULT 0,
            quantite INTEGER DEFAULT 0, notes TEXT DEFAULT '',
            source_loge_id TEXT, source_vin_id INTEGER,
            UNIQUE(rangee, position)
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sorties (
            id SERIAL PRIMARY KEY,
            vin_id INTEGER, loge_id TEXT,
            domaine TEXT, appellation TEXT, millesime TEXT,
            quantite INTEGER, date_sortie TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            motif TEXT DEFAULT ''
        )''')
    else:
        cur.execute('''CREATE TABLE IF NOT EXISTS vins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loge_id TEXT, mur INTEGER DEFAULT 0, col_beton INTEGER DEFAULT 0,
            bloc INTEGER DEFAULT 0, loge INTEGER DEFAULT 0,
            domaine TEXT NOT NULL DEFAULT '', appellation TEXT DEFAULT '',
            cuvee TEXT DEFAULT '', millesime TEXT DEFAULT '',
            boire_a_partir INTEGER, boire_avant INTEGER,
            couleur TEXT DEFAULT '', prix_unit REAL DEFAULT 0,
            quantite INTEGER DEFAULT 0, bottles_haut INTEGER DEFAULT 0,
            bottles_bas INTEGER DEFAULT 0, notes TEXT DEFAULT '',
            groupe_id TEXT, loge_slot INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS frigo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rangee INTEGER NOT NULL, position INTEGER NOT NULL,
            domaine TEXT DEFAULT '', appellation TEXT DEFAULT '',
            cuvee TEXT DEFAULT '', millesime TEXT DEFAULT '',
            couleur TEXT DEFAULT '', boire_a_partir INTEGER,
            boire_avant INTEGER, prix_unit REAL DEFAULT 0,
            quantite INTEGER DEFAULT 0, notes TEXT DEFAULT '',
            source_loge_id TEXT, source_vin_id INTEGER,
            UNIQUE(rangee, position)
        )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sorties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_id INTEGER, loge_id TEXT,
            domaine TEXT, appellation TEXT, millesime TEXT,
            quantite INTEGER, date_sortie TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            motif TEXT DEFAULT ''
        )''')
        # Migrations SQLite
        for sql in [
            "ALTER TABLE vins ADD COLUMN groupe_id TEXT",
            "ALTER TABLE vins ADD COLUMN cuvee TEXT DEFAULT ''",
            "ALTER TABLE frigo ADD COLUMN cuvee TEXT DEFAULT ''",
            "ALTER TABLE vins ADD COLUMN loge_slot INTEGER DEFAULT 0",
        ]:
            try: cur.execute(sql)
            except: pass

    conn.commit()

    # Seed si table vide
    cur.execute('SELECT COUNT(*) FROM vins')
    count = cur.fetchone()[0]
    if count == 0:
        seed_path = os.path.join(os.path.dirname(__file__), 'inventory_wat.json')
        if os.path.exists(seed_path):
            with open(seed_path, encoding='utf-8') as f:
                records = json.load(f)
            for r in records:
                raw = r.get('millesime','')
                millesime_clean, _ = parse_millesime(raw)
                bap = r.get('boire_a_partir')
                cur.execute(q('''INSERT INTO vins
                    (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,millesime,
                     boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''),
                    (r['loge_id'],r['mur'],r['col_beton'],r['bloc'],r['loge'],
                     r['domaine'],r['appellation'],r.get('cuvee',''),millesime_clean,
                     bap, r.get('boire_avant'), r.get('couleur',''), r.get('prix_unit',0) or 0,
                     r['quantite'], r.get('bottles_haut',0), r.get('bottles_bas',0)))
            conn.commit()

    cur.close()
    conn.close()

def parse_millesime(raw):
    if not raw: return '', None
    raw = str(raw).strip()
    m = re.match(r'\((\d{4})\)\s*(\d{4})', raw)
    if m: return m.group(2), int(m.group(1))
    m2 = re.search(r'(\d{4})', raw)
    return (m2.group(1) if m2 else raw), None

def bottles_from_qte(qte):
    qte = max(0, min(6, qte))
    return min(3, qte), max(0, qte - 3)

@app.route('/')
def index(): return render_template('index.html')

# ── CAVE STRUCTURE ─────────────────────────────────────────────────────────
@app.route('/api/cave_structure')
def cave_structure():
    path = os.path.join(os.path.dirname(__file__), 'cave_structure.json')
    with open(path, encoding='utf-8') as f: static = json.load(f)
    conn = get_db()
    cur = ex(conn, 'SELECT * FROM vins ORDER BY loge_id, loge_slot, id')
    rows = rows2list(fetchall(cur)); cur.close(); conn.close()
    from collections import defaultdict
    by_loge = defaultdict(list)
    for r in rows:
        if r['loge_id']:
            by_loge[r['loge_id']].append(r)
    for bloc_id, bloc in static['blocs'].items():
        for lk in ['L1','L2','L3','L4']:
            loge = bloc.get(lk)
            if not loge: continue
            lid = loge.get('loge_id')
            if not lid or lid not in by_loge: continue
            vins = by_loge[lid]
            total_qte = sum(v['quantite'] for v in vins)
            if len(vins) == 1 and vins[0]['quantite'] > 0:
                db = vins[0]
                loge.update({k: db[k] for k in ['quantite','bottles_haut','bottles_bas','domaine',
                    'appellation','cuvee','millesime','boire_a_partir','boire_avant','prix_unit','couleur','notes']})
                loge['db_id'] = db['id']; loge['est_vide'] = False; loge['multi'] = False
            elif len(vins) > 1 and total_qte > 0:
                active = [v for v in vins if v['quantite'] > 0]
                loge['multi'] = True; loge['multi_vins'] = active
                loge['quantite'] = total_qte; loge['db_id'] = active[0]['id'] if active else None
                loge['est_vide'] = False; loge['domaine'] = f"{len(active)} vins"
                loge['couleur'] = active[0]['couleur'] if active else ''
                bh, bb = bottles_from_qte(min(6, total_qte))
                loge['bottles_haut'] = bh; loge['bottles_bas'] = bb
            else:
                loge['est_vide'] = True; loge['multi'] = False
    return jsonify(static)

# ── STATS ──────────────────────────────────────────────────────────────────
@app.route('/api/stats')
def stats():
    conn = get_db(); an = datetime.now().year
    def scalar(sql, params=()): cur=ex(conn,sql,params); v=cur.fetchone()[0]; cur.close(); return v
    def one(sql, params=()): cur=ex(conn,sql,params); v=cur.fetchone(); cur.close(); return row2dict(v) if v else None
    def many(sql, params=()): cur=ex(conn,sql,params); v=rows2list(fetchall(cur)); cur.close(); return v

    total  = scalar('SELECT COALESCE(SUM(quantite),0) FROM vins')
    valeur = scalar("SELECT COALESCE(SUM(quantite*prix_unit),0) FROM vins WHERE prix_unit>0")
    doms   = scalar("SELECT COUNT(DISTINCT domaine) FROM vins WHERE quantite>0 AND domaine!=''")
    refs   = scalar('SELECT COUNT(*) FROM vins WHERE quantite>0')
    alert  = scalar('SELECT COUNT(*) FROM vins WHERE quantite>0 AND boire_a_partir IS NOT NULL AND boire_a_partir<=?',(an,))
    frigo_total = scalar('SELECT COALESCE(SUM(quantite),0) FROM frigo')
    plus_chere  = one("SELECT domaine,appellation,millesime,prix_unit FROM vins WHERE quantite>0 AND prix_unit>0 ORDER BY prix_unit DESC LIMIT 1")
    # Millésime le plus ancien — GLOB en SQLite, SIMILAR en PG
    if USE_PG:
        plus_vieille = one("SELECT domaine,appellation,millesime FROM vins WHERE quantite>0 AND millesime IS NOT NULL AND millesime!='' AND millesime ~ '^[0-9]{4}$' ORDER BY millesime ASC LIMIT 1")
        decennies_raw = many("SELECT millesime, SUM(quantite) as total FROM vins WHERE quantite>0 AND millesime IS NOT NULL AND millesime!='' AND millesime ~ '^[0-9]{4}$' GROUP BY millesime ORDER BY millesime")
        bues_annee = scalar("SELECT COALESCE(SUM(quantite),0) FROM sorties WHERE EXTRACT(YEAR FROM date_sortie)=?",(an,))
    else:
        plus_vieille = one("SELECT domaine,appellation,millesime FROM vins WHERE quantite>0 AND millesime IS NOT NULL AND millesime!='' AND millesime GLOB '[0-9][0-9][0-9][0-9]' ORDER BY millesime ASC LIMIT 1")
        decennies_raw = many("SELECT millesime, SUM(quantite) as total FROM vins WHERE quantite>0 AND millesime IS NOT NULL AND millesime!='' AND millesime GLOB '[0-9][0-9][0-9][0-9]' GROUP BY millesime ORDER BY millesime")
        bues_annee = scalar("SELECT COALESCE(SUM(quantite),0) FROM sorties WHERE strftime('%Y', date_sortie)=?",(str(an),))
    val_moy = round(float(valeur)/max(1,int(total)),2)
    top_domaines = many("SELECT domaine, SUM(quantite) as total FROM vins WHERE quantite>0 AND domaine!='' GROUP BY domaine ORDER BY total DESC LIMIT 5")
    couleurs     = many("SELECT couleur, SUM(quantite) as total FROM vins WHERE quantite>0 AND couleur!='' GROUP BY couleur ORDER BY total DESC")
    top_appels   = many("SELECT appellation, SUM(quantite) as total FROM vins WHERE quantite>0 AND appellation!='' GROUP BY appellation ORDER BY total DESC LIMIT 5")
    decennies = {}
    for r in decennies_raw:
        try:
            d=(int(r['millesime'])//10)*10; k=f"{d}s"
            decennies[k]=decennies.get(k,0)+r['total']
        except: pass
    bientot = scalar('SELECT COUNT(*) FROM vins WHERE quantite>0 AND boire_avant IS NOT NULL AND boire_avant<=?',(an+1,))
    conn.close()
    return jsonify({'total_bouteilles':int(total),'valeur_totale':round(float(valeur),2),
        'valeur_moyenne':val_moy,'nb_domaines':int(doms),'nb_refs':int(refs),
        'alertes':int(alert),'alertes_bientot':int(bientot),'frigo_total':int(frigo_total),
        'bues_annee':int(bues_annee),'plus_chere':plus_chere,'plus_vieille':plus_vieille,
        'top_domaines':top_domaines,'couleurs':couleurs,'top_appels':top_appels,'decennies':decennies})

# ── INVENTAIRE ─────────────────────────────────────────────────────────────
@app.route('/api/vins')
def get_vins():
    conn = get_db()
    q_str = request.args.get('q',''); alerte = request.args.get('alerte','')
    sort  = request.args.get('sort','domaine')
    direction = 'DESC' if request.args.get('dir',1,type=int)<0 else 'ASC'
    an = datetime.now().year
    sql = "SELECT * FROM vins WHERE quantite>0 AND (domaine!='' OR appellation='divers')"; p = []
    if q_str:
        sql += ' AND (domaine LIKE ? OR appellation LIKE ? OR millesime LIKE ?)'
        p += [f'%{q_str}%']*3
    if alerte=='1':
        sql += ' AND (boire_a_partir IS NOT NULL AND boire_a_partir<=?)'; p += [an]
    allowed = ['domaine','appellation','millesime','boire_avant','boire_a_partir','prix_unit','quantite','mur','bloc','couleur']
    sql += f' ORDER BY {sort} {direction}' if sort in allowed else ' ORDER BY domaine ASC'
    cur = ex(conn, sql, p); rows = rows2list(fetchall(cur)); cur.close(); conn.close()
    seen_groupes = {}; result = []
    for r in rows:
        grp = r.get('groupe_id')
        if grp and grp in seen_groupes:
            seen_groupes[grp]['quantite'] += r['quantite']
            seen_groupes[grp]['loges_ids'].append(r['loge_id'])
        else:
            r['loges_ids'] = [r['loge_id']] if r.get('loge_id') else []
            r['is_divers'] = (not r.get('domaine') and r.get('appellation') == 'divers')
            if grp: seen_groupes[grp] = r
            result.append(r)
    return jsonify(result)

@app.route('/api/vins/<int:vid>', methods=['GET'])
def get_vin(vid):
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vid,))
    row=row2dict(cur.fetchone()); cur.close(); conn.close()
    return jsonify(row) if row else (jsonify({'error':'Not found'}),404)

@app.route('/api/vins/<int:vid>/groupe')
def get_groupe(vid):
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vid,))
    row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify([])
    grp=row.get('groupe_id')
    if grp:
        cur=ex(conn,'SELECT * FROM vins WHERE groupe_id=? ORDER BY id',(grp,))
        members=rows2list(fetchall(cur)); cur.close(); conn.close(); return jsonify(members)
    conn.close(); return jsonify([row])

@app.route('/api/autocomplete')
def autocomplete():
    field=request.args.get('field','domaine'); q_str=request.args.get('q','')
    if field not in ('domaine','appellation'): return jsonify([])
    conn=get_db()
    cur=ex(conn,f"SELECT DISTINCT {field} FROM vins WHERE {field} LIKE ? ORDER BY {field} LIMIT 10",(f'%{q_str}%',))
    rows=fetchall(cur); cur.close(); conn.close()
    return jsonify([r[0] for r in rows if r[0]])

@app.route('/api/vins', methods=['POST'])
def add_vin():
    data=request.json
    qte=int(data.get('quantite',0) or 0); prix=float(data.get('prix_unit',0) or 0)
    bap=int(data['boire_a_partir']) if data.get('boire_a_partir') else None
    ba=int(data['boire_avant'])     if data.get('boire_avant')    else None
    loges=data.get('loges',[]); groupe_id=str(uuid.uuid4())[:8] if len(loges)>1 else None
    conn=get_db(); first_id=None; reste=qte
    if loges:
        for lid in loges:
            qte_loge=min(6,reste); reste-=qte_loge; bh,bb=bottles_from_qte(qte_loge)
            parts=re.match(r'M(\d+)-C(\d+)-B(\d+)-L(\d+)',lid)
            mur,col,bloc,loge=(int(parts.group(i)) for i in range(1,5)) if parts else (0,0,0,0)
            cur=ex(conn,'SELECT id FROM vins WHERE loge_id=?',(lid,)); ex_row=cur.fetchone(); cur.close()
            if ex_row:
                ex(conn,'''UPDATE vins SET domaine=?,appellation=?,cuvee=?,millesime=?,boire_a_partir=?,
                    boire_avant=?,couleur=?,prix_unit=?,quantite=?,bottles_haut=?,bottles_bas=?,notes=?,groupe_id=? WHERE id=?''',
                    (data['domaine'],data.get('appellation',''),data.get('cuvee',''),data.get('millesime',''),bap,ba,
                     data.get('couleur',''),prix,qte_loge,bh,bb,data.get('notes',''),groupe_id,ex_row[0]))
                if not first_id: first_id=ex_row[0]
            else:
                cur=ex(conn,'''INSERT INTO vins (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,
                    millesime,boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas,notes,groupe_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (lid,mur,col,bloc,loge,data['domaine'],data.get('appellation',''),data.get('cuvee',''),data.get('millesime',''),
                     bap,ba,data.get('couleur',''),prix,qte_loge,bh,bb,data.get('notes',''),groupe_id))
                if not first_id: first_id=lastid(cur); cur.close()
    else:
        bh,bb=bottles_from_qte(qte)
        cur=ex(conn,'''INSERT INTO vins (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,
            millesime,boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas,notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (None,0,0,0,0,data['domaine'],data.get('appellation',''),data.get('cuvee',''),data.get('millesime',''),
             bap,ba,data.get('couleur',''),prix,qte,bh,bb,data.get('notes','')))
        first_id=lastid(cur); cur.close()
    conn.commit(); conn.close()
    return jsonify({'id':first_id,'message':'Ajouté'}),201

@app.route('/api/vins/<int:vid>', methods=['PUT'])
def update_vin(vid):
    data=request.json; qte=int(data.get('quantite',0) or 0); prix=float(data.get('prix_unit',0) or 0)
    bap=int(data['boire_a_partir']) if data.get('boire_a_partir') else None
    ba=int(data['boire_avant'])     if data.get('boire_avant')    else None
    loges=data.get('loges',None)
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vid,)); row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify({'error':'Not found'}),404
    grp=row.get('groupe_id')
    def clear_group(g, single_id=None):
        if g: ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,notes='',groupe_id=NULL WHERE groupe_id=?''',(g,))
        elif single_id: ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,notes='',groupe_id=NULL WHERE id=?''',(single_id,))
    if loges is not None:
        clear_group(grp, vid); new_grp=str(uuid.uuid4())[:8] if len(loges)>1 else None; reste=qte; first_id=None
        for lid in loges:
            qte_loge=min(6,reste); reste-=qte_loge; bh,bb=bottles_from_qte(qte_loge)
            parts=re.match(r'M(\d+)-C(\d+)-B(\d+)-L(\d+)',lid)
            mur,col,bloc,loge=(int(parts.group(i)) for i in range(1,5)) if parts else (0,0,0,0)
            cur=ex(conn,'SELECT id FROM vins WHERE loge_id=?',(lid,)); ex_row=cur.fetchone(); cur.close()
            if ex_row:
                ex(conn,'''UPDATE vins SET domaine=?,appellation=?,cuvee=?,millesime=?,boire_a_partir=?,
                    boire_avant=?,couleur=?,prix_unit=?,quantite=?,bottles_haut=?,bottles_bas=?,notes=?,groupe_id=? WHERE id=?''',
                    (data['domaine'],data.get('appellation',''),data.get('cuvee',''),data.get('millesime',''),bap,ba,
                     data.get('couleur',''),prix,qte_loge,bh,bb,data.get('notes',''),new_grp,ex_row[0]))
                if not first_id: first_id=ex_row[0]
            else:
                cur=ex(conn,'''INSERT INTO vins (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,
                    millesime,boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas,notes,groupe_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (lid,mur,col,bloc,loge,data['domaine'],data.get('appellation',''),data.get('cuvee',''),data.get('millesime',''),
                     bap,ba,data.get('couleur',''),prix,qte_loge,bh,bb,data.get('notes',''),new_grp))
                if not first_id: first_id=lastid(cur); cur.close()
    else:
        if grp:
            cur=ex(conn,'SELECT id,quantite FROM vins WHERE groupe_id=? ORDER BY id',(grp,))
            members=rows2list(fetchall(cur)); cur.close(); reste=qte
            for m in members:
                qte_loge=min(6,reste); reste-=qte_loge; bh,bb=bottles_from_qte(qte_loge)
                ex(conn,'''UPDATE vins SET domaine=?,appellation=?,millesime=?,boire_a_partir=?,
                    boire_avant=?,couleur=?,prix_unit=?,quantite=?,bottles_haut=?,bottles_bas=?,notes=? WHERE id=?''',
                    (data['domaine'],data.get('appellation',''),data.get('millesime',''),bap,ba,
                     data.get('couleur',''),prix,qte_loge,bh,bb,data.get('notes',''),m['id']))
        else:
            bh,bb=bottles_from_qte(qte)
            ex(conn,'''UPDATE vins SET domaine=?,appellation=?,millesime=?,boire_a_partir=?,
                boire_avant=?,couleur=?,prix_unit=?,quantite=?,bottles_haut=?,bottles_bas=?,notes=? WHERE id=?''',
                (data['domaine'],data.get('appellation',''),data.get('millesime',''),bap,ba,
                 data.get('couleur',''),prix,qte,bh,bb,data.get('notes',''),vid))
    conn.commit(); conn.close(); return jsonify({'message':'Mis à jour'})

@app.route('/api/vins/<int:vid>', methods=['DELETE'])
def delete_vin(vid):
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vid,)); row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify({'error':'Not found'}),404
    grp=row.get('groupe_id')
    if grp:
        ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,notes='',groupe_id=NULL WHERE groupe_id=?''',(grp,))
    elif row.get('loge_id'):
        ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,notes='',groupe_id=NULL WHERE id=?''',(vid,))
    else:
        ex(conn,'DELETE FROM vins WHERE id=?',(vid,))
    conn.commit(); conn.close(); return jsonify({'message':'Supprimé'})

@app.route('/api/sortir/<int:vid>', methods=['POST'])
def sortir_vin(vid):
    data=request.json; nb=int(data.get('nb',1))
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vid,)); row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify({'error':'Not found'}),404
    nqte=max(0,row['quantite']-nb); bh,bb=bottles_from_qte(nqte)
    ex(conn,'INSERT INTO sorties (vin_id,loge_id,domaine,appellation,millesime,quantite) VALUES (?,?,?,?,?,?)',
        (vid,row['loge_id'],row['domaine'],row['appellation'],row['millesime'],nb))
    if nqte==0:
        ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,
            notes='',groupe_id=NULL WHERE id=?''',(vid,))
    else:
        ex(conn,'UPDATE vins SET quantite=?,bottles_haut=?,bottles_bas=? WHERE id=?',(nqte,bh,bb,vid))
    conn.commit(); conn.close()
    return jsonify({'nouvelle_qte':nqte,'message':f'{nb} bouteille(s) sorties'})

@app.route('/api/emplacements_libres')
def emplacements_libres():
    conn=get_db()
    cur=ex(conn,"SELECT DISTINCT loge_id FROM vins WHERE loge_id IS NOT NULL AND quantite>0 AND domaine!='' AND appellation!='divers'")
    occ=set(r[0] for r in fetchall(cur)); cur.close(); conn.close()
    loge_labels={1:'L1↖',2:'L2↗',3:'L3↙',4:'L4↘'}
    libres=[]
    for mur in [1,2]:
        for col in range(1,4):
            for bloc in range(1,5):
                for loge in range(1,5):
                    lid=f"M{mur}-C{col}-B{bloc}-L{loge}"
                    if lid not in occ:
                        libres.append({'loge_id':lid,'mur':mur,'col_beton':col,'bloc':bloc,'loge':loge,
                            'label':f'Mur {mur} — Col {col} — Bloc {bloc} — {loge_labels[loge]}',
                            'qte_actuelle':0,'place_restante':6,'semi_occupe':False})
    return jsonify(libres)

@app.route('/api/loge/<path:loge_id>/vins')
def get_loge_vins(loge_id):
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE loge_id=? AND quantite>0 ORDER BY loge_slot,id',(loge_id,))
    rows=rows2list(fetchall(cur)); cur.close(); conn.close()
    return jsonify(rows)

@app.route('/api/loge/<path:loge_id>/partager', methods=['POST'])
def partager_loge(loge_id):
    data=request.json; conn=get_db()
    cur=ex(conn,'SELECT COALESCE(SUM(quantite),0) FROM vins WHERE loge_id=?',(loge_id,))
    total=cur.fetchone()[0]; cur.close()
    qte=int(data.get('quantite',1) or 1)
    if total+qte > 6: conn.close(); return jsonify({'error':f'Pas assez de place (max 6 btl, occupé: {total})'}),400
    cur=ex(conn,'SELECT COALESCE(MAX(loge_slot),0) FROM vins WHERE loge_id=?',(loge_id,))
    max_slot=cur.fetchone()[0]; cur.close()
    parts=re.match(r'M(\d+)-C(\d+)-B(\d+)-L(\d+)',loge_id)
    mur,col,bloc,loge=(int(parts.group(i)) for i in range(1,5)) if parts else (0,0,0,0)
    bap=int(data['boire_a_partir']) if data.get('boire_a_partir') else None
    ba=int(data['boire_avant']) if data.get('boire_avant') else None
    bh,bb=bottles_from_qte(qte)
    cur=ex(conn,'''INSERT INTO vins (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,
        millesime,boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas,notes,loge_slot)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (loge_id,mur,col,bloc,loge,data['domaine'],data.get('appellation',''),data.get('cuvee',''),
         data.get('millesime',''),bap,ba,data.get('couleur',''),float(data.get('prix_unit',0) or 0),
         qte,bh,bb,data.get('notes',''),max_slot+1))
    new_id=lastid(cur); cur.close()
    conn.commit(); conn.close()
    return jsonify({'id':new_id,'message':'Vin ajouté dans la case'}),201

# ── FRIGO ──────────────────────────────────────────────────────────────────
FRIGO_RANGEES=3; FRIGO_TOTAL=50; FRIGO_PAR_RANGEE=[17,17,16]

@app.route('/api/frigo')
def get_frigo():
    conn=get_db(); cur=ex(conn,'SELECT * FROM frigo ORDER BY rangee, position')
    rows=rows2list(fetchall(cur)); cur.close(); conn.close()
    grille={r:{} for r in range(1,FRIGO_RANGEES+1)}
    for row in rows: grille[row['rangee']][row['position']]=row
    return jsonify({'grille':grille,'capacite':FRIGO_PAR_RANGEE,'total':FRIGO_TOTAL})

@app.route('/api/frigo/transferer', methods=['POST'])
def transferer_vers_frigo():
    data=request.json; vin_id=int(data['vin_id']); nb=int(data.get('nb',1))
    conn=get_db(); cur=ex(conn,'SELECT * FROM vins WHERE id=?',(vin_id,)); row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify({'error':'Not found'}),404
    if nb > row['quantite']: conn.close(); return jsonify({'error':'Pas assez de bouteilles'}),400
    cuvee_val=row.get('cuvee','')
    cur=ex(conn,'SELECT rangee,position FROM frigo ORDER BY rangee,position')
    occ=set((r[0],r[1]) for r in fetchall(cur)); cur.close()
    slots=[]
    for rangee_idx,cap in enumerate(FRIGO_PAR_RANGEE):
        r=rangee_idx+1
        for pos in range(1,cap+1):
            if (r,pos) not in occ:
                slots.append((r,pos)); occ.add((r,pos))
                if len(slots)==nb: break
        if len(slots)==nb: break
    if len(slots)<nb: conn.close(); return jsonify({'error':'Pas assez de place dans le frigo'}),400
    nqte=row['quantite']-nb; bh,bb=bottles_from_qte(nqte)
    if nqte==0:
        ex(conn,'''UPDATE vins SET domaine='',appellation='',cuvee='',millesime='',boire_a_partir=NULL,
            boire_avant=NULL,couleur='',prix_unit=0,quantite=0,bottles_haut=0,bottles_bas=0,
            notes='',groupe_id=NULL WHERE id=?''',(vin_id,))
    else:
        ex(conn,'UPDATE vins SET quantite=?,bottles_haut=?,bottles_bas=? WHERE id=?',(nqte,bh,bb,vin_id))
    frigo_ids=[]
    for slot in slots:
        cur=ex(conn,'''INSERT INTO frigo (rangee,position,domaine,appellation,cuvee,millesime,couleur,
            boire_a_partir,boire_avant,prix_unit,quantite,notes,source_loge_id,source_vin_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?)''',
            (slot[0],slot[1],row['domaine'],row['appellation'],cuvee_val,row['millesime'],row['couleur'],
             row['boire_a_partir'],row['boire_avant'],row['prix_unit'],row['notes'],row['loge_id'],vin_id))
        frigo_ids.append(lastid(cur)); cur.close()
    conn.commit(); conn.close()
    return jsonify({'message':f'{nb} btl transférée(s) au frigo','frigo_ids':frigo_ids})

@app.route('/api/frigo/<int:fid>/retour_cave', methods=['POST'])
def retour_cave(fid):
    data=request.json; loge_cible=data.get('loge_id')
    conn=get_db(); cur=ex(conn,'SELECT * FROM frigo WHERE id=?',(fid,)); frigo_row=row2dict(cur.fetchone()); cur.close()
    if not frigo_row: conn.close(); return jsonify({'error':'Not found'}),404
    ex(conn,'DELETE FROM frigo WHERE id=?',(fid,)); placed_in=None
    cur=ex(conn,"""SELECT * FROM vins WHERE domaine=? AND appellation=? AND millesime=?
           AND loge_id IS NOT NULL AND quantite>0 AND quantite<6 ORDER BY quantite DESC""",
        (frigo_row['domaine'],frigo_row['appellation'],frigo_row['millesime']))
    same_wine=row2dict(cur.fetchone()); cur.close()
    if same_wine:
        nqte_cave=min(6,same_wine['quantite']+1); bh,bb=bottles_from_qte(nqte_cave)
        ex(conn,'UPDATE vins SET quantite=?,bottles_haut=?,bottles_bas=? WHERE id=?',(nqte_cave,bh,bb,same_wine['id']))
        placed_in=same_wine['loge_id']
    elif loge_cible:
        parts=re.match(r'M(\d+)-C(\d+)-B(\d+)-L(\d+)',loge_cible)
        mur,col,bloc,loge=(int(parts.group(i)) for i in range(1,5)) if parts else (0,0,0,0)
        cur=ex(conn,'SELECT * FROM vins WHERE loge_id=?',(loge_cible,)); ex_row=row2dict(cur.fetchone()); cur.close()
        cuvee_val=frigo_row.get('cuvee',''); bh,bb=bottles_from_qte(1)
        if ex_row and ex_row['quantite']>0:
            nqte_cave=min(6,ex_row['quantite']+1); bh2,bb2=bottles_from_qte(nqte_cave)
            ex(conn,'UPDATE vins SET quantite=?,bottles_haut=?,bottles_bas=? WHERE id=?',(nqte_cave,bh2,bb2,ex_row['id']))
        elif ex_row:
            ex(conn,'''UPDATE vins SET domaine=?,appellation=?,cuvee=?,millesime=?,boire_a_partir=?,
                boire_avant=?,couleur=?,prix_unit=?,quantite=1,bottles_haut=?,bottles_bas=?,notes=? WHERE id=?''',
                (frigo_row['domaine'],frigo_row['appellation'],cuvee_val,frigo_row['millesime'],
                 frigo_row['boire_a_partir'],frigo_row['boire_avant'],frigo_row['couleur'],
                 frigo_row['prix_unit'],bh,bb,frigo_row['notes'],ex_row['id']))
        else:
            ex(conn,'''INSERT INTO vins (loge_id,mur,col_beton,bloc,loge,domaine,appellation,cuvee,
                millesime,boire_a_partir,boire_avant,couleur,prix_unit,quantite,bottles_haut,bottles_bas,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)''',
                (loge_cible,mur,col,bloc,loge,frigo_row['domaine'],frigo_row['appellation'],cuvee_val,
                 frigo_row['millesime'],frigo_row['boire_a_partir'],frigo_row['boire_avant'],
                 frigo_row['couleur'],frigo_row['prix_unit'],bh,bb,frigo_row['notes']))
        placed_in=loge_cible
    conn.commit(); conn.close()
    return jsonify({'message':f'1 btl remise en cave ({placed_in})' if same_wine else '1 btl retournée en cave',
        'placed_in':placed_in,'auto_placed':same_wine is not None})

@app.route('/api/frigo/<int:fid>/sortir', methods=['POST'])
def sortir_frigo(fid):
    conn=get_db(); cur=ex(conn,'SELECT * FROM frigo WHERE id=?',(fid,)); row=row2dict(cur.fetchone()); cur.close()
    if not row: conn.close(); return jsonify({'error':'Not found'}),404
    ex(conn,'DELETE FROM frigo WHERE id=?',(fid,))
    ex(conn,'INSERT INTO sorties (loge_id,domaine,appellation,millesime,quantite,motif) VALUES (?,?,?,?,?,?)',
        ('frigo',row['domaine'],row['appellation'],row['millesime'],1,'bu depuis frigo'))
    conn.commit(); conn.close()
    return jsonify({'message':'1 bouteille sortie'})

@app.route('/api/frigo/<int:fid>/same_wine_cave')
def same_wine_cave(fid):
    conn=get_db(); cur=ex(conn,'SELECT * FROM frigo WHERE id=?',(fid,)); frigo_row=row2dict(cur.fetchone()); cur.close()
    if not frigo_row: conn.close(); return jsonify(None)
    cur=ex(conn,"""SELECT * FROM vins WHERE domaine=? AND appellation=? AND millesime=?
           AND loge_id IS NOT NULL AND quantite>0 AND quantite<6 ORDER BY quantite DESC LIMIT 1""",
        (frigo_row['domaine'],frigo_row['appellation'],frigo_row['millesime']))
    same=row2dict(cur.fetchone()); cur.close(); conn.close()
    return jsonify(same)

@app.route('/api/vivino_search')
def vivino_search():
    import urllib.request as urllib2, urllib.parse as urlparse
    domaine=request.args.get('domaine','').strip(); appellation=request.args.get('appellation','').strip()
    millesime=request.args.get('millesime','').strip()
    if not domaine and not appellation: return jsonify({'error':'Pas assez d\'infos'}),400
    q_str=' '.join(filter(None,[domaine,appellation]))
    url=f'https://www.vivino.com/search/wines?q={urlparse.quote(q_str)}'
    api_url=f'https://www.vivino.com/api/explore/explore?q={urlparse.quote(q_str)}&currency_code=EUR&language=fr&page=1'
    try:
        req=urllib2.Request(api_url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json','Accept-Language':'fr-FR,fr;q=0.9'})
        with urllib2.urlopen(req,timeout=10) as resp: data=json.loads(resp.read())
        matches=data.get('explore_vintage',{}).get('matches',[])
        if not matches: return jsonify({'error':'Aucun vin trouvé','search_url':url}),404
        best=next((m for m in matches if str(m.get('vintage',{}).get('year',''))==millesime),matches[0]) if millesime else matches[0]
        v=best.get('vintage',{}); w=v.get('wine',{}); st=v.get('statistics',{}); wst=w.get('statistics',{})
        price_obj=best.get('price',{}); wine_slug=w.get('seo_name',''); wine_id=w.get('id','')
        note=st.get('ratings_average') or wst.get('ratings_average'); nb_avis=st.get('ratings_count') or wst.get('ratings_count')
        prix=price_obj.get('amount') if price_obj else None
        return jsonify({'note_vivino':round(float(note),1) if note else None,'nb_avis':nb_avis,
            'prix_estime':round(float(prix),2) if prix else None,'millesime_trouve':v.get('year',''),
            'nom_vin':w.get('name',''),'producteur':w.get('winery',{}).get('name',''),
            'appellation':w.get('region',{}).get('name','') if w.get('region') else None,
            'url_vivino':f"https://www.vivino.com/wines/{wine_slug or wine_id}" if (wine_slug or wine_id) else url,
            'nb_resultats':len(matches)})
    except Exception as e: return jsonify({'error':str(e),'search_url':url}),500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False)
