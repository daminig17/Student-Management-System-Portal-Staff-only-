import os, io, sqlite3, traceback
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "portal.db")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
TEMPLATE_DIR = os.path.join(BASE_DIR, "sample_data")
os.makedirs(UPLOAD_DIR, exist_ok=True)
ALLOWED = {".xlsx", ".csv"}

app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-in-prod"

# ---------------- DB helpers ----------------
def get_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con

def run_schema():
    with get_db() as con:
        con.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE NOT NULL,
          password TEXT NOT NULL,
          role TEXT NOT NULL CHECK(role IN ('admin'))
        );

        CREATE TABLE IF NOT EXISTS uploads(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          filename TEXT NOT NULL,
          upload_type TEXT NOT NULL, -- Students, Attendance, Marks
          row_count INTEGER DEFAULT 0,
          created_at TEXT NOT NULL,
          uploader_username TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS students(
          roll_no TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT,
          phone TEXT,
          father_name TEXT,
          father_phone TEXT,
          semester INTEGER
        );

        CREATE TABLE IF NOT EXISTS upload_students_map(
          upload_id INTEGER NOT NULL REFERENCES uploads(id) ON DELETE CASCADE,
          roll_no TEXT NOT NULL,
          created_new INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS attendance(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          roll_no TEXT NOT NULL,
          subject TEXT NOT NULL,
          attended INTEGER DEFAULT 0,
          total INTEGER DEFAULT 0,
          semester INTEGER,
          source_upload_id INTEGER REFERENCES uploads(id) ON DELETE SET NULL,
          UNIQUE(roll_no, subject),
          FOREIGN KEY (roll_no) REFERENCES students(roll_no) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS marks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          roll_no TEXT NOT NULL,
          exam TEXT NOT NULL,
          subject TEXT NOT NULL,
          max_marks INTEGER NOT NULL,
          marks_obtained INTEGER NOT NULL,
          credits INTEGER,
          semester INTEGER,
          source_upload_id INTEGER REFERENCES uploads(id) ON DELETE SET NULL,
          UNIQUE(roll_no, exam, subject),
          FOREIGN KEY (roll_no) REFERENCES students(roll_no) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS remarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          roll_no TEXT NOT NULL,
          remark_text TEXT NOT NULL,
          author_username TEXT NOT NULL,
          created_at TEXT NOT NULL,
          FOREIGN KEY (roll_no) REFERENCES students(roll_no) ON DELETE CASCADE
        );
        """)
        # Migrate older DBs: drop photo if exists (can't drop easily; just ignore), ensure missing cols exist
        cur = con.cursor()
        cur.execute("PRAGMA table_info(students)"); scols=[r[1] for r in cur.fetchall()]
        for col in [("phone","TEXT"),("father_name","TEXT"),("father_phone","TEXT")]:
            if col[0] not in scols:
                con.execute(f"ALTER TABLE students ADD COLUMN {col[0]} {col[1]}")
        # provenance columns
        cur.execute("PRAGMA table_info(attendance)"); acols=[r[1] for r in cur.fetchall()]
        if "source_upload_id" not in acols:
            con.execute("ALTER TABLE attendance ADD COLUMN source_upload_id INTEGER REFERENCES uploads(id) ON DELETE SET NULL")
        cur.execute("PRAGMA table_info(marks)"); mcols=[r[1] for r in cur.fetchall()]
        if "source_upload_id" not in mcols:
            con.execute("ALTER TABLE marks ADD COLUMN source_upload_id INTEGER REFERENCES uploads(id) ON DELETE SET NULL")
        con.commit()

def ensure_admin():
    with get_db() as con:
        r = con.execute("SELECT 1 FROM users WHERE username='admin'").fetchone()
        if not r:
            con.execute("INSERT INTO users(username,password,role) VALUES(?,?,?)",
                        ("admin", generate_password_hash("admin123"), "admin"))
            con.commit()

# --------------- Utils ---------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: str(c).strip().lower())
    aliases = {
        "roll":"roll_no","roll no":"roll_no","rollno":"roll_no","rollnumber":"roll_no","roll_no.":"roll_no",
        "student name":"name","student":"name","name of student":"name",
        "student email":"email","mail":"email","e-mail":"email",
        "student phone number":"phone","phone number":"phone","mobile":"phone","student phone":"phone","contact":"phone",
        "father name":"father_name","father's name":"father_name","guardian name":"father_name",
        "father phone number":"father_phone","father mobile":"father_phone","guardian phone":"father_phone",
        "sem":"semester","sub":"subject","subject name":"subject",
        "max":"max_marks","maxmarks":"max_marks","marks":"marks_obtained","obtained":"marks_obtained"
    }
    for k, v in aliases.items():
        if k in df.columns and v not in df.columns:
            df[v] = df.pop(k)
    return df

def load_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsx":
        return pd.read_excel(path, sheet_name=0)
    if ext == ".csv":
        return pd.read_csv(path)
    raise ValueError("Only .xlsx or .csv supported.")

def save_upload(fs):
    ext = os.path.splitext(fs.filename)[1].lower()
    if ext not in {".xlsx", ".csv"}:
        raise ValueError("Only .xlsx or .csv allowed.")
    name = f"{int(pd.Timestamp.now().timestamp()*1000)}_{secure_filename(fs.filename)}"
    full = os.path.join(UPLOAD_DIR, name)
    fs.save(full)
    return full, name

def create_upload_record(filename: str, upload_type: str) -> int:
    with get_db() as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO uploads(filename, upload_type, row_count, created_at, uploader_username) VALUES(?,?,?,?,?)",
            (filename, upload_type, 0, datetime.now().isoformat(timespec='seconds'), session.get("username", "admin")),
        )
        con.commit()
        return cur.lastrowid

def bump_rowcount(upload_id: int, n: int):
    with get_db() as con:
        con.execute("UPDATE uploads SET row_count=row_count+? WHERE id=?", (n, upload_id))
        con.commit()

# --------------- Importers (with provenance) ---------------
def import_students(df: pd.DataFrame, upload_id: int):
    df = normalize_columns(df).fillna("")
    required = ["roll_no","name","email","phone","father_name","father_phone","semester"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Students: missing column '{col}'")
    n = 0
    with get_db() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            roll = str(r["roll_no"]).strip()
            if not roll: 
                continue
            name = str(r["name"]).strip()
            email = str(r["email"]).strip() or None
            phone = str(r["phone"]).strip() or None
            fname = str(r["father_name"]).strip() or None
            fphone = str(r["father_phone"]).strip() or None
            # semester
            semester = None
            sr = str(r["semester"]).strip()
            if sr and sr.lower() != "nan":
                try: semester = int(float(sr))
                except: semester = None
            existed = cur.execute("SELECT 1 FROM students WHERE roll_no=?", (roll,)).fetchone()
            cur.execute(
                """INSERT INTO students(roll_no,name,email,phone,father_name,father_phone,semester)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(roll_no) DO UPDATE SET
                     name=excluded.name,
                     email=excluded.email,
                     phone=excluded.phone,
                     father_name=excluded.father_name,
                     father_phone=excluded.father_phone,
                     semester=excluded.semester""",
                (roll, name, email, phone, fname, fphone, semester),
            )
            cur.execute(
                "INSERT INTO upload_students_map(upload_id, roll_no, created_new) VALUES(?,?,?)",
                (upload_id, roll, 0 if existed else 1),
            )
            n += 1
        con.commit()
    bump_rowcount(upload_id, n)

def import_attendance(df: pd.DataFrame, upload_id: int):
    df = normalize_columns(df).fillna(0)
    for col in ["roll_no", "subject", "attended", "total"]:
        if col not in df.columns:
            raise ValueError(f"Attendance: missing column '{col}'")
    n = 0
    with get_db() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            roll = str(r["roll_no"]).strip()
            subj = str(r["subject"]).strip()
            if not roll or not subj:
                continue
            attended = int(float(r["attended"])) if str(r["attended"]).strip() else 0
            total = int(float(r["total"])) if str(r["total"]).strip() else 0
            semester = None
            if "semester" in df.columns:
                sr = str(r["semester"]).strip()
                if sr and sr.lower() != "nan":
                    try: semester = int(float(sr))
                    except: semester = None
            cur.execute(
                """INSERT INTO attendance(roll_no,subject,attended,total,semester,source_upload_id)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(roll_no,subject) DO UPDATE SET
                     attended=excluded.attended,
                     total=excluded.total,
                     semester=COALESCE(excluded.semester, attendance.semester),
                     source_upload_id=excluded.source_upload_id""",
                (roll, subj, attended, total, semester, upload_id),
            )
            n += 1
        con.commit()
    bump_rowcount(upload_id, n)

def import_marks(df: pd.DataFrame, upload_id: int):
    df = normalize_columns(df).fillna(0)
    for col in ["roll_no", "exam", "subject", "max_marks", "marks_obtained"]:
        if col not in df.columns:
            raise ValueError(f"Marks: missing column '{col}'")
    n = 0
    with get_db() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            roll = str(r["roll_no"]).strip()
            exam = str(r["exam"]).strip()
            subj = str(r["subject"]).strip()
            if not roll or not exam or not subj:
                continue
            maxm = int(float(r["max_marks"])) if str(r["max_marks"]).strip() else 0
            got = int(float(r["marks_obtained"])) if str(r["marks_obtained"]).strip() else 0
            credits = None
            if "credits" in df.columns:
                cr = str(r["credits"]).strip()
                if cr and cr.lower() != "nan":
                    try: credits = int(float(cr))
                    except: credits = None
            semester = None
            if "semester" in df.columns:
                sr = str(r["semester"]).strip()
                if sr and sr.lower() != "nan":
                    try: semester = int(float(sr))
                    except: semester = None
            cur.execute(
                """INSERT INTO marks(roll_no,exam,subject,max_marks,marks_obtained,credits,semester,source_upload_id)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(roll_no,exam,subject) DO UPDATE SET
                     max_marks=excluded.max_marks,
                     marks_obtained=excluded.marks_obtained,
                     credits=COALESCE(excluded.credits, marks.credits),
                     semester=COALESCE(excluded.semester, marks.semester),
                     source_upload_id=excluded.source_upload_id""",
                (roll, exam, subj, maxm, got, credits, semester, upload_id),
            )
            n += 1
        con.commit()
    bump_rowcount(upload_id, n)

def delete_upload(upload_id: int) -> bool:
    with get_db() as con:
        cur = con.cursor()
        up = cur.execute("SELECT * FROM uploads WHERE id=?", (upload_id,)).fetchone()
        if not up:
            return False
        cur.execute("DELETE FROM attendance WHERE source_upload_id=?", (upload_id,))
        cur.execute("DELETE FROM marks WHERE source_upload_id=?", (upload_id,))
        created = cur.execute(
            "SELECT roll_no FROM upload_students_map WHERE upload_id=? AND created_new=1", (upload_id,)
        ).fetchall()
        if created:
            rolls = [r["roll_no"] for r in created]
            q = f"DELETE FROM students WHERE roll_no IN ({','.join('?'*len(rolls))})"
            cur.execute(q, rolls)
        cur.execute("DELETE FROM upload_students_map WHERE upload_id=?", (upload_id,))
        cur.execute("DELETE FROM uploads WHERE id=?", (upload_id,))
        con.commit()
    return True

# --------------- Wide views ---------------
def letter_grade(pct: float) -> str:
    if pct >= 85: return "A+"
    if pct >= 75: return "A"
    if pct >= 65: return "B+"
    if pct >= 55: return "B"
    if pct >= 45: return "C"
    if pct >= 35: return "D"
    return "F"

def build_marks_grid(roll: str):
    with get_db() as con:
        rows = con.execute(
            "SELECT exam, subject, max_marks, marks_obtained FROM marks WHERE roll_no=? ORDER BY exam, subject", (roll,)
        ).fetchall()
    if not rows: 
        return [], []
    subjects = sorted({r["subject"] for r in rows})
    exams = []
    for r in rows:
        if r["exam"] not in exams:
            exams.append(r["exam"])
    grid_rows = []
    for ex in exams:
        row = {"Exam": ex}
        for sub in subjects:
            match = [r for r in rows if r["exam"]==ex and r["subject"]==sub]
            if match:
                r = match[0]
                pct = (100.0 * r["marks_obtained"] / r["max_marks"]) if r["max_marks"] else 0.0
                row[sub] = f"{letter_grade(pct)} ({r['marks_obtained']}/{r['max_marks']})"
            else:
                row[sub] = "—"
        grid_rows.append(row)
    headers = ["Exam"] + subjects
    return headers, grid_rows

def build_attendance_grid(roll: str):
    with get_db() as con:
        rows = con.execute(
            "SELECT subject, attended, total FROM attendance WHERE roll_no=? ORDER BY subject", (roll,)
        ).fetchall()
    if not rows: 
        return [], []
    subjects = [r["subject"] for r in rows]
    row = {"Row": "Attendance"}
    for r in rows:
        pct = (100.0 * r["attended"] / r["total"]) if r["total"] else 0.0
        row[r["subject"]] = f"{r['attended']}/{r['total']} ({pct:.0f}%)"
    headers = ["Row"] + subjects
    return headers, [row]

# --------------- Routes ---------------
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/templates/<kind>')
def download_template(kind):
    filename = kind
    path = os.path.join(TEMPLATE_DIR, filename)
    if not os.path.exists(path):
        flash('Template not found','error')
        return redirect(url_for('home'))
    return send_from_directory(TEMPLATE_DIR, filename, as_attachment=True)

@app.route('/search')
def search():
    roll=(request.args.get('roll') or '').strip()
    if not roll:
        flash('Enter a roll number','error'); return redirect(url_for('home'))
    return redirect(url_for('student_view', roll_no=roll))

@app.route('/student/<roll_no>')
def student_view(roll_no):
    with get_db() as con:
        cur=con.cursor()
        s=cur.execute('SELECT * FROM students WHERE roll_no=?',(roll_no,)).fetchone()
        if not s:
            flash('Student not found.','error'); return redirect(url_for('home'))
        remarks=cur.execute('SELECT * FROM remarks WHERE roll_no=? ORDER BY created_at DESC',(roll_no,)).fetchall()
    marks_headers, marks_rows = build_marks_grid(roll_no)
    att_headers, att_rows = build_attendance_grid(roll_no)
    # simple % and cgpa
    # (reuse compute inline for brevity)
    total_att = 0; total_tot = 0
    for r in att_rows[0].keys() if att_rows else []:
        pass
    return render_template('student.html', student=s, remarks=remarks,
                           marks_headers=marks_headers, marks_rows=marks_rows,
                           att_headers=att_headers, att_rows=att_rows)

@app.route('/student/<roll_no>/remark', methods=['POST'])
def add_remark(roll_no):
    text=(request.form.get('remark') or '').strip()
    if not text:
        flash('Remark cannot be empty.','error'); return redirect(url_for('student_view', roll_no=roll_no))
    with get_db() as con:
        con.execute('INSERT INTO remarks(roll_no,remark_text,author_username,created_at) VALUES(?,?,?,?)',
                    (roll_no, text, session.get('username','admin'), datetime.now().isoformat(timespec='seconds')))
        con.commit()
    flash('Remark added.','success'); return redirect(url_for('student_view', roll_no=roll_no))

# --- Admin auth ---
@app.route('/admin/login', methods=['GET','POST'])
def admin_login():
    if request.method=='POST':
        u=(request.form.get('username') or '').strip()
        p=(request.form.get('password') or '')
        with get_db() as con:
            row=con.execute('SELECT * FROM users WHERE username=?',(u,)).fetchone()
        if row and check_password_hash(row['password'], p) and row['role']=='admin':
            session['username']=u; session['role']='admin'
            flash('Welcome, Admin!','success'); return redirect(url_for('admin_imports'))
        flash('Invalid credentials','error')
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    session.clear(); flash('Logged out.','success'); return redirect(url_for('admin_login'))

# --- Admin: Single uploads + recent uploads list ---
@app.route('/admin/imports')
def admin_imports():
    if session.get('role')!='admin': return redirect(url_for('admin_login'))
    with get_db() as con:
        stats = {
            'students': con.execute('SELECT COUNT(*) FROM students').fetchone()[0],
            'attendance': con.execute('SELECT COUNT(*) FROM attendance').fetchone()[0],
            'marks': con.execute('SELECT COUNT(*) FROM marks').fetchone()[0],
        }
        recent = con.execute('SELECT * FROM uploads ORDER BY id DESC LIMIT 50').fetchall()
    student_cols = ["roll_no","name","email","phone","father_name","father_phone","semester"]
    attendance_cols = ["roll_no","subject","attended","total","semester"]
    marks_cols = ["roll_no","exam","subject","max_marks","marks_obtained","credits","semester"]
    return render_template('admin_imports.html', stats=stats, uploads=recent,
                           student_cols=student_cols, attendance_cols=attendance_cols, marks_cols=marks_cols)

@app.route('/admin/upload/students', methods=['POST'])
def upload_students():
    if session.get('role')!='admin': return redirect(url_for('admin_login'))
    f = request.files.get('students_file')
    if not f or not f.filename:
        flash('Choose a Students Excel/CSV file.','error'); return redirect(url_for('admin_imports'))
    try:
        path, name = save_upload(f)
        up_id = create_upload_record(name, 'Students')
        df = load_table(path)
        import_students(df, up_id)
        flash('Students imported/updated successfully.','success')
    except Exception as e:
        traceback.print_exc(); flash(f'Import failed (Students): {e}','error')
    return redirect(url_for('admin_imports'))

@app.route('/admin/upload/attendance', methods=['POST'])
def upload_attendance():
    if session.get('role')!='admin': return redirect(url_for('admin_login'))
    f = request.files.get('attendance_file')
    if not f or not f.filename:
        flash('Choose an Attendance Excel/CSV file.','error'); return redirect(url_for('admin_imports'))
    try:
        path, name = save_upload(f)
        up_id = create_upload_record(name, 'Attendance')
        df = load_table(path)
        import_attendance(df, up_id)
        flash('Attendance imported/updated successfully.','success')
    except Exception as e:
        traceback.print_exc(); flash(f'Import failed (Attendance): {e}','error')
    return redirect(url_for('admin_imports'))

@app.route('/admin/upload/marks', methods=['POST'])
def upload_marks():
    if session.get('role')!='admin': return redirect(url_for('admin_login'))
    f = request.files.get('marks_file')
    if not f or not f.filename:
        flash('Choose a Marks Excel/CSV file.','error'); return redirect(url_for('admin_imports'))
    try:
        path, name = save_upload(f)
        up_id = create_upload_record(name, 'Marks')
        df = load_table(path)
        import_marks(df, up_id)
        flash('Marks imported/updated successfully.','success')
    except Exception as e:
        traceback.print_exc(); flash(f'Import failed (Marks): {e}','error')
    return redirect(url_for('admin_imports'))

@app.route('/admin/uploads/<int:upload_id>/delete', methods=['POST'])
def delete_upload_route(upload_id):
    if session.get('role')!='admin': return redirect(url_for('admin_login'))
    ok = delete_upload(upload_id)
    flash('Upload deleted and data rolled back.' if ok else 'Upload not found.',
         'success' if ok else 'error')
    return redirect(url_for('admin_imports'))

# PDF
@app.route('/student/<roll>/pdf')
def student_pdf(roll):
    # simple PDF with identity and tables can be added later
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    c.drawString(100, 800, f"Student Report: {roll}")
    c.showPage(); c.save(); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f'{roll}_report.pdf')

# --------------- Entry ---------------
if __name__ == '__main__':
    run_schema()
    ensure_admin()
    app.run(debug=True, use_reloader=False, threaded=False)
