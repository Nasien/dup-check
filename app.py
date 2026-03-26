from __future__ import annotations

import html
import os
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz, process
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import NullPool

# ==========================
# PATHS / ENV
# ==========================
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
IS_VERCEL = os.getenv("VERCEL") == "1"
SIMILARITY_THRESHOLD = int(os.getenv("SIMILARITY_THRESHOLD", "90"))
MAX_FUZZY_CANDIDATES = int(os.getenv("MAX_FUZZY_CANDIDATES", "300"))
APP_TITLE = "Duplicate Grantee Checker"


def normalize_database_url(raw_url: str | None) -> str:
    if not raw_url:
        raise RuntimeError("DATABASE_URL is not set. Add your Neon Postgres connection string.")

    db_url = raw_url.strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    parsed = urlparse(db_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("sslmode", os.getenv("DB_SSLMODE", "require"))

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query),
            parsed.fragment,
        )
    )


DATABASE_URL = normalize_database_url(os.getenv("DATABASE_URL"))

engine_kwargs = {"pool_pre_ping": True, "future": True}
if IS_VERCEL:
    engine_kwargs["poolclass"] = NullPool

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


# ==========================
# DATABASE MODELS
# ==========================
class Upload(Base):
    __tablename__ = "uploads"

    id = Column(String(36), primary_key=True, index=True)
    filename = Column(String, nullable=False)
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Grantee(Base):
    __tablename__ = "grantees"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String, index=True, nullable=False)
    academic_year = Column(String, index=True, nullable=False)
    semester = Column(String, index=True, nullable=False)
    scholarship = Column(String, nullable=False)
    hei = Column(String, index=True)
    upload_id = Column(String(36), index=True, nullable=False)
    duplicate = Column(String, default="NO", index=True)
    duplicate_with_scholarship = Column(String)
    duplicate_with_name = Column(String)
    duplicate_with_hei = Column(String)
    match_type = Column(String)
    match_score = Column(Float)


# ==========================
# APP
# ==========================
app = FastAPI(title=APP_TITLE)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS duplicate VARCHAR DEFAULT 'NO'"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS duplicate_with_scholarship VARCHAR"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS duplicate_with_name VARCHAR"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS duplicate_with_hei VARCHAR"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS match_type VARCHAR"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS match_score FLOAT"))
        conn.execute(text("ALTER TABLE grantees ADD COLUMN IF NOT EXISTS hei VARCHAR"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_ay_sem ON grantees (academic_year, semester)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_upload ON grantees (upload_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_name ON grantees (full_name)"))


# ==========================
# DEPENDENCY
# ==========================
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ==========================
# UTILITIES
# ==========================
def safe(value: object) -> str:
    return html.escape("" if value is None else str(value))


REQUIRED_COLUMNS = [
    "LastName",
    "FirstName",
    "MiddleName",
    "Scholarship",
    "AcademicYear",
    "Semester",
    "HEI",
]


def normalize_token(value: object) -> str:
    text_value = "" if pd.isna(value) else str(value)
    return " ".join(text_value.strip().upper().split())



def normalize_name(row: pd.Series) -> str:
    parts: list[str] = []
    for col in ["LastName", "FirstName", "MiddleName"]:
        val = normalize_token(row.get(col, ""))
        if val:
            parts.append(val)
    return " ".join(parts)



def build_file_label(df: pd.DataFrame) -> str:
    ay = normalize_token(df.loc[0, "AcademicYear"]).replace(" ", "_")
    sem = normalize_token(df.loc[0, "Semester"]).replace(" ", "_")
    hei_name = normalize_token(df.loc[0, "HEI"]).replace(" ", "_")
    return f"{hei_name}_{ay}_{sem}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"



def get_name_block(name: str) -> str:
    if not name:
        return ""
    surname = name.split()[0]
    return surname[:3]



def page_shell(title: str, body: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{safe(title)}</title>
        <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
        <div class="page-shell">
            <aside class="sidebar">
                <div>
                    <div class="brand">DG</div>
                    <h1>{safe(APP_TITLE)}</h1>
                    <p>Fast duplicate checking for TES and TDP uploads.</p>
                </div>
                <nav class="nav-links">
                    <a href="/">Dashboard</a>
                    <a href="/history">Upload History</a>
                    <a href="/healthz">Health Check</a>
                </nav>
                <div class="sidebar-foot">
                    Optimized for Neon, Render, and Vercel
                </div>
            </aside>
            <main class="main-content">{body}</main>
        </div>
    </body>
    </html>
    """



def stat_card(label: str, value: object, tone: str = "") -> str:
    tone_class = f" tone-{tone}" if tone else ""
    return f"<div class='stat-card{tone_class}'><span>{safe(label)}</span><strong>{safe(value)}</strong></div>"



def detect_duplicates(df: pd.DataFrame, existing_rows: list[Grantee]) -> pd.DataFrame:
    df = df.copy()
    df["Duplicate"] = "NO"
    df["DuplicateWithScholarship"] = ""
    df["DuplicateWithName"] = ""
    df["DuplicateWithHEI"] = ""
    df["MatchType"] = ""
    df["MatchScore"] = 0.0

    exact_existing: dict[str, list[dict[str, str]]] = defaultdict(list)
    blocked_existing: dict[str, list[dict[str, str]]] = defaultdict(list)

    for item in existing_rows:
        key = normalize_token(item.full_name)
        if not key:
            continue
        payload = {
            "name": key,
            "scholarship": normalize_token(item.scholarship),
            "hei": normalize_token(item.hei),
        }
        exact_existing[key].append(payload)
        blocked_existing[get_name_block(key)].append(payload)

    local_seen_exact: dict[str, list[dict[str, str]]] = defaultdict(list)
    local_blocked: dict[str, list[dict[str, str]]] = defaultdict(list)

    for i, row in df.iterrows():
        current_name = normalize_token(row["FullName"])
        current_sch = normalize_token(row["Scholarship"])
        current_hei = normalize_token(row["HEI"])
        block = get_name_block(current_name)

        best_match: dict[str, object] | None = None

        def consider(candidate: dict[str, str], match_type: str, score: float) -> None:
            nonlocal best_match
            cross_program = candidate["scholarship"] != current_sch
            priority = 3 if cross_program else 2
            if match_type == "possible_duplicate":
                priority = 1 if cross_program else 0

            new_match = {
                "name": candidate["name"],
                "scholarship": candidate["scholarship"],
                "hei": candidate["hei"],
                "match_type": match_type,
                "score": round(float(score), 2),
                "priority": priority,
            }
            if best_match is None or (new_match["priority"], new_match["score"]) > (
                best_match["priority"],
                best_match["score"],
            ):
                best_match = new_match

        for candidate in exact_existing.get(current_name, []):
            match_type = "cross_program_exact" if candidate["scholarship"] != current_sch else "exact_duplicate"
            consider(candidate, match_type, 100)

        for candidate in local_seen_exact.get(current_name, []):
            match_type = "cross_program_exact" if candidate["scholarship"] != current_sch else "exact_duplicate"
            consider(candidate, match_type, 100)

        if best_match is None and block:
            candidates = blocked_existing.get(block, []) + local_blocked.get(block, [])
            unique_candidates = []
            seen_keys = set()
            for candidate in candidates:
                name_key = candidate["name"]
                if name_key != current_name and name_key not in seen_keys:
                    seen_keys.add(name_key)
                    unique_candidates.append(candidate)
                if len(unique_candidates) >= MAX_FUZZY_CANDIDATES:
                    break

            if unique_candidates:
                match = process.extractOne(
                    current_name,
                    [candidate["name"] for candidate in unique_candidates],
                    scorer=fuzz.token_sort_ratio,
                )
                if match and match[1] >= SIMILARITY_THRESHOLD:
                    candidate_name = match[0]
                    candidate = next(c for c in unique_candidates if c["name"] == candidate_name)
                    consider(candidate, "possible_duplicate", match[1])

        if best_match is not None:
            df.at[i, "Duplicate"] = "YES"
            df.at[i, "DuplicateWithScholarship"] = best_match["scholarship"]
            df.at[i, "DuplicateWithName"] = best_match["name"]
            df.at[i, "DuplicateWithHEI"] = best_match["hei"]
            df.at[i, "MatchType"] = best_match["match_type"]
            df.at[i, "MatchScore"] = best_match["score"]

        new_payload = {"name": current_name, "scholarship": current_sch, "hei": current_hei}
        if current_name:
            local_seen_exact[current_name].append(new_payload)
            local_blocked[block].append(new_payload)

    return df



def summarize_match_types(rows: list[Grantee]) -> dict[str, int]:
    counts = defaultdict(int)
    for row in rows:
        key = row.match_type or "clean"
        counts[key] += 1
    return counts


# ==========================
# ROUTES
# ==========================
@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(db: Session = Depends(get_db)) -> str:
    total_uploads = db.query(Upload).count()
    total_grantees = db.query(Grantee).count()
    total_duplicates = db.query(Grantee).filter(Grantee.duplicate == "YES").count()
    total_cross = db.query(Grantee).filter(Grantee.match_type == "cross_program_exact").count()

    recent_uploads = db.query(Upload).order_by(Upload.uploaded_at.desc()).limit(5).all()
    recent_rows = "".join(
        f"""
        <tr>
            <td>{safe(item.filename)}</td>
            <td>{item.uploaded_at.strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td><a class='table-link' href='/results/{item.id}'>View results</a></td>
        </tr>
        """
        for item in recent_uploads
    ) or "<tr><td colspan='3' class='empty-state'>No uploads yet.</td></tr>"

    body = f"""
    <section class="hero-card">
        <div>
            <span class="eyebrow">TES and TDP duplicate screening</span>
            <h2>Upload a masterlist and detect exact, cross-program, and possible duplicates.</h2>
            <p>
                This version is optimized for faster matching, better result browsing, and cleaner deployment on Neon,
                Render, and Vercel.
            </p>
        </div>
        <div class="hero-actions">
            <a class="btn btn-secondary" href="/history">View history</a>
        </div>
    </section>

    <section class="stats-grid">
        {stat_card('Total uploads', total_uploads)}
        {stat_card('Total grantees', total_grantees)}
        {stat_card('Flagged duplicates', total_duplicates, 'danger')}
        {stat_card('Cross-program exact matches', total_cross, 'warning')}
    </section>

    <section class="grid-two">
        <div class="panel">
            <div class="panel-head">
                <div>
                    <span class="eyebrow">Upload checker</span>
                    <h3>Upload Excel file</h3>
                </div>
            </div>
            <form class="upload-form" action="/upload" method="post" enctype="multipart/form-data">
                <label class="field-label">Required Excel columns</label>
                <div class="hint-box">LastName, FirstName, MiddleName, Scholarship, AcademicYear, Semester, HEI</div>
                <input class="file-input" type="file" name="file" accept=".xlsx,.xls" required>
                <button class="btn" type="submit">Upload and check duplicates</button>
            </form>
        </div>

        <div class="panel">
            <div class="panel-head">
                <div>
                    <span class="eyebrow">Latest uploads</span>
                    <h3>Recent activity</h3>
                </div>
            </div>
            <div class="table-wrap compact-table">
                <table>
                    <thead>
                        <tr>
                            <th>File</th>
                            <th>Uploaded</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>{recent_rows}</tbody>
                </table>
            </div>
        </div>
    </section>
    """
    return page_shell(APP_TITLE, body)


@app.get("/history", response_class=HTMLResponse)
def history(db: Session = Depends(get_db)) -> str:
    uploads = db.query(Upload).order_by(Upload.uploaded_at.desc()).all()

    rows = []
    for idx, upload in enumerate(uploads, start=1):
        duplicate_count = db.query(Grantee).filter(Grantee.upload_id == upload.id, Grantee.duplicate == "YES").count()
        total_count = db.query(Grantee).filter(Grantee.upload_id == upload.id).count()
        rows.append(
            f"""
            <tr>
                <td>{idx}</td>
                <td>{safe(upload.filename)}</td>
                <td>{upload.uploaded_at.strftime('%Y-%m-%d %H:%M:%S')}</td>
                <td>{total_count}</td>
                <td>{duplicate_count}</td>
                <td>
                    <div class='action-links'>
                        <a class='table-link' href='/results/{upload.id}'>View</a>
                        <a class='table-link danger-link' href='/delete/{upload.id}' onclick="return confirm('Delete this upload and its results?')">Delete</a>
                    </div>
                </td>
            </tr>
            """
        )

    table_rows = "".join(rows) or "<tr><td colspan='6' class='empty-state'>No upload history available.</td></tr>"

    body = f"""
    <section class='panel'>
        <div class='panel-head'>
            <div>
                <span class='eyebrow'>Upload history</span>
                <h2>Browse past duplicate checks</h2>
            </div>
            <a class='btn btn-secondary' href='/'>New upload</a>
        </div>

        <div class='toolbar'>
            <input id='historySearch' class='search-input' type='text' placeholder='Search file name...' oninput='filterHistory()'>
        </div>

        <div class='table-wrap'>
            <table id='historyTable'>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>File</th>
                        <th>Uploaded</th>
                        <th>Grantees</th>
                        <th>Duplicates</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>{table_rows}</tbody>
            </table>
        </div>
    </section>

    <script>
    function filterHistory() {{
        const needle = document.getElementById('historySearch').value.toLowerCase();
        const rows = document.querySelectorAll('#historyTable tbody tr');
        rows.forEach((row) => {{
            row.style.display = row.textContent.toLowerCase().includes(needle) ? '' : 'none';
        }});
    }}
    </script>
    """
    return page_shell("Upload History", body)


@app.get("/results/{upload_id}", response_class=HTMLResponse)
def view_results(upload_id: str, db: Session = Depends(get_db)) -> str:
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    grantees = db.query(Grantee).filter(Grantee.upload_id == upload_id).order_by(Grantee.id.asc()).all()

    if not grantees:
        return page_shell("No Results", "<section class='panel'><h2>No results found</h2></section>")

    total_count = len(grantees)
    duplicate_count = sum(1 for g in grantees if g.duplicate == "YES")
    cross_exact_count = sum(1 for g in grantees if g.match_type == "cross_program_exact")
    possible_count = sum(1 for g in grantees if g.match_type == "possible_duplicate")
    summary = summarize_match_types(grantees)

    rows = []
    for idx, item in enumerate(grantees, start=1):
        badge_class = "badge-clean"
        badge_label = "Clean"
        if item.match_type == "cross_program_exact":
            badge_class, badge_label = "badge-warning", "Cross-program exact"
        elif item.match_type == "exact_duplicate":
            badge_class, badge_label = "badge-danger", "Exact duplicate"
        elif item.match_type == "possible_duplicate":
            badge_class, badge_label = "badge-info", "Possible duplicate"

        rows.append(
            f"""
            <tr data-duplicate='{safe(item.duplicate)}' data-matchtype='{safe(item.match_type or 'clean')}'>
                <td>{idx}</td>
                <td>{safe(item.full_name)}</td>
                <td>{safe(item.hei)}</td>
                <td>{safe(item.academic_year)}</td>
                <td>{safe(item.semester)}</td>
                <td>{safe(item.scholarship)}</td>
                <td><span class='result-badge {badge_class}'>{badge_label}</span></td>
                <td>{safe(item.duplicate_with_name or '')}</td>
                <td>{safe(item.duplicate_with_hei or '')}</td>
                <td>{safe(item.duplicate_with_scholarship or '')}</td>
                <td>{safe(item.match_score or '')}</td>
            </tr>
            """
        )

    body = f"""
    <section class='hero-card results-hero'>
        <div>
            <span class='eyebrow'>Result set</span>
            <h2>{safe(upload.filename if upload else 'Results')}</h2>
            <p>Use the quick filters below to review only flagged rows and focus on cross-matches between TES and TDP.</p>
        </div>
        <div class='hero-actions'>
            <a class='btn btn-secondary' href='/history'>Back to history</a>
        </div>
    </section>

    <section class='stats-grid'>
        {stat_card('Total rows', total_count)}
        {stat_card('Flagged duplicates', duplicate_count, 'danger')}
        {stat_card('Cross-program exact', cross_exact_count, 'warning')}
        {stat_card('Possible duplicates', possible_count, 'info')}
    </section>

    <section class='panel'>
        <div class='panel-head'>
            <div>
                <span class='eyebrow'>Review tools</span>
                <h3>Filter and search results</h3>
            </div>
        </div>

        <div class='toolbar toolbar-wrap'>
            <input id='resultSearch' class='search-input' type='text' placeholder='Search name, HEI, scholarship...' oninput='applyFilters()'>
            <label class='toggle'><input type='checkbox' id='duplicatesOnly' onchange='applyFilters()'> Duplicates only</label>
            <select id='matchTypeFilter' class='select-input' onchange='applyFilters()'>
                <option value='all'>All match types</option>
                <option value='cross_program_exact'>Cross-program exact</option>
                <option value='exact_duplicate'>Exact duplicate</option>
                <option value='possible_duplicate'>Possible duplicate</option>
                <option value='clean'>Clean only</option>
            </select>
        </div>

        <div class='match-summary'>
            <span class='pill'>Clean: {summary.get('clean', 0)}</span>
            <span class='pill warning'>Cross-program exact: {summary.get('cross_program_exact', 0)}</span>
            <span class='pill danger'>Exact duplicate: {summary.get('exact_duplicate', 0)}</span>
            <span class='pill info'>Possible duplicate: {summary.get('possible_duplicate', 0)}</span>
        </div>

        <div class='table-wrap'>
            <table id='resultsTable'>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Full name</th>
                        <th>HEI</th>
                        <th>AY</th>
                        <th>Semester</th>
                        <th>Scholarship</th>
                        <th>Status</th>
                        <th>Matched name</th>
                        <th>Matched HEI</th>
                        <th>Matched scholarship</th>
                        <th>Score</th>
                    </tr>
                </thead>
                <tbody>{''.join(rows)}</tbody>
            </table>
        </div>
    </section>

    <script>
    function applyFilters() {{
        const needle = document.getElementById('resultSearch').value.toLowerCase();
        const duplicatesOnly = document.getElementById('duplicatesOnly').checked;
        const matchType = document.getElementById('matchTypeFilter').value;
        const rows = document.querySelectorAll('#resultsTable tbody tr');

        rows.forEach((row) => {{
            const rowText = row.textContent.toLowerCase();
            const isDuplicate = row.dataset.duplicate === 'YES';
            const rowType = row.dataset.matchtype || 'clean';
            const matchTypeOk = matchType === 'all' ? true : rowType === matchType;
            const duplicateOk = duplicatesOnly ? isDuplicate : true;
            const textOk = rowText.includes(needle);
            row.style.display = matchTypeOk && duplicateOk && textOk ? '' : 'none';
        }});
    }}
    </script>
    """
    return page_shell("Results", body)


@app.post("/upload", response_class=HTMLResponse)
def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file selected.")
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed.")

    upload_id = str(uuid.uuid4())
    suffix = Path(file.filename).suffix or ".xlsx"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file.file.read())
        temp_path = tmp.name

    try:
        df = pd.read_excel(temp_path)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not read the uploaded Excel file: {exc}") from exc
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Excel format. Missing columns: {', '.join(missing_columns)}",
        )

    df = df[REQUIRED_COLUMNS].fillna("")
    if df.empty:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")

    df["AcademicYear"] = df["AcademicYear"].map(normalize_token)
    df["Semester"] = df["Semester"].map(normalize_token)
    df["Scholarship"] = df["Scholarship"].map(normalize_token)
    df["HEI"] = df["HEI"].map(normalize_token)
    df["LastName"] = df["LastName"].map(normalize_token)
    df["FirstName"] = df["FirstName"].map(normalize_token)
    df["MiddleName"] = df["MiddleName"].map(normalize_token)
    df["FullName"] = df.apply(normalize_name, axis=1)

    academic_year = str(df.loc[0, "AcademicYear"])
    semester = str(df.loc[0, "Semester"])

    existing_rows = (
        db.query(Grantee)
        .filter(Grantee.academic_year == academic_year, Grantee.semester == semester)
        .all()
    )

    df = detect_duplicates(df, existing_rows)
    file_label = build_file_label(df)

    db.add(Upload(id=upload_id, filename=f"{file_label}.xlsx"))
    db.bulk_save_objects(
        [
            Grantee(
                full_name=row["FullName"],
                academic_year=row["AcademicYear"],
                semester=row["Semester"],
                scholarship=row["Scholarship"],
                hei=row["HEI"],
                upload_id=upload_id,
                duplicate=row["Duplicate"],
                duplicate_with_scholarship=row["DuplicateWithScholarship"],
                duplicate_with_name=row["DuplicateWithName"],
                duplicate_with_hei=row["DuplicateWithHEI"],
                match_type=row["MatchType"],
                match_score=float(row["MatchScore"] or 0),
            )
            for _, row in df.iterrows()
        ]
    )
    db.commit()

    return RedirectResponse(f"/results/{upload_id}", status_code=303)


@app.get("/delete/{upload_id}")
def delete_upload(upload_id: str, db: Session = Depends(get_db)):
    db.query(Grantee).filter(Grantee.upload_id == upload_id).delete()
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if upload:
        db.delete(upload)
    db.commit()
    return RedirectResponse("/history", status_code=303)
