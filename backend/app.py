from __future__ import annotations

import os
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Generator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import NullPool
from rapidfuzz import fuzz, process

SIMILARITY_THRESHOLD = int(os.getenv("SIMILARITY_THRESHOLD", "90"))
MAX_FUZZY_CANDIDATES = int(os.getenv("MAX_FUZZY_CANDIDATES", "300"))
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")
APP_TITLE = "Duplicate Grantee Checker API"

REQUIRED_COLUMNS = [
    "LastName",
    "FirstName",
    "MiddleName",
    "Scholarship",
    "AcademicYear",
    "Semester",
    "HEI",
]


def normalize_database_url(raw_url: str | None) -> str:
    if not raw_url:
        raise RuntimeError("DATABASE_URL is not set. Add your Neon Postgres connection string.")

    db_url = raw_url.strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    parsed = urlparse(db_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("sslmode", DB_SSLMODE)

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
IS_VERCEL = os.getenv("VERCEL") == "1"

engine_kwargs: dict[str, Any] = {"pool_pre_ping": True, "future": True}
if IS_VERCEL:
    engine_kwargs["poolclass"] = NullPool

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


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


app = FastAPI(title=APP_TITLE)

allowed_origins = [origin.strip() for origin in FRONTEND_ORIGIN.split(",") if origin.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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


def serialize_upload(upload: Upload, db: Session) -> dict[str, Any]:
    grantee_rows = db.query(Grantee).filter(Grantee.upload_id == upload.id).all()
    total_count = len(grantee_rows)
    duplicate_count = sum(1 for row in grantee_rows if row.duplicate == "YES")
    cross_program_exact = sum(1 for row in grantee_rows if row.match_type == "cross_program_exact")
    possible_duplicates = sum(1 for row in grantee_rows if row.match_type == "possible_duplicate")
    return {
        "id": upload.id,
        "filename": upload.filename,
        "uploaded_at": upload.uploaded_at.isoformat(),
        "total_count": total_count,
        "duplicate_count": duplicate_count,
        "cross_program_exact": cross_program_exact,
        "possible_duplicates": possible_duplicates,
    }


def serialize_grantee(item: Grantee) -> dict[str, Any]:
    return {
        "id": item.id,
        "full_name": item.full_name,
        "academic_year": item.academic_year,
        "semester": item.semester,
        "scholarship": item.scholarship,
        "hei": item.hei,
        "duplicate": item.duplicate,
        "duplicate_with_name": item.duplicate_with_name,
        "duplicate_with_hei": item.duplicate_with_hei,
        "duplicate_with_scholarship": item.duplicate_with_scholarship,
        "match_type": item.match_type or "clean",
        "match_score": float(item.match_score or 0),
    }


@app.get("/")
def root() -> dict[str, str]:
    return {"message": APP_TITLE, "health": "/api/healthz"}


@app.get("/api/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/stats")
def stats(db: Session = Depends(get_db)) -> dict[str, int]:
    return {
        "total_uploads": db.query(Upload).count(),
        "total_grantees": db.query(Grantee).count(),
        "flagged_duplicates": db.query(Grantee).filter(Grantee.duplicate == "YES").count(),
        "cross_program_exact": db.query(Grantee).filter(Grantee.match_type == "cross_program_exact").count(),
    }


@app.get("/api/uploads")
def list_uploads(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    uploads = db.query(Upload).order_by(Upload.uploaded_at.desc()).all()
    return [serialize_upload(upload, db) for upload in uploads]


@app.get("/api/uploads/{upload_id}")
def get_upload_results(upload_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found.")

    rows = db.query(Grantee).filter(Grantee.upload_id == upload_id).order_by(Grantee.id.asc()).all()
    summary: dict[str, int] = defaultdict(int)
    for row in rows:
        summary[row.match_type or "clean"] += 1

    return {
        "upload": serialize_upload(upload, db),
        "summary": dict(summary),
        "rows": [serialize_grantee(row) for row in rows],
    }


@app.post("/api/uploads")
def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)) -> dict[str, Any]:
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

    upload = Upload(id=upload_id, filename=f"{file_label}.xlsx")
    db.add(upload)
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

    return {"upload_id": upload_id, "filename": upload.filename}


@app.delete("/api/uploads/{upload_id}")
def delete_upload(upload_id: str, db: Session = Depends(get_db)) -> dict[str, str]:
    db.query(Grantee).filter(Grantee.upload_id == upload_id).delete()
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        db.commit()
        raise HTTPException(status_code=404, detail="Upload not found.")
    db.delete(upload)
    db.commit()
    return {"status": "deleted"}
