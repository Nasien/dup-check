from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Generator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker
from sqlalchemy.pool import NullPool
from rapidfuzz import fuzz, process

SIMILARITY_THRESHOLD = int(os.getenv("SIMILARITY_THRESHOLD", "90"))
MAX_FUZZY_CANDIDATES = int(os.getenv("MAX_FUZZY_CANDIDATES", "300"))
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")
APP_TITLE = "Duplicate Grantee Checker API"
SECRET_KEY = os.getenv("SECRET_KEY", "change-this-secret-key")
TOKEN_HOURS = int(os.getenv("TOKEN_HOURS", "12"))
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

NAME_COLUMNS = ["LastName", "FirstName", "MiddleName"]
REQUIRED_COLUMNS = NAME_COLUMNS
OPTION_CATEGORIES = ["hei", "scholarship", "academic_year", "semester", "batch"]
DEFAULT_HEIS = [
    "ABE International Business College - Iloilo City, Inc.",
    "ACLC College of Iloilo City, Inc.",
    "ACSI College Iloilo, Inc.",
    "Advance Central College, Inc.",
    "Aklan Catholic College, Inc.",
    "Aklan Polytechnic College, Inc.",
    "Aklan State University - Banga Campus",
    "Aklan State University - Ibajay Campus",
    "Aklan State University - Kalibo Campus",
    "Aklan State University - Makato Campus",
    "Aklan State University - New Washington Campus",
    "Altavas College",
    "AMA Computer College - Iloilo City, Inc.",
    "Balete Community College",
    "Batan Integrated College of Technology",
    "Cabalum Western College, Inc.",
    "Capiz State University - Burias Campus",
    "Capiz State University - Dayao Campus",
    "Capiz State University - Dumarao Campus",
    "Capiz State University - Main Campus",
    "Capiz State University - Mambusao Campus",
    "Capiz State University - Pilar Campus",
    "Capiz State University - Pontevedra Campus",
    "Capiz State University - Sigma Campus",
    "Capiz State University - Tapaz Campus",
    "Central Philippine University, Inc.",
    "Colegio de la Purisima Concepcion, Inc.",
    "Colegio de San Jose, Jaro, Iloilo City, Inc.",
    "Colegio del Sagrado Corazon de Jesus, Inc.",
    "College of St. John - Roxas, Inc.",
    "Filamer Christian University, Inc.",
    "Garcia College of Technology, Inc.",
    "Green International Technological College",
    "GSEF College, Inc.",
    "Guimaras State University - Baterna Campus",
    "Guimaras State University - Main Campus",
    "Guimaras State University - Mosqueda Campus",
    "Hercor College, Inc.",
    "Hua Siong College of Iloilo, Inc.",
    "Iloilo City Community College",
    "Iloilo Doctors' College",
    "Iloilo Doctors' College of Medicine, Inc.",
    "Iloilo Merchant Marine School",
    "Iloilo Science and Technology University - Barotac Nuevo Campus",
    "Iloilo Science and Technology University - Dumangas Campus",
    "Iloilo Science and Technology University - Leon Campus",
    "Iloilo Science and Technology University -Main Campus",
    "Iloilo Science and Technology University -Miagao Campus",
    "Iloilo State University of Fisheries Science and Technology - Barotac Nuevo Campus (Poblacion)",
    "Iloilo State University of Fisheries Science and Technology - Dingle Campus",
    "Iloilo State University of Fisheries Science and Technology - Dumangas Campus",
    "Iloilo State University of Fisheries Science and Technology - Main Campus (Tiwi Campus)",
    "Iloilo State University of Fisheries Science and Technology - San Enrique Campus",
    "Integrated Midwives Association of the Philippines (IMAP) Foundation School of Midwifery, Inc.",
    "Interface Computer College, Inc.",
    "John B. Lacson Foundation Maritime University (Arevalo), Inc.",
    "John B. Lacson Foundation Maritime University (Molo), Inc.",
    "Malay College",
    "Northern Iloilo State University - Ajuy Campus",
    "Northern Iloilo State University - Barotac Viejo Campus",
    "Northern Iloilo State University - Batad Campus",
    "Northern Iloilo State University - Concepcion Campus",
    "Northern Iloilo State University - Estancia Campus",
    "Northern Iloilo State University - Lemery Campus",
    "Northern Iloilo State University - Sara Campus",
    "Northwestern Visayan Colleges",
    "Pandan Bay Institute, Inc.",
    "Passi City College",
    "Philippine College of Business and Accountancy",
    "Saint Gabriel College, Inc.",
    "Sancta Maria Mater et Regina Seminarium, Inc.",
    "Santa Isabel College of Iloilo City",
    "St. Anthony College of Roxas City, Inc.",
    "St. Anthony's College, Inc.",
    "St. Joseph Regional Seminary Graduate School of Theology",
    "St. Paul University Iloilo",
    "St. Therese - MTC Colleges - Jalandoni, Inc.",
    "St. Therese - MTC Colleges - La Fiesta Site, Inc.",
    "St. Therese - MTC Colleges Tigbauan, Inc.",
    "St. Vincent College of Business and Accountancy, Inc.",
    "St. Vincent College of Science and Technology, Inc.",
    "St. Vincent Ferrer Seminary",
    "STI College Kalibo",
    "Sto. Niño Seminary, Inc.",
    "University of Antique - Hamtic Campus",
    "University of Antique - Main Campus",
    "University of Antique - Tario Lim Memorial Campus",
    "University of Antique - Caluya Campus",
    "University of Antique - Libertad Campus",
    "University of Iloilo",
    "University of Perpetual Help System Laguna Pueblo de Panay Campus",
    "University of San Agustin",
    "University of the Philippines Visayas",
    "Vicente A. Javier Memorial Community College",
    "West Visayas State University - Calinog Campus",
    "West Visayas State University - College of Agriculture and Forestry Campus",
    "West Visayas State University - Janiuay Campus",
    "West Visayas State University - Lambunao Campus",
    "West Visayas State University - Main Campus",
    "West Visayas State University - Pototan Campus",
    "Western Institute of Technology, Inc.",
    "Pius XII College Iloilo Inc.",
]
DEFAULT_OPTIONS = {
    "hei": DEFAULT_HEIS,
    "scholarship": ["TES", "TDP"],
    "academic_year": [],
    "semester": ["1ST SEM", "2ND SEM"],
    "batch": [],
}


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


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(50), nullable=False, default="admin")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class MasterOption(Base):
    __tablename__ = "master_options"

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String(50), index=True, nullable=False)
    name = Column(String(255), nullable=False)
    normalized_name = Column(String(255), index=True, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Upload(Base):
    __tablename__ = "uploads"

    id = Column(String(36), primary_key=True, index=True)
    filename = Column(String, nullable=False)
    hei = Column(String, index=True, nullable=False)
    scholarship = Column(String, index=True, nullable=False)
    academic_year = Column(String, index=True, nullable=False)
    semester = Column(String, index=True, nullable=False)
    batch = Column(String, index=True, nullable=False)
    status = Column(String, index=True, nullable=False, default="Needs review")
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    rows = relationship("Grantee", back_populates="upload", cascade="all, delete")


class Grantee(Base):
    __tablename__ = "grantees"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String, index=True, nullable=False)
    academic_year = Column(String, index=True, nullable=False)
    semester = Column(String, index=True, nullable=False)
    scholarship = Column(String, index=True, nullable=False)
    batch = Column(String, index=True, nullable=False)
    hei = Column(String, index=True, nullable=False)
    upload_id = Column(String(36), ForeignKey("uploads.id", ondelete="CASCADE"), index=True, nullable=False)
    duplicate = Column(String, default="NO", index=True)
    duplicate_with_name = Column(String)
    duplicate_with_hei = Column(String)
    duplicate_with_scholarship = Column(String)
    duplicate_with_batch = Column(String)
    match_type = Column(String)
    match_score = Column(Float)
    upload = relationship("Upload", back_populates="rows")


app = FastAPI(title=APP_TITLE)

allowed_origins = [origin.strip() for origin in FRONTEND_ORIGIN.split(",") if origin.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_token(value: object) -> str:
    text_value = "" if pd.isna(value) else str(value)
    return " ".join(text_value.strip().upper().split())


def normalize_name(row: pd.Series) -> str:
    parts: list[str] = []
    for col in NAME_COLUMNS:
        val = normalize_token(row.get(col, ""))
        if val:
            parts.append(val)
    return " ".join(parts)


def get_name_block(name: str) -> str:
    if not name:
        return ""
    surname = name.split()[0]
    return surname[:3]


def summarize_match_types(rows: list[Grantee]) -> dict[str, int]:
    counts = defaultdict(int)
    for row in rows:
        key = row.match_type or "clean"
        counts[key] += 1
    return counts


def make_upload_label(hei: str, scholarship: str, academic_year: str, semester: str, batch: str) -> str:
    return f"{hei} / {scholarship} / {academic_year} / {semester} / {batch}"


def determine_upload_status(duplicate_count: int, possible_count: int) -> str:
    if duplicate_count == 0:
        return "No duplicates found"
    if possible_count > 0:
        return "Needs review"
    return "With duplicates"


def hash_password(password: str, salt: str | None = None) -> str:
    salt_bytes = (salt or secrets.token_hex(16)).encode("utf-8")
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, 390000)
    return f"{salt_bytes.decode('utf-8')}${base64.urlsafe_b64encode(digest).decode('utf-8')}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, digest = stored.split("$", 1)
    except ValueError:
        return False
    check = hash_password(password, salt)
    return hmac.compare_digest(check, stored)


def create_token(username: str, role: str) -> str:
    exp = int((datetime.now(timezone.utc) + timedelta(hours=TOKEN_HOURS)).timestamp())
    payload = f"{username}|{role}|{exp}"
    signature = hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{payload}|{signature}".encode("utf-8")).decode("utf-8")


def decode_token(token: str) -> dict[str, str]:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        username, role, exp_text, signature = raw.split("|", 3)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=401, detail="Invalid token.") from exc

    payload = f"{username}|{role}|{exp_text}"
    expected = hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid token signature.")
    if int(exp_text) < int(datetime.now(timezone.utc).timestamp()):
        raise HTTPException(status_code=401, detail="Token expired.")
    return {"username": username, "role": role}


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required.")
    return authorization.split(" ", 1)[1].strip()


def require_admin(token: str = Depends(get_bearer_token), db: Session = Depends(get_db)) -> User:
    payload = decode_token(token)
    user = db.query(User).filter(User.username == payload["username"]).first()
    if not user or user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user


def seed_master_options(db: Session) -> None:
    for category, values in DEFAULT_OPTIONS.items():
        for value in values:
            normalized = normalize_token(value)
            exists = db.query(MasterOption).filter(
                MasterOption.category == category,
                MasterOption.normalized_name == normalized,
            ).first()
            if not exists:
                db.add(MasterOption(category=category, name=value, normalized_name=normalized, is_active=True))
    db.commit()


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_scope ON grantees (hei, scholarship, academic_year, semester)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_upload ON grantees (upload_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_grantees_name ON grantees (full_name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_uploads_scope ON uploads (hei, scholarship, academic_year, semester, batch)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_master_options_unique ON master_options (category, normalized_name)"))

    db = SessionLocal()
    try:
        admin = db.query(User).filter(User.username == ADMIN_USERNAME).first()
        if not admin:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password(ADMIN_PASSWORD), role="admin"))
            db.commit()
        seed_master_options(db)
    finally:
        db.close()


@app.get("/api/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/auth/login")
def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)) -> dict[str, Any]:
    user = db.query(User).filter(User.username == username).first()
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    token = create_token(user.username, user.role)
    return {"token": token, "user": {"username": user.username, "role": user.role}}


@app.get("/api/options")
def get_options(db: Session = Depends(get_db)) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for category in OPTION_CATEGORIES:
        rows = db.query(MasterOption).filter(
            MasterOption.category == category,
            MasterOption.is_active.is_(True),
        ).order_by(MasterOption.name.asc()).all()
        payload[category] = [{"id": row.id, "name": row.name} for row in rows]
    return payload


@app.get("/api/admin/options")
def get_admin_options(_: User = Depends(require_admin), db: Session = Depends(get_db)) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for category in OPTION_CATEGORIES:
        rows = db.query(MasterOption).filter(MasterOption.category == category).order_by(MasterOption.name.asc()).all()
        payload[category] = [
            {"id": row.id, "name": row.name, "is_active": row.is_active, "created_at": row.created_at.isoformat()}
            for row in rows
        ]
    return payload


@app.post("/api/admin/options/{category}")
def add_option(
    category: str,
    name: str = Form(...),
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if category not in OPTION_CATEGORIES:
        raise HTTPException(status_code=400, detail="Invalid option category.")
    normalized = normalize_token(name)
    if not normalized:
        raise HTTPException(status_code=400, detail="Option name is required.")
    exists = db.query(MasterOption).filter(
        MasterOption.category == category,
        MasterOption.normalized_name == normalized,
    ).first()
    if exists:
        exists.is_active = True
        db.commit()
        return {"message": "Option already existed and is now active."}
    db.add(MasterOption(category=category, name=name.strip(), normalized_name=normalized, is_active=True))
    db.commit()
    return {"message": "Option added successfully."}


@app.post("/api/admin/options/{category}/{option_id}/toggle")
def toggle_option(
    category: str,
    option_id: int,
    _: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    row = db.query(MasterOption).filter(MasterOption.category == category, MasterOption.id == option_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Option not found.")
    row.is_active = not row.is_active
    db.commit()
    return {"message": "Option updated.", "is_active": row.is_active}


@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)) -> dict[str, int]:
    total_uploads = db.query(Upload).count()
    total_grantees = db.query(Grantee).count()
    flagged_duplicates = db.query(Grantee).filter(Grantee.duplicate == "YES").count()
    exact_duplicates = db.query(Grantee).filter(Grantee.match_type == "exact_duplicate").count()
    possible_duplicates = db.query(Grantee).filter(Grantee.match_type == "possible_duplicate").count()
    return {
        "total_uploads": total_uploads,
        "total_grantees": total_grantees,
        "flagged_duplicates": flagged_duplicates,
        "exact_duplicates": exact_duplicates,
        "possible_duplicates": possible_duplicates,
    }


@app.get("/api/uploads")
def list_uploads(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    uploads = db.query(Upload).order_by(Upload.uploaded_at.desc()).all()
    items = []
    for upload in uploads:
        rows = db.query(Grantee).filter(Grantee.upload_id == upload.id).order_by(Grantee.id.asc()).all()
        duplicate_count = sum(1 for row in rows if row.duplicate == "YES")
        possible_count = sum(1 for row in rows if row.match_type == "possible_duplicate")
        items.append(
            {
                "id": upload.id,
                "filename": upload.filename,
                "hei": upload.hei,
                "scholarship": upload.scholarship,
                "academic_year": upload.academic_year,
                "semester": upload.semester,
                "batch": upload.batch,
                "status": upload.status,
                "uploaded_at": upload.uploaded_at.isoformat(),
                "total_count": len(rows),
                "duplicate_count": duplicate_count,
                "possible_count": possible_count,
            }
        )
    return items


@app.get("/api/uploads/{upload_id}")
def get_upload_results(upload_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found.")
    rows = db.query(Grantee).filter(Grantee.upload_id == upload_id).order_by(Grantee.id.asc()).all()
    summary = summarize_match_types(rows)
    duplicate_count = sum(1 for row in rows if row.duplicate == "YES")
    possible_duplicates = sum(1 for row in rows if row.match_type == "possible_duplicate")
    return {
        "upload": {
            "id": upload.id,
            "filename": upload.filename,
            "hei": upload.hei,
            "scholarship": upload.scholarship,
            "academic_year": upload.academic_year,
            "semester": upload.semester,
            "batch": upload.batch,
            "status": upload.status,
            "uploaded_at": upload.uploaded_at.isoformat(),
            "total_count": len(rows),
            "duplicate_count": duplicate_count,
            "possible_duplicates": possible_duplicates,
        },
        "summary": summary,
        "rows": [
            {
                "id": row.id,
                "full_name": row.full_name,
                "academic_year": row.academic_year,
                "semester": row.semester,
                "scholarship": row.scholarship,
                "batch": row.batch,
                "hei": row.hei,
                "duplicate": row.duplicate,
                "duplicate_with_name": row.duplicate_with_name,
                "duplicate_with_hei": row.duplicate_with_hei,
                "duplicate_with_scholarship": row.duplicate_with_scholarship,
                "duplicate_with_batch": row.duplicate_with_batch,
                "match_type": row.match_type,
                "match_score": row.match_score,
                "matched_record": {
                    "full_name": row.duplicate_with_name,
                    "hei": row.duplicate_with_hei,
                    "scholarship": row.duplicate_with_scholarship,
                    "batch": row.duplicate_with_batch,
                    "match_type": row.match_type,
                    "match_score": row.match_score,
                } if row.duplicate == "YES" else None,
            }
            for row in rows
        ],
    }


@app.delete("/api/uploads/{upload_id}")
def delete_upload(upload_id: str, _: User = Depends(require_admin), db: Session = Depends(get_db)) -> dict[str, str]:
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found.")
    db.delete(upload)
    db.commit()
    return {"message": "Upload deleted successfully."}


def detect_duplicates(df: pd.DataFrame, existing_rows: list[Grantee]) -> pd.DataFrame:
    df = df.copy()
    df["Duplicate"] = "NO"
    df["DuplicateWithScholarship"] = ""
    df["DuplicateWithName"] = ""
    df["DuplicateWithHEI"] = ""
    df["DuplicateWithBatch"] = ""
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
            "batch": normalize_token(item.batch),
        }
        exact_existing[key].append(payload)
        blocked_existing[get_name_block(key)].append(payload)

    local_seen_exact: dict[str, list[dict[str, str]]] = defaultdict(list)
    local_blocked: dict[str, list[dict[str, str]]] = defaultdict(list)

    for i, row in df.iterrows():
        current_name = normalize_token(row["FullName"])
        current_sch = normalize_token(row["Scholarship"])
        current_hei = normalize_token(row["HEI"])
        current_batch = normalize_token(row["Batch"])
        block = get_name_block(current_name)

        best_match: dict[str, object] | None = None

        def consider(candidate: dict[str, str], match_type: str, score: float) -> None:
            nonlocal best_match
            new_match = {
                "name": candidate["name"],
                "scholarship": candidate["scholarship"],
                "hei": candidate["hei"],
                "batch": candidate["batch"],
                "match_type": match_type,
                "score": round(float(score), 2),
                "priority": 2 if match_type == "exact_duplicate" else 1,
            }
            if best_match is None or (new_match["priority"], new_match["score"]) > (
                best_match["priority"],
                best_match["score"],
            ):
                best_match = new_match

        for candidate in exact_existing.get(current_name, []):
            if candidate["scholarship"] == current_sch and candidate["hei"] == current_hei:
                consider(candidate, "exact_duplicate", 100)

        for candidate in local_seen_exact.get(current_name, []):
            if candidate["scholarship"] == current_sch and candidate["hei"] == current_hei:
                consider(candidate, "exact_duplicate", 100)

        if best_match is None and block:
            candidates = blocked_existing.get(block, []) + local_blocked.get(block, [])
            unique_candidates = []
            seen_keys = set()
            for candidate in candidates:
                if candidate["scholarship"] != current_sch or candidate["hei"] != current_hei:
                    continue
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
            df.at[i, "DuplicateWithBatch"] = best_match["batch"]
            df.at[i, "MatchType"] = best_match["match_type"]
            df.at[i, "MatchScore"] = best_match["score"]

        new_payload = {"name": current_name, "scholarship": current_sch, "hei": current_hei, "batch": current_batch}
        if current_name:
            local_seen_exact[current_name].append(new_payload)
            local_blocked[block].append(new_payload)

    return df


@app.post("/api/uploads")
def create_upload(
    file: UploadFile = File(...),
    hei: str = Form(...),
    scholarship: str = Form(...),
    academic_year: str = Form(...),
    semester: str = Form(...),
    batch: str = Form(...),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file selected.")
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed.")

    selected = {
        "hei": normalize_token(hei),
        "scholarship": normalize_token(scholarship),
        "academic_year": normalize_token(academic_year),
        "semester": normalize_token(semester),
        "batch": normalize_token(batch),
    }
    if not all(selected.values()):
        raise HTTPException(status_code=400, detail="All dropdown values are required.")

    active_options = db.query(MasterOption).filter(MasterOption.is_active.is_(True)).all()
    allowed = {(opt.category, opt.normalized_name) for opt in active_options}
    for category, value in selected.items():
        if (category, value) not in allowed:
            raise HTTPException(status_code=400, detail=f"Invalid {category.replace('_', ' ')} selected.")

    upload_id = str(uuid.uuid4())
    suffix = ".xlsx" if file.filename.lower().endswith(".xlsx") else ".xls"
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
        raise HTTPException(status_code=400, detail=f"Invalid Excel format. Missing columns: {', '.join(missing_columns)}")

    df = df[REQUIRED_COLUMNS].fillna("")
    if df.empty:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")

    for col in NAME_COLUMNS:
        df[col] = df[col].map(normalize_token)

    if (df[NAME_COLUMNS].apply(lambda col: col.eq("")).all(axis=1)).any():
        raise HTTPException(status_code=400, detail="Each row must contain at least one name value.")

    df["HEI"] = selected["hei"]
    df["Scholarship"] = selected["scholarship"]
    df["AcademicYear"] = selected["academic_year"]
    df["Semester"] = selected["semester"]
    df["Batch"] = selected["batch"]
    df["FullName"] = df.apply(normalize_name, axis=1)

    existing_rows = (
        db.query(Grantee)
        .filter(
            Grantee.hei == selected["hei"],
            Grantee.scholarship == selected["scholarship"],
            Grantee.academic_year == selected["academic_year"],
            Grantee.semester == selected["semester"],
        )
        .all()
    )

    df = detect_duplicates(df, existing_rows)
    duplicate_count = int((df["Duplicate"] == "YES").sum())
    possible_count = int((df["MatchType"] == "possible_duplicate").sum())
    status = determine_upload_status(duplicate_count, possible_count)
    label = make_upload_label(hei, scholarship, academic_year, semester, batch)

    db.add(
        Upload(
            id=upload_id,
            filename=label,
            hei=selected["hei"],
            scholarship=selected["scholarship"],
            academic_year=selected["academic_year"],
            semester=selected["semester"],
            batch=selected["batch"],
            status=status,
        )
    )
    db.bulk_save_objects(
        [
            Grantee(
                full_name=row["FullName"],
                academic_year=row["AcademicYear"],
                semester=row["Semester"],
                scholarship=row["Scholarship"],
                batch=row["Batch"],
                hei=row["HEI"],
                upload_id=upload_id,
                duplicate=row["Duplicate"],
                duplicate_with_name=row["DuplicateWithName"],
                duplicate_with_hei=row["DuplicateWithHEI"],
                duplicate_with_scholarship=row["DuplicateWithScholarship"],
                duplicate_with_batch=row["DuplicateWithBatch"],
                match_type=row["MatchType"],
                match_score=float(row["MatchScore"] or 0),
            )
            for _, row in df.iterrows()
        ]
    )
    db.commit()
    return {"upload_id": upload_id, "status": status}
