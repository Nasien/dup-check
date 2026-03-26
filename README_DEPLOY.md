# Duplicate Checker Deployment Notes

## Stack
- FastAPI
- PostgreSQL on Neon
- Deployable to Render or Vercel

## Required environment variables
```env
DATABASE_URL=postgresql://username:password@host/database?sslmode=require
SIMILARITY_THRESHOLD=90
MAX_FUZZY_CANDIDATES=300
DB_SSLMODE=require
```

## What changed in this version
- Added faster duplicate matching using exact match first, then surname-blocked fuzzy matching.
- Added match details in the database: matched name, matched HEI, match type, and similarity score.
- Added stronger TES vs TDP cross-program detection.
- Improved the UI for upload history and result review.
- Added database indexes for common filters.

## Render
Build command:
```bash
pip install -r requirements.txt
```

Start command:
```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Vercel
- Import the repository.
- Framework preset: `Other`.
- Add the same environment variables.
- Keep the included `vercel.json` file.

## Upload format
Required columns:
- LastName
- FirstName
- MiddleName
- Scholarship
- AcademicYear
- Semester
- HEI

## Notes
- Vercel does not keep local uploaded files permanently, so uploaded Excel files are processed in temporary storage and results are saved in PostgreSQL.
- Duplicate detection is evaluated against existing rows in the same Academic Year and Semester, plus duplicates inside the current uploaded file.
