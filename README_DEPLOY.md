# Duplicate Checker Deployment Guide

This project is now split into:

- `backend/` -> FastAPI API for Render
- `frontend/` -> static UI for Vercel
- Database -> Neon PostgreSQL

## 1) Neon

Create a Neon project, then copy the connection string.

Example:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
```

## 2) Render backend

Create a new **Web Service** from the `backend` folder.

Use these settings:

- Root Directory: `backend`
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`

Add environment variables:

```env
DATABASE_URL=your_neon_connection_string
DB_SSLMODE=require
SIMILARITY_THRESHOLD=90
MAX_FUZZY_CANDIDATES=300
FRONTEND_ORIGIN=https://your-frontend.vercel.app
```

Test after deploy:

- `https://your-render-app.onrender.com/api/healthz`
- `https://your-render-app.onrender.com/api/uploads`

## 3) Vercel frontend

In `frontend/index.html`, replace this line value:

```js
API_BASE_URL: "https://your-render-backend.onrender.com"
```

with your real Render URL.

Then deploy the `frontend` folder to Vercel.

Recommended Vercel settings:

- Framework Preset: `Other`
- Root Directory: `frontend`

## 4) How it works now

- Frontend uploads the Excel file to `POST /api/uploads`
- Backend reads the file, stores results in Neon, and returns an `upload_id`
- Frontend loads history from `GET /api/uploads`
- Frontend loads results from `GET /api/uploads/{upload_id}`
- Frontend deletes uploads through `DELETE /api/uploads/{upload_id}`

## 5) Required Excel columns

- LastName
- FirstName
- MiddleName
- Scholarship
- AcademicYear
- Semester
- HEI

## 6) Local testing

Backend:

```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --reload
```

Frontend:

Open `frontend/index.html` with Live Server, or any static server.

## 7) Important note

The frontend is now separate from the backend, so `fetch("/upload")` is no longer used.
All requests go to the Render API base URL.
