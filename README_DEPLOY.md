# Duplicate Grantee Checker Deployment Guide

## Stack
- Frontend: Vercel (`frontend/`)
- Backend: Render (`backend/`)
- Database: Neon PostgreSQL

## Important app rules
- Upload file columns: `LastName`, `FirstName`, `MiddleName`
- Dropdown metadata is selected in the UI
- Duplicate checking only happens within the same:
  - HEI
  - Scholarship
  - Academic Year
  - Semester
- Batch is saved and displayed, but it does not control duplicate matching

## Backend deploy (Render)
1. Push this project to GitHub.
2. Create a new Render Web Service.
3. Set **Root Directory** to `backend`.
4. Use:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
5. Add environment variables:
   - `DATABASE_URL`
   - `SECRET_KEY`
   - `SIMILARITY_THRESHOLD=90`
   - `MAX_FUZZY_CANDIDATES=300`
   - `DB_SSLMODE=require`
   - `FRONTEND_ORIGIN=https://your-frontend.vercel.app`
   - `ADMIN_USERNAME=admin`
   - `ADMIN_PASSWORD=admin123`
6. Deploy and test `/api/healthz`.

## Frontend deploy (Vercel)
1. Import the same GitHub repo into Vercel.
2. Set **Root Directory** to `frontend`.
3. Edit `frontend/index.html` and replace:
   - `https://your-render-backend.onrender.com`
   with your actual Render backend URL.
4. Deploy.

## Admin login
Default admin credentials are controlled by environment variables:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

Change them in Render before you go live.

## Master data
The backend seeds:
- the HEI list provided by the user
- Scholarship options: `TES`, `TDP`
- Semester options: `1ST SEM`, `2ND SEM`

Academic Year and Batch are managed from the admin panel.
