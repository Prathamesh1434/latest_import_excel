# B&I Controls Hub v2 — Setup Guide

## What's New
- 🖼 **Preview button** — fetches live PNG snapshot via Tableau REST API
- ⬇ **Download PDF** — downloads PDF of the dashboard
- 🤖 **Chat button** — opens scoped AI chatbot popup
- **Open ↗** — opens full interactive Tableau in new tab

---

## Step 1 — Fill in .env

```cmd
cd bi-controls-hub-v2\chatbot
copy .env.example .env
```

Edit `.env`:
```
ANTHROPIC_API_KEY=your-key
TABLEAU_SERVER=https://your-tableau-server.com
TABLEAU_USERNAME=your-username
TABLEAU_PASSWORD=your-password
TABLEAU_SITE=                         # blank = Default site
TABLEAU_API_VERSION=3.18              # check your server version
TABLEAU_SSL_CERT_PATH=/path/to/cert.pem
```

---

## Step 2 — Add View IDs to scorecards.js

Open `assets/scorecards.js`.
For each scorecard replace `YOUR_VIEW_ID_HERE` with the Tableau View ID.

The Preview button only appears when a valid view_id is set.

---

## Step 3 — Run Backend (Terminal 1)

```cmd
conda activate prath
cd bi-controls-hub-v2\chatbot
uvicorn chat_api:app --host 0.0.0.0 --port 8000 --reload
```

Test: http://localhost:8000/health

---

## Step 4 — Run Frontend (Terminal 2)

```cmd
cd bi-controls-hub-v2
python -m http.server 8080
```

Open: http://localhost:8080

---

## How to Find Your Tableau API Version

In Tableau Server: Help menu → About Tableau Server → shows version number.
Map version to API:
- Tableau 2023.1+ → API 3.18
- Tableau 2022.4  → API 3.17
- Tableau 2022.3  → API 3.16

---

*B&I Data Analytics · Citi Confidential · Workstream 2*
