# B&I Controls Hub — v3 Setup Guide

## Folder Structure

```
bi-hub-v3/
│
├── index.html                  ← Open this in browser (main hub)
├── chatbot.html                ← Chatbot popup (auto-opened by hub)
│
├── assets/
│   ├── style.css               ← All styles
│   ├── scorecards.js           ← ADD YOUR SCORECARDS HERE
│   ├── app.js                  ← Renders cards (do not edit)
│   └── logo.png                ← PUT YOUR LOGO HERE (rename your file)
│
└── chatbot/
    ├── chat_api.py             ← FastAPI backend (run this)
    ├── tableau_client.py       ← Tableau TSC connection
    ├── context_loader.py       ← Loads YAML metadata
    ├── .env.example            ← Copy to .env, fill in values
    └── metadata/
        ├── _template.yaml      ← Copy for new scorecards
        ├── crmr-cde.yaml
        ├── uk-kri.yaml
        └── kri-dashboard.yaml
```

---

## Step 1 — Create .env file

```cmd
cd chatbot
copy .env.example .env
```

Open `.env` in Notepad and fill in:

```
GOOGLE_PROJECT_ID=your-gcp-project-id
GOOGLE_LOCATION=us-central1
VERTEX_MODEL=gemini-1.5-pro

TABLEAU_SERVER=https://uat.bidata.analytics.global.citigroup.net
TABLEAU_USERNAME=PG23137
TABLEAU_PASSWORD=your-password
TABLEAU_SITE=BI_DATA
TABLEAU_API_VERSION=3.0
TABLEAU_SSL_CERT_PATH=C:\path\to\cert.pem
```

---

## Step 2 — Add your logo (optional)

Rename your logo file to `logo.png` and place it in `assets/`.
If missing, "citi" text shows as fallback automatically.

---

## Step 3 — Add View IDs to scorecards.js

Open `assets/scorecards.js`.
For each scorecard, paste your Tableau View ID:

```js
view_id: "your-actual-view-id",
```

The Preview button only appears when a view_id is set.

---

## Step 4 — Authenticate with Google (one time only)

```cmd
conda activate prath
gcloud auth application-default login
```

This opens a browser → log in → credentials saved automatically.

---

## Step 5 — Start Backend (Terminal 1)

```cmd
conda activate prath
cd "C:\path\to\bi-hub-v3\chatbot"
uvicorn chat_api:app --host 0.0.0.0 --port 8000 --reload
```

Test: http://localhost:8000/health
Should return: {"status": "ok"}

---

## Step 6 — Start Frontend (Terminal 2)

```cmd
cd "C:\path\to\bi-hub-v3"
python -m http.server 8080
```

Open: http://localhost:8080

---

## What Each Button Does

| Button | What happens |
|---|---|
| Preview | Fetches PNG snapshot from Tableau API |
| Download PDF | Downloads PDF via Tableau API |
| Chat | Opens AI chatbot popup scoped to that scorecard |
| Open | Opens full interactive Tableau in new tab |

---

## Add Chatbot Context for a New Scorecard

1. Copy `chatbot\metadata\_template.yaml`
2. Rename to `your-scorecard-id.yaml` (must match id in scorecards.js)
3. Fill in KRI values, thresholds, RAG status
4. Restart backend

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Preview blank/error | Check view_id in scorecards.js + backend running |
| "Tableau snapshot error" | Check .env credentials + SSL cert path |
| Chat not working | Check GOOGLE_PROJECT_ID in .env + gcloud auth |
| Cards not loading | Use python -m http.server, not direct file open |
| Path error on Windows | Wrap paths with spaces in quotes |

---

*B&I Data Analytics · Citi Confidential · Workstream 2: Controls Codification*
