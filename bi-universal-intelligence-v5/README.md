# B&I Controls Hub — Enterprise v3

## Architecture

```
bi-final/
├── index.html              ← Main hub (3 tabs: Scorecards, Analytics, History)
├── chatbot.html            ← Standalone chatbot popup with chart rendering
├── .env.example            ← Copy → .env, fill credentials
│
├── backend/
│   ├── main.py             ← FastAPI entry point (RUN THIS)
│   ├── config.py           ← All config from .env
│   ├── routers/api.py      ← All routes in one file
│   ├── services/
│   │   ├── tableau_service.py   ← TSC + cache + retry
│   │   ├── vertex_service.py    ← Gemini + JSON chart response
│   │   └── oracle_service.py    ← Connection pool + history + analytics
│   ├── models/schemas.py   ← Pydantic models including ChartData
│   └── context/loader.py   ← YAML + CSV context builder
│
├── metadata/
│   ├── _template.yaml      ← Copy for new scorecards
│   ├── uk-kri.yaml
│   ├── crmr-cde.yaml
│   └── kri-dashboard.yaml
│
├── sql/init.sql            ← Oracle tables (run once)
└── tests/test_all.py       ← Full test suite
```

---

## How Charts Work

```
User types question
      ↓
POST /chat/ → Vertex AI Gemini
Gemini responds with JSON:
  {
    "reply": "2-3 sentence summary",
    "chart_type": "bar|line|pie|kpi|table|text",
    "chart": { chart-specific data }
  }
      ↓
Frontend reads chart_type
Renders Chart.js visualisation automatically
User can override chart type using the selector bar
```

---

## API Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/health/` | GET | All service health |
| `/snapshot/{view_id}` | GET | PNG snapshot |
| `/snapshot/{view_id}/pdf` | GET | PDF download |
| `/snapshot/{view_id}/csv` | GET | Raw CSV data |
| `/chat/` | POST | AI chat → reply + chart |
| `/history/sessions` | GET | User sessions |
| `/history/sessions/{id}` | GET | Session messages |
| `/analytics/summary` | GET | 30-day stats |
| `/scorecards/` | GET | Available scorecards |
| `/cache/stats` | GET | Snapshot cache status |
| `/docs` | GET | Swagger UI |

---

## Step 1 — Create .env

```cmd
copy .env.example .env
```
Fill in all values. No quotes around values. No spaces around =.

---

## Step 2 — Run Oracle SQL (one time)

Connect to Oracle and run `sql/init.sql`. Creates 4 tables. Additive only.

---

## Step 3 — Add View IDs

Open `index.html`. Find `SCORECARDS` array. Fill in `view_id` for each scorecard.

---

## Step 4 — Google Auth (one time)

```cmd
conda activate prath
gcloud auth application-default login
```

---

## Step 5 — Backend (Terminal 1)

```cmd
conda activate prath
cd "path\to\bi-final\backend"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Verify: http://localhost:8000/health/

---

## Step 6 — Frontend (Terminal 2)

```cmd
cd "path\to\bi-final"
python -m http.server 8080
```

Open: http://localhost:8080

---

## Step 7 — Run Tests

```cmd
conda activate prath
cd "path\to\bi-final"
python -m pytest tests/test_all.py -v
```

---

## What Works Without Oracle

Oracle is optional. Without it:
- ✅ Scorecard hub works
- ✅ Tableau snapshots work
- ✅ AI chat with charts works
- ❌ Chat history not saved
- ❌ Analytics tab shows placeholder

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Health dots grey | Backend not running |
| Snapshot error | Check view_id + Tableau credentials |
| Chat 503 | Set GOOGLE_PROJECT_ID + run gcloud auth |
| SCORECARDS not defined | Wrong folder — must serve from bi-final/ |
| Path spaces error | Wrap in quotes: `cd "C:\path with spaces"` |

---
*B&I Data Analytics · Citi Confidential · Workstream 2*
