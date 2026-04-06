# Oracle DDL — Setup Instructions

## What This Creates

5 tables for the B&I Controls Hub:

| Table | Purpose | Key Column |
|---|---|---|
| `BI_CHAT_SESSION` | One row per conversation | `SESSION_ID` (UUID) |
| `BI_CHAT_HISTORY` | Every chat message | `ROLE` user/assistant |
| `BI_API_AUDIT` | Immutable compliance log | `ENDPOINT`, `STATUS_CODE` |
| `BI_SNAPSHOT_LOG` | Tableau fetch audit | `VIEW_ID`, `FILE_TYPE` |
| `BI_DATA_CONTEXT` | LLM context cache | `SOURCE_ID`, `CHUNK_TYPE` |

---

## Prerequisites

- Oracle 12c or later (uses `GENERATED ALWAYS AS IDENTITY`)
- Your user must have `CREATE TABLE` privilege
- `CREATE INDEX` privilege
- If granting to another user: `GRANT` privilege

---

## Method 1 — SQL*Plus (command line)

```cmd
conda activate prath
sqlplus YOUR_USER/YOUR_PASSWORD@YOUR_DSN

-- Once connected:
@C:\Users\PG23137\Desktop\Control_cod_dev\CC AI\bi-complete\sql\oracle_ddl_complete.sql
```

Or in one line:
```cmd
sqlplus YOUR_USER/YOUR_PASSWORD@HOST:1521/SERVICE @sql\oracle_ddl_complete.sql
```

---

## Method 2 — SQL Developer (GUI)

1. Open **SQL Developer**
2. Connect to your Oracle database
3. Go to **File → Open** → navigate to `sql/oracle_ddl_complete.sql`
4. Press **F5** (Run as Script) — NOT F9 (Run Statement)
5. Check the **Script Output** tab at the bottom
6. You should see:
   ```
   Table BI_CHAT_SESSION created.
   Table BI_CHAT_HISTORY created.
   Table BI_API_AUDIT created.
   Table BI_SNAPSHOT_LOG created.
   Table BI_DATA_CONTEXT created.
   ```

---

## Method 3 — VS Code with Oracle Extension

1. Install **Oracle Developer Tools for VS Code**
2. Open `sql/oracle_ddl_complete.sql`
3. Right-click → **Execute as Script** (not Execute Statement)
4. Select your connection

---

## Method 4 — Python (oracledb)

Run this from your `prath` conda environment:

```python
import oracledb
import os
from dotenv import load_dotenv
load_dotenv()

conn = oracledb.connect(
    user     = os.getenv("ORACLE_USER"),
    password = os.getenv("ORACLE_PASSWORD"),
    dsn      = os.getenv("ORACLE_DSN"),
)

with open("sql/oracle_ddl_complete.sql") as f:
    ddl = f.read()

# Execute each statement separately (split on semicolons)
statements = [s.strip() for s in ddl.split(";") if s.strip() and not s.strip().startswith("--")]

with conn.cursor() as cur:
    for stmt in statements:
        if stmt.upper().startswith(("CREATE","INSERT","UPDATE","GRANT","COMMENT")):
            try:
                cur.execute(stmt)
                print(f"✅ {stmt[:60]}…")
            except Exception as e:
                print(f"⚠️  {e} — {stmt[:60]}")

conn.commit()
conn.close()
print("Done")
```

---

## Verification

After running the DDL, verify with:

```sql
-- Should return 5 rows
SELECT TABLE_NAME FROM USER_TABLES
WHERE TABLE_NAME LIKE 'BI_%'
ORDER BY TABLE_NAME;

-- Should return 20+ indexes
SELECT INDEX_NAME, TABLE_NAME FROM USER_INDEXES
WHERE TABLE_NAME LIKE 'BI_%'
ORDER BY TABLE_NAME, INDEX_NAME;
```

---

## Your `.env` file

Set these for Oracle connectivity:

```
ORACLE_USER=your_oracle_username
ORACLE_PASSWORD=your_oracle_password
ORACLE_DSN=host:1521/service_name
ORACLE_POOL_MIN=2
ORACLE_POOL_MAX=10
```

DSN format examples:
```
# Thin mode (no Oracle Client needed)
ORACLE_DSN=myhost.citigroup.net:1521/BICPROD

# Thick mode (with Oracle Client)
ORACLE_DSN=myhost:1521/BICPROD
```

---

## Running from Project Root

The backend reads `.env` automatically. Oracle is **optional** — the app works without it, but history/analytics are disabled.

```cmd
conda activate prath
cd "C:\Users\PG23137\Desktop\Control_cod_dev\CC AI\bi-complete"
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

---

## Troubleshooting

| Error | Fix |
|---|---|
| `ORA-01031: insufficient privileges` | Ask DBA to grant `CREATE TABLE` to your user |
| `ORA-00955: name is already used` | Tables exist — run rollback script first, then re-run |
| `ORA-02291: integrity constraint violated` | Run in correct order (SESSION before HISTORY) |
| `ORA-12154: TNS could not resolve` | Check `ORACLE_DSN` in `.env` |
| `DPI-1047: Cannot locate a 64-bit Oracle Client` | Use thin mode: `oracledb.init(driver_mode=thin)` |

---

## Rollback (undo everything)

```sql
DROP TABLE BI_CHAT_HISTORY  CASCADE CONSTRAINTS;
DROP TABLE BI_CHAT_SESSION  CASCADE CONSTRAINTS;
DROP TABLE BI_API_AUDIT     CASCADE CONSTRAINTS;
DROP TABLE BI_SNAPSHOT_LOG  CASCADE CONSTRAINTS;
DROP TABLE BI_DATA_CONTEXT  CASCADE CONSTRAINTS;
```

Run in SQL Developer or SQL*Plus. Order matters — `BI_CHAT_HISTORY` must be dropped before `BI_CHAT_SESSION` due to the foreign key.

---

## Oracle is Optional

If Oracle is not available, the application runs in degraded mode:

| Feature | Without Oracle | With Oracle |
|---|---|---|
| Scorecard hub | ✅ Works | ✅ Works |
| Tableau snapshots | ✅ Works | ✅ Works |
| AI chat | ✅ Works | ✅ Works |
| Chat history saved | ❌ No | ✅ Yes |
| Analytics tab | ❌ No data | ✅ Full data |
| Session resume | ❌ No | ✅ Yes |
| Compliance audit | ❌ No | ✅ Yes |

The system logs a warning at startup if Oracle is not configured but continues running normally.
