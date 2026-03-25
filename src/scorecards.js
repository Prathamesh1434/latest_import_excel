/* ═══════════════════════════════════════════════════════════
   SCORECARDS REGISTRY
   File: assets/scorecards.js

   ── HOW TO ADD A NEW SCORECARD ──────────────────────────────
   Copy one object block below, paste it at the end of the
   array (before the closing bracket), and fill in your values.

   Fields:
     id       → unique slug (no spaces) — used later for chatbot
     icon     → any emoji
     name     → full display name of the scorecard
     desc     → short subtitle / KRI codes shown below the name
     region   → label shown on the card  (UK / SGP / CEP / ALL)
     rag      → RAG status colour:  "red" | "amber" | "green" | "na"
     url      → full Tableau scorecard URL
   ═══════════════════════════════════════════════════════════ */

const SCORECARDS = [
  {
    id:     "crmr-cde",
    icon:   "📊",
    name:   "CRMR CDE Exceptions Scorecard",
    desc:   "CDE Data Quality in CRMR · UK-K11",
    region: "UK",
    rag:    "amber",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/LandingPageRiskSystems"
  },
  {
    id:     "uk-kri",
    icon:   "🎯",
    name:   "UK KRI Scorecard",
    desc:   "UK-K40 · UK-K41 · UK-K42 · UK-K43",
    region: "UK",
    rag:    "red",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/UKKRIScorecard"
  },
  {
    id:     "kri-dashboard",
    icon:   "📈",
    name:   "KRI Dashboard",
    desc:   "Overall: 82.35% · 34 Total Rules",
    region: "UK",
    rag:    "amber",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/KRIDashboard"
  },
  {
    id:     "dcrm-details",
    icon:   "🔍",
    name:   "DCRM Details",
    desc:   "CDE Rules by DCRM · Risk Systems",
    region: "UK",
    rag:    "na",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/DCRMDetailsRiskSystems"
  },
  {
    id:     "sgp-dq",
    icon:   "🌏",
    name:   "SGP DQ Scorecard",
    desc:   "Singapore Data Quality Metrics",
    region: "SGP",
    rag:    "green",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/SGPScorecard"
  },
  {
    id:     "cep",
    icon:   "🏦",
    name:   "CEP Scorecard",
    desc:   "Central Eastern Europe & Poland",
    region: "CEP",
    rag:    "green",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CEPScorecard"
  },
  {
    id:     "bi-vault",
    icon:   "🗄️",
    name:   "B&I Vault",
    desc:   "Evidence Store · All Controls",
    region: "ALL",
    rag:    "na",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/BIVault"
  },
  {
    id:     "it-audit",
    icon:   "🛡️",
    name:   "IT Audit Controls",
    desc:   "7 Codified Controls · Workstream 2",
    region: "UK",
    rag:    "na",
    url:    "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/ITAuditControls"
  }

  /* ── ADD NEW SCORECARD HERE ─────────────────────────────────
  ,{
    id:     "your-id",
    icon:   "📋",
    name:   "Your Scorecard Name",
    desc:   "Short description or KRI codes",
    region: "UK",
    rag:    "green",
    url:    "https://your-tableau-url-here"
  }
  ─────────────────────────────────────────────────────────── */
];
