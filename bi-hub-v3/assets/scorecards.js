/* ═══════════════════════════════════════════════════════════════
   scorecards.js — EDIT THIS FILE to add / update scorecards

   HOW TO ADD A NEW SCORECARD:
   Copy one object, paste at end of array, fill in your values.

   view_id : Tableau View ID — Preview button only shows when set
   chips   : Suggested questions shown in the chatbot
   url     : Full Tableau dashboard URL
   ═══════════════════════════════════════════════════════════════ */

const SCORECARDS = [
  {
    id:      "crmr-cde",
    icon:    "📊",
    name:    "CRMR CDE Exceptions Scorecard",
    desc:    "CDE Data Quality in CRMR · UK-K11",
    region:  "UK",
    rag:     "amber",
    view_id: "",   // ← paste your Tableau View ID here
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/LandingPageRiskSystems",
    chips:   ["How many CDE rules failed?", "Show Fail Red details", "Currency Code status?", "Which CDEs have most defects?"]
  },
  {
    id:      "uk-kri",
    icon:    "🎯",
    name:    "UK KRI Scorecard",
    desc:    "UK-K40 · UK-K41 · UK-K42 · UK-K43",
    region:  "UK",
    rag:     "red",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/UKKRIScorecard",
    chips:   ["Which KRIs are Red?", "UK-K41 status?", "Show variance breaches", "Trend for UK-K41?"]
  },
  {
    id:      "kri-dashboard",
    icon:    "📈",
    name:    "KRI Dashboard",
    desc:    "Overall: 82.35% · 34 Total Rules",
    region:  "UK",
    rag:     "amber",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/KRIDashboard",
    chips:   ["Overall KRI result?", "Solo vs Consolidated?", "Lowest scoring product?"]
  },
  {
    id:      "dcrm-details",
    icon:    "🔍",
    name:    "DCRM Details",
    desc:    "CDE Rules by DCRM · Risk Systems",
    region:  "UK",
    rag:     "na",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CRMRCDEExceptionsScorecard_2/DCRMDetailsRiskSystems",
    chips:   ["Show all CDEs for CRMR", "Completeness status?", "Any Accuracy failures?"]
  },
  {
    id:      "sgp-dq",
    icon:    "🌏",
    name:    "SGP DQ Scorecard",
    desc:    "Singapore Data Quality Metrics",
    region:  "SGP",
    rag:     "green",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/SGPScorecard",
    chips:   ["SGP RAG status?", "Any SGP breaches?"]
  },
  {
    id:      "cep",
    icon:    "🏦",
    name:    "CEP Scorecard",
    desc:    "Central Eastern Europe and Poland",
    region:  "CEP",
    rag:     "green",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/CEPScorecard",
    chips:   ["CEP RAG status?", "Any CEP breaches?"]
  },
  {
    id:      "bi-vault",
    icon:    "🗄️",
    name:    "B&I Vault",
    desc:    "Evidence Store · All Controls",
    region:  "ALL",
    rag:     "na",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/BIVault",
    chips:   ["What evidence is stored?", "Which controls link here?"]
  },
  {
    id:      "it-audit",
    icon:    "🛡️",
    name:    "IT Audit Controls",
    desc:    "7 Codified Controls · Workstream 2",
    region:  "UK",
    rag:     "na",
    view_id: "",
    url:     "https://uat.bidata.analytics.global.citigroup.net/#/site/BI_DATA/views/ITAuditControls",
    chips:   ["List all 7 controls", "What is Control 1088848?", "Which controls are event-driven?"]
  }
];
