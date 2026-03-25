/* ═══════════════════════════════════════════════════════════
   B&I Controls Hub — App Logic
   File: assets/app.js
   (No need to edit this file)
   ═══════════════════════════════════════════════════════════ */

const RAG_PILL = {
  red:   '<span class="rag-pill rp-red">RED</span>',
  amber: '<span class="rag-pill rp-amber">AMBER</span>',
  green: '<span class="rag-pill rp-green">GREEN</span>',
  na:    '<span class="rag-pill rp-na">N/A</span>',
};

function renderGrid() {
  const grid = document.getElementById("sc-grid");
  if (!grid) return;

  grid.innerHTML = SCORECARDS.map(sc => `
    <a
      class="sc-card"
      href="${sc.url}"
      target="_blank"
      rel="noopener noreferrer"
      data-id="${sc.id}"
      data-rag="${sc.rag}"
      title="Open ${sc.name}"
    >
      <div class="card-top">
        <div class="card-icon">${sc.icon}</div>
        ${RAG_PILL[sc.rag] || RAG_PILL.na}
      </div>

      <div>
        <div class="card-name">${sc.name}</div>
        <div class="card-desc">${sc.desc}</div>
      </div>

      <div class="card-footer">
        <span class="card-region">${sc.region}</span>
        <span class="card-open-link">
          Open
          <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/>
          </svg>
        </span>
      </div>
    </a>
  `).join("");
}

document.addEventListener("DOMContentLoaded", renderGrid);
