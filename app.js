/* ═══════════════════════════════════════════════════════
   app.js — Renders cards + snapshot modal
   ═══════════════════════════════════════════════════════ */

const API = "http://localhost:8000";

const RAG_PILL = {
  red:   '<span class="rag-pill rp-red">RED</span>',
  amber: '<span class="rag-pill rp-amber">AMBER</span>',
  green: '<span class="rag-pill rp-green">GREEN</span>',
  na:    '<span class="rag-pill rp-na">N/A</span>',
};

/* ── Render cards ─────────────────────────────────────────── */
function renderGrid() {
  const grid = document.getElementById("sc-grid");
  if (!grid) return;

  grid.innerHTML = SCORECARDS.map(sc => `
    <div class="sc-card" data-id="${sc.id}" data-rag="${sc.rag}">
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
        <div class="card-actions">

          ${sc.view_id && sc.view_id !== "YOUR_VIEW_ID_HERE" ? `
          <button class="btn-preview"
            onclick="openSnapshot('${sc.view_id}', '${sc.name.replace(/'/g,"\\'")}', '${sc.id}', ${JSON.stringify(sc.chips||[])})">
            🖼 Preview
          </button>` : ""}

          <button class="btn-chat"
            onclick="openChatPopup('${sc.id}', '${sc.name.replace(/'/g,"\\'")}', ${JSON.stringify(sc.chips||[])})">
            🤖 Chat
          </button>

          <a class="btn-open" href="${sc.url}" target="_blank" rel="noopener noreferrer">
            Open ↗
          </a>

        </div>
      </div>
    </div>
  `).join("");
}

/* ── Snapshot modal ───────────────────────────────────────── */
function openSnapshot(viewId, name, scorecardId, chips) {
  const modal     = document.getElementById("snap-modal");
  const modalName = document.getElementById("snap-name");
  const imgEl     = document.getElementById("snap-img");
  const spinner   = document.getElementById("snap-spinner");
  const errEl     = document.getElementById("snap-error");
  const dlBtn     = document.getElementById("snap-download");
  const chatBtn   = document.getElementById("snap-chat-btn");

  // Reset state
  modalName.textContent = name;
  imgEl.style.display   = "none";
  imgEl.src             = "";
  errEl.style.display   = "none";
  spinner.style.display = "flex";
  dlBtn.href            = `${API}/snapshot/${viewId}/pdf`;
  chatBtn.onclick       = () => openChatPopup(scorecardId, name, chips);

  modal.style.display = "flex";
  document.body.style.overflow = "hidden";

  // Fetch PNG from FastAPI → Tableau API
  imgEl.onload = () => {
    spinner.style.display = "none";
    imgEl.style.display   = "block";
  };

  imgEl.onerror = () => {
    spinner.style.display = "none";
    errEl.style.display   = "block";
  };

  imgEl.src = `${API}/snapshot/${viewId}?resolution=1920&t=${Date.now()}`;
}

function closeSnapshot() {
  document.getElementById("snap-modal").style.display = "none";
  document.body.style.overflow = "";
  // Stop loading image
  const img = document.getElementById("snap-img");
  img.src = "";
}

// Close on backdrop click
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("snap-modal").addEventListener("click", function(e) {
    if (e.target === this) closeSnapshot();
  });
  // ESC key
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeSnapshot();
  });

  renderGrid();
});

/* ── Chat popup ───────────────────────────────────────────── */
function openChatPopup(id, name, chips = []) {
  const params = new URLSearchParams({
    id,
    name,
    chips: encodeURIComponent(JSON.stringify(chips))
  });

  const W = 400, H = 620;
  const left = window.screen.width - W - 20;
  const top  = Math.round((window.screen.height - H) / 2);

  const popup = window.open(
    `chatbot.html?${params.toString()}`,
    `bi-chat-${id}`,
    `width=${W},height=${H},left=${left},top=${top},resizable=yes,scrollbars=no,toolbar=no,menubar=no,location=no`
  );

  if (popup) popup.focus();
}
