/* ═══════════════════════════════════════════════════════
   app.js — Card rendering + modal + chat popup
   Uses data-index attributes — no inline onclick strings
   so special characters in names never break the UI.
   ═══════════════════════════════════════════════════════ */

const API = "http://localhost:8000";

const RAG_PILL = {
  red:   '<span class="rag-pill rp-red">RED</span>',
  amber: '<span class="rag-pill rp-amber">AMBER</span>',
  green: '<span class="rag-pill rp-green">GREEN</span>',
  na:    '<span class="rag-pill rp-na">N/A</span>',
};

/* ── Render cards ─────────────────────────────────────── */
function renderGrid() {
  const grid = document.getElementById("sc-grid");
  if (!grid) return;

  grid.innerHTML = SCORECARDS.map((sc, index) => {
    const hasViewId = sc.view_id && sc.view_id.trim() !== "";

    return `
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
            ${hasViewId ? `<button class="btn-preview" data-index="${index}">🖼 Preview</button>` : ""}
            <button class="btn-chat" data-index="${index}">🤖 Chat</button>
            <a class="btn-open" href="${sc.url}" target="_blank" rel="noopener noreferrer">Open ↗</a>
          </div>
        </div>
      </div>`;
  }).join("");

  attachHandlers();
}

/* ── Event listeners via data-index ──────────────────── */
function attachHandlers() {
  document.querySelectorAll(".btn-preview").forEach(btn => {
    btn.addEventListener("click", function() {
      const sc = SCORECARDS[parseInt(this.dataset.index)];
      openSnapshot(sc.view_id, sc.name, sc.id, sc.chips || []);
    });
  });

  document.querySelectorAll(".btn-chat").forEach(btn => {
    btn.addEventListener("click", function() {
      const sc = SCORECARDS[parseInt(this.dataset.index)];
      openChatPopup(sc.id, sc.name, sc.chips || []);
    });
  });
}

/* ── Snapshot modal state ─────────────────────────────── */
let _viewId = null, _scId = null, _name = null, _chips = [];

function openSnapshot(viewId, name, scId, chips) {
  _viewId = viewId; _scId = scId; _name = name; _chips = chips;

  const modal   = document.getElementById("snap-modal");
  const nameEl  = document.getElementById("snap-name");
  const imgEl   = document.getElementById("snap-img");
  const spinner = document.getElementById("snap-spinner");
  const errEl   = document.getElementById("snap-error");
  const dlBtn   = document.getElementById("snap-download");

  nameEl.textContent    = name;
  imgEl.style.display   = "none";
  imgEl.src             = "";
  errEl.style.display   = "none";
  spinner.style.display = "flex";
  dlBtn.href            = `${API}/snapshot/${viewId}/pdf`;

  modal.style.display          = "flex";
  document.body.style.overflow = "hidden";

  imgEl.onload  = () => { spinner.style.display = "none"; imgEl.style.display = "block"; };
  imgEl.onerror = () => { spinner.style.display = "none"; errEl.style.display = "block"; };
  imgEl.src = `${API}/snapshot/${viewId}?t=${Date.now()}`;
}

function closeSnapshot() {
  document.getElementById("snap-modal").style.display = "none";
  document.body.style.overflow = "";
  document.getElementById("snap-img").src = "";
}

function refreshSnapshot() {
  if (_viewId) openSnapshot(_viewId, _name, _scId, _chips);
}

/* ── Chat popup ───────────────────────────────────────── */
function openChatPopup(id, name, chips) {
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

/* ── Init ─────────────────────────────────────────────── */
document.addEventListener("DOMContentLoaded", () => {
  renderGrid();

  document.getElementById("snap-modal").addEventListener("click", function(e) {
    if (e.target === this) closeSnapshot();
  });

  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeSnapshot();
  });

  document.getElementById("snap-chat-btn").addEventListener("click", () => {
    if (_scId) openChatPopup(_scId, _name, _chips);
  });
});
