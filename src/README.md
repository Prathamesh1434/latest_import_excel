# B&I Controls Hub
### B&I Data Metrics and Controls — Scorecard Navigation Page

---

## Folder Structure

```
bi-controls-hub/
│
├── index.html              ← Main page (open this in a browser)
│
├── assets/
│   ├── style.css           ← All styles (Citi design tokens)
│   ├── scorecards.js       ← Scorecard list — EDIT THIS to add/update scorecards
│   ├── app.js              ← Renders the cards (do not edit)
│   └── logo.png            ← PUT YOUR LOGO FILE HERE
│
└── README.md               ← This file
```

---

## How to Run

### Option 1 — Open directly in a browser (simplest)
1. Unzip / download the folder
2. Double-click `index.html`
3. It opens in your default browser — done

> ⚠️ Note: Some browsers block local files loading other local files.
> If cards don't appear, use Option 2 below.

---

### Option 2 — Run a local server (recommended)

**Using Python (no install needed on Mac/Linux):**
```bash
cd bi-controls-hub
python -m http.server 8080
```
Then open → http://localhost:8080

**Using Python on Windows:**
```cmd
cd bi-controls-hub
python -m http.server 8080
```
Then open → http://localhost:8080

**Using Node.js (if installed):**
```bash
cd bi-controls-hub
npx serve .
```
Then open the URL it prints.

---

## How to Add Your Organisation Logo

1. Get your logo file (PNG or SVG recommended, transparent background)
2. Rename it to `logo.png`
3. Place it inside the `assets/` folder
4. Refresh the page — logo appears automatically in the top-left

**Logo sizing tip:**  
The logo displays at `height: 30px`. If it looks too small or large,
open `assets/style.css` and change this line:
```css
.org-logo {
  height: 30px;   ← change this value
}
```

**Supported formats:** `.png` `.jpg` `.svg` `.webp`  
*(Just rename your file to `logo.png` or update the `src` in `index.html`)*

If no logo file is found, the page shows "citi" as a text fallback automatically.

---

## How to Add or Edit Scorecards

Open `assets/scorecards.js` — it's the only file you need to change.

Each scorecard is one object:
```js
{
  id:     "your-unique-id",      // no spaces, used for chatbot later
  icon:   "📊",                  // any emoji
  name:   "Scorecard Full Name",
  desc:   "Short description",
  region: "UK",                  // UK / SGP / CEP / ALL
  rag:    "green",               // "red" | "amber" | "green" | "na"
  url:    "https://your-tableau-link"
}
```

Add it inside the `SCORECARDS = [ ... ]` array. The grid updates automatically.

---

## How to Update User Name / Environment Tag

Open `index.html` and find these lines:

**User name:**
```html
<div class="avatar">PG</div>
<span>Prathmesh Gayakwad</span>
```
Replace `PG` and the name with your initials and full name.

**Environment tag** (UAT / PROD):
```html
<span class="env-tag">UAT</span>
```
Change `UAT` to `PROD` when moving to production.

---

## Browser Support

Works in all modern browsers:
- Chrome ✓
- Edge ✓
- Firefox ✓
- Safari ✓

No internet required after first load (fonts load from Google — optional).

---

## Step 2 (Coming Next)
Each scorecard will have a scoped AI chatbot.
The `data-id` attribute on each card is already in place for wiring up the chatbot.

---

*B&I Data Analytics · Citi — Confidential · Workstream 2: Controls Codification*
