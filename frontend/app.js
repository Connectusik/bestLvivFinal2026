const form = document.getElementById("form");
const btn = document.getElementById("btn");
const btnLabel = document.getElementById("btnLabel");
const resultBox = document.getElementById("result");
const errorBox = document.getElementById("error");

const nfInt = new Intl.NumberFormat("uk-UA", { maximumFractionDigits: 0 });
const nfShort = new Intl.NumberFormat("uk-UA", { maximumFractionDigits: 1 });

function wireDrop(inputId, hintId) {
  const input = document.getElementById(inputId);
  const label = input.closest(".drop");
  const filePanel = document.getElementById(hintId);
  const nameEl = filePanel.querySelector(".drop-file-name");
  const sizeEl = filePanel.querySelector(".drop-file-size");
  const clearBtn = filePanel.querySelector(".drop-file-clear");

  input.addEventListener("change", render);

  ["dragenter", "dragover"].forEach(evt => {
    label.addEventListener(evt, e => { e.preventDefault(); label.classList.add("dragover"); });
  });
  ["dragleave", "drop"].forEach(evt => {
    label.addEventListener(evt, e => { e.preventDefault(); label.classList.remove("dragover"); });
  });
  label.addEventListener("drop", e => {
    if (e.dataTransfer?.files?.length) {
      input.files = e.dataTransfer.files;
      render();
    }
  });

  clearBtn.addEventListener("click", e => {
    e.preventDefault();
    e.stopPropagation();
    input.value = "";
    render();
  });

  function render() {
    const f = input.files?.[0];
    if (f) {
      label.classList.add("filled");
      nameEl.textContent = f.name;
      sizeEl.textContent = formatSize(f.size);
      filePanel.hidden = false;
    } else {
      label.classList.remove("filled");
      filePanel.hidden = true;
    }
  }
}

function formatSize(bytes) {
  if (bytes < 1024) return bytes + " Б";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " КБ";
  return (bytes / 1024 / 1024).toFixed(2) + " МБ";
}

wireDrop("land", "landFile");
wireDrop("realestate", "reFile");

loadReferencesBadge();

async function loadReferencesBadge() {
  const label = document.getElementById("refBadgeLabel");
  const pop = document.getElementById("refPop");
  try {
    const r = await fetch("/api/references/status");
    if (!r.ok) throw new Error("status " + r.status);
    const data = await r.json();
    const parts = [];
    if (data.koatuu) parts.push("КОАТУУ");
    if (data.tax_rates) parts.push("ставки");
    if (data.edrpou_checksum) parts.push("ЄДРПОУ");
    label.textContent = "довідники: " + parts.join(" · ");
    pop.replaceChildren(makeRefPop(data));
  } catch (e) {
    label.textContent = "довідники: недоступні";
    pop.replaceChildren(document.createTextNode("Не вдалося завантажити /api/references/status"));
  }
}

function makeRefPop(data) {
  const frag = document.createDocumentFragment();
  const h = document.createElement("h4");
  h.textContent = "Зовнішні довідники, з яких зараз працює детектор";
  frag.appendChild(h);
  frag.appendChild(refPopItem("КОАТУУ", data.koatuu, k => [
    `${k.oblasts} областей · ${k.rayons} районів`,
    `джерело: ${k.source}`,
    `завантажено з: ${k.loaded_from}${k.version ? ` · v${k.version}` : ""}`,
  ]));
  frag.appendChild(refPopItem("Податкові ставки", data.tax_rates, t => [
    `${t.source}`,
    `МЗП: ${t.minimum_wage_uah} ₴ · ставка землі: ${t.defaults?.land_tax_rate_pct_of_value ?? "?"} % від НГО`,
    `завантажено з: ${t.loaded_from}${t.version ? ` · v${t.version}` : ""}`,
  ]));
  frag.appendChild(refPopItem("ЄДРПОУ checksum", data.edrpou_checksum, e => [
    e.source,
    e.note,
  ]));
  return frag;
}

function refPopItem(title, entry, linesOf) {
  const el = document.createElement("div");
  el.className = "ref-pop-item";
  const b = document.createElement("b");
  b.textContent = title;
  el.appendChild(b);
  if (!entry) {
    const m = document.createElement("div");
    m.className = "muted";
    m.textContent = "недоступно";
    el.appendChild(m);
    return el;
  }
  for (const line of linesOf(entry)) {
    const d = document.createElement("div");
    d.className = "muted";
    d.textContent = line;
    el.appendChild(d);
  }
  return el;
}

form.addEventListener("submit", async e => {
  e.preventDefault();
  errorBox.hidden = true;
  resultBox.hidden = true;
  resultBox.replaceChildren();

  const land = document.getElementById("land").files[0];
  const re = document.getElementById("realestate").files[0];
  if (!land || !re) return;

  const fd = new FormData();
  fd.append("land", land);
  fd.append("realestate", re);

  btn.classList.add("loading");
  btn.disabled = true;
  btnLabel.textContent = "Обробка…";

  const t0 = performance.now();
  let objectUrl = null;
  try {
    const r = await fetch("/api/clean", { method: "POST", body: fd });
    if (!r.ok) {
      let detail = r.statusText;
      try { detail = (await r.json()).detail || detail; } catch (_) {}
      throw new Error(detail);
    }

    const stats = extractStats(r.headers);
    const blob = await r.blob();
    objectUrl = URL.createObjectURL(blob);
    const cd = r.headers.get("Content-Disposition") || "";
    const filename = /filename="([^"]+)"/.exec(cd)?.[1] || "otg_audit.xlsx";

    triggerDownload(objectUrl, filename);

    const elapsed = (performance.now() - t0) / 1000;
    renderResult(resultBox, { stats, elapsed, filename, objectUrl });
    resultBox.hidden = false;
    resultBox.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (err) {
    renderError(errorBox, err);
    errorBox.hidden = false;
    if (objectUrl) URL.revokeObjectURL(objectUrl);
  } finally {
    btn.classList.remove("loading");
    btn.disabled = false;
    btnLabel.textContent = "Очистити та отримати Excel";
  }
});

function extractStats(h) {
  return {
    land: +h.get("X-Stats-Land") || 0,
    re: +h.get("X-Stats-RealEstate") || 0,
    owners: +h.get("X-Stats-Owners") || 0,
    landChanged: +h.get("X-Stats-LandChanged") || 0,
    reChanged: +h.get("X-Stats-RealEstateChanged") || 0,
    findingsTotal: +h.get("X-Findings-Total") || 0,
    findingsCritical: +h.get("X-Findings-Critical") || 0,
    findingsHigh: +h.get("X-Findings-High") || 0,
    findingsMedium: +h.get("X-Findings-Medium") || 0,
    findingsLow: +h.get("X-Findings-Low") || 0,
    exposure: +h.get("X-Findings-Exposure") || 0,
  };
}

function triggerDownload(url, filename) {
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

function formatExposure(uah) {
  if (uah >= 1_000_000) {
    return { value: nfShort.format(uah / 1_000_000), unit: "млн ₴" };
  }
  if (uah >= 1_000) {
    return { value: nfShort.format(uah / 1_000), unit: "тис. ₴" };
  }
  return { value: nfInt.format(uah), unit: "₴" };
}

function svg(markup, cls) {
  const wrapper = document.createElement("span");
  wrapper.innerHTML = markup;
  const node = wrapper.firstElementChild;
  if (cls) node.setAttribute("class", cls);
  return node;
}

const ICONS = {
  check: `<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M5 13l4 4L19 7"/></svg>`,
  target: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor"/></svg>`,
  coins: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="9" cy="7" rx="6" ry="3"/><path d="M3 7v5c0 1.66 2.69 3 6 3M3 12v5c0 1.66 2.69 3 6 3"/><ellipse cx="15" cy="14" rx="6" ry="3"/><path d="M21 14v5c0 1.66-2.69 3-6 3"/></svg>`,
  users: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 00-4-4H7a4 4 0 00-4 4v2"/><circle cx="10" cy="7" r="4"/><path d="M22 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75"/></svg>`,
  download: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12m0 0l-4-4m4 4l4-4M5 21h14"/></svg>`,
};

function renderResult(root, { stats, elapsed, filename, objectUrl }) {
  const exposure = formatExposure(stats.exposure);
  const total = stats.findingsTotal || 1;
  const segs = [
    { key: "crit", count: stats.findingsCritical, label: "Критичні" },
    { key: "high", count: stats.findingsHigh, label: "Високі" },
    { key: "med",  count: stats.findingsMedium, label: "Середні" },
    { key: "low",  count: stats.findingsLow, label: "Низькі" },
  ];

  root.replaceChildren();

  const head = document.createElement("div");
  head.className = "result-head";
  const badge = document.createElement("div");
  badge.className = "result-badge";
  badge.appendChild(svg(ICONS.check));
  head.appendChild(badge);

  const title = document.createElement("div");
  title.className = "result-title";
  const h2 = document.createElement("h2");
  h2.textContent = "Готово — звіт завантажено";
  const p = document.createElement("p");
  p.textContent = `${filename} · опрацьовано за ${elapsed.toFixed(1)} с`;
  title.append(h2, p);
  head.appendChild(title);

  const actions = document.createElement("div");
  actions.className = "result-actions";
  const downloadAgain = document.createElement("button");
  downloadAgain.type = "button";
  downloadAgain.className = "btn-secondary";
  downloadAgain.appendChild(svg(ICONS.download));
  downloadAgain.append(" Завантажити знову");
  downloadAgain.addEventListener("click", () => triggerDownload(objectUrl, filename));
  actions.appendChild(downloadAgain);
  head.appendChild(actions);
  root.appendChild(head);

  // KPI grid
  const kpi = document.createElement("div");
  kpi.className = "kpi-grid";
  kpi.append(
    makeKpi({ icon: ICONS.target, label: "Розбіжностей знайдено", value: nfInt.format(stats.findingsTotal), hint: "у аркуші «Розбіжності»" }),
    makeKpi({ icon: ICONS.coins, label: "Річні втрати бюджету", value: exposure.value, unit: exposure.unit, hint: "проєкція за ставками ПКУ (ст. 266, 274)", accent: true }),
    makeKpi({ icon: ICONS.users, label: "Унікальних власників", value: nfInt.format(stats.owners), hint: "один рядок на податковий номер" }),
  );
  root.appendChild(kpi);

  // Severity bar
  const sev = document.createElement("div");
  sev.className = "severity";
  const sevHead = document.createElement("div");
  sevHead.className = "severity-head";
  const sevH3 = document.createElement("h3");
  sevH3.textContent = "Розподіл за рівнями серйозності";
  const sevTotal = document.createElement("span");
  sevTotal.className = "total";
  sevTotal.textContent = `усього ${nfInt.format(stats.findingsTotal)}`;
  sevHead.append(sevH3, sevTotal);
  sev.appendChild(sevHead);

  const bar = document.createElement("div");
  bar.className = "severity-bar";
  bar.setAttribute("role", "img");
  bar.setAttribute("aria-label",
    `Критичних ${stats.findingsCritical}, високих ${stats.findingsHigh}, середніх ${stats.findingsMedium}, низьких ${stats.findingsLow}`);
  segs.forEach(s => {
    if (!s.count) return;
    const el = document.createElement("div");
    el.className = `severity-seg ${s.key}`;
    el.style.width = "0%";
    bar.appendChild(el);
    requestAnimationFrame(() => {
      el.style.width = `${(s.count / total) * 100}%`;
    });
  });
  sev.appendChild(bar);

  const legend = document.createElement("div");
  legend.className = "severity-legend";
  segs.forEach(s => legend.appendChild(makeLegend(s)));
  sev.appendChild(legend);
  root.appendChild(sev);

  // Processing footer
  const proc = document.createElement("div");
  proc.className = "processing";
  proc.append(
    makeProc("Земельні ділянки", nfInt.format(stats.land), `з них нормалізовано ${nfInt.format(stats.landChanged)}`),
    makeProc("Нерухомість", nfInt.format(stats.re), `з них нормалізовано ${nfInt.format(stats.reChanged)}`),
    makeProc("Час обробки", `${elapsed.toFixed(1)} с`, "повний цикл"),
  );
  root.appendChild(proc);
}

function makeKpi({ icon, label, value, unit, hint, accent }) {
  const card = document.createElement("div");
  card.className = "kpi" + (accent ? " accent" : "");

  const l = document.createElement("div");
  l.className = "kpi-label";
  l.appendChild(svg(icon));
  l.append(document.createTextNode(label));
  card.appendChild(l);

  const n = document.createElement("div");
  n.className = "kpi-num";
  n.textContent = value;
  if (unit) {
    const u = document.createElement("span");
    u.className = "unit";
    u.textContent = " " + unit;
    n.appendChild(u);
  }
  card.appendChild(n);

  if (hint) {
    const h = document.createElement("div");
    h.className = "kpi-hint";
    h.textContent = hint;
    card.appendChild(h);
  }
  return card;
}

function makeLegend({ key, count, label }) {
  const item = document.createElement("div");
  item.className = "severity-legend-item";
  const dot = document.createElement("span");
  dot.className = `severity-dot ${key}`;
  const body = document.createElement("div");
  body.className = "severity-legend-body";
  const name = document.createElement("span");
  name.className = "severity-legend-name";
  name.textContent = label;
  const num = document.createElement("span");
  num.className = "severity-legend-num";
  num.textContent = nfInt.format(count);
  body.append(name, num);
  item.append(dot, body);
  return item;
}

function makeProc(label, value, delta) {
  const wrap = document.createElement("div");
  wrap.className = "processing-item";
  const l = document.createElement("div");
  l.className = "processing-label";
  l.textContent = label;
  const v = document.createElement("div");
  v.className = "processing-value";
  v.textContent = value;
  if (delta) {
    const d = document.createElement("span");
    d.className = "delta";
    d.textContent = `· ${delta}`;
    v.appendChild(d);
  }
  wrap.append(l, v);
  return wrap;
}

function renderError(root, err) {
  root.replaceChildren();
  const b = document.createElement("b");
  b.textContent = "Не вдалося обробити файли";
  const p = document.createElement("div");
  p.textContent = err?.message || String(err);
  root.append(b, p);
}
