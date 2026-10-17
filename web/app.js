// Presenter dashboard. Reads one view per second from the API and renders the act the
// presenter is on. Routes: #act1 #act2 #close, plus a drawer tab: #act1/backlog,
// #act1/completions, #act1/tenants, #act1/customer.

const COLORS = { acme: "#7f86f1", birch: "#4fd8ff", cedar: "#ff6bff", dune: "#c3ff62" };
const TABS = ["backlog", "completions", "tenants", "customer"];
const POLL_MS = 1000;

const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];

let settings = null;   // GET /api/settings
let view = null;       // GET /api/runs/{run}/view
let viewAt = 0;        // when the last view arrived (ms)
let failing = false;
let act = "act1", tab = null;

// ---------- Per-tab run state (sessionStorage is already scoped to this tab) ----------
const store = {
  get(k, d) { try { return JSON.parse(sessionStorage.getItem(`fairness:${k}`)) ?? d; } catch { return d; } },
  set(k, v) { sessionStorage.setItem(`fairness:${k}`, JSON.stringify(v)); },
};
const newRunId = () => [...crypto.getRandomValues(new Uint8Array(4))].map(b => b.toString(16).padStart(2, "0")).join("");
const returning = store.get("run", null) !== null;
let run = store.get("run", null) || newRunId();
let runInfo = store.get("runInfo", { kind: "act1", done: [] });
let slotsWanted = null;
store.set("run", run);
const saveRun = () => { store.set("run", run); store.set("runInfo", runInfo); };

// ---------- Formatting ----------
const T = id => settings.tenants.find(t => t.id === id);
const num = n => Number(n || 0).toLocaleString("en-US");
function dur(s) {
  if (s == null) return "—";
  if (s < 59.5) return `${Math.max(0, Math.round(s))} s`;
  const m = Math.floor(Math.round(s) / 60), r = Math.round(s) % 60;
  return `${m} m ${String(r).padStart(2, "0")} s`;
}
const clock = s => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, "0")}`;
const tenantCell = id => `<span class="tenant" style="--c:${COLORS[id]}"><i class="sw" aria-hidden="true"></i><span class="name">${T(id).name}</span></span>`;
const sideBadge = side => side === "off" ? `<span class="badge idle">Fairness off</span>` : `<span class="pill">Fairness on</span>`;
const hasWork = side => side && Object.values(side.tenants).some(t => t.total);
const weights = () => Object.fromEntries($$(".weights-edit input").map(i => [i.dataset.plan, Math.min(10, Math.max(1, Number(i.value) || 1))]));
const tenantWeight = id => runInfo.kind === "act2" ? (runInfo.weights?.[T(id).plan] ?? T(id).weight) : T(id).weight;

// ---------- Phase, derived from live data ----------
function phase() {
  if (act !== "act1") return act;
  const off = view?.sides.off;
  if (off?.small.total) return "trickle";
  if (hasWork(off) || runInfo.done.includes("bulk")) return "bulk";
  return "ready";
}

// ---------- Main screen ----------
function renderHeadline(p) {
  const slots = settings.worker_slots;
  const eyebrow = $("#eyebrow"), h = $("#headline"), sub = $("#headline-sub");
  if (act === "act2") {
    eyebrow.textContent = `Act 2 · Paid tiers · Fairness on · ${slots} worker slots`;
    const w = runInfo.weights || weights();
    const ratio = w.Enterprise / w.Starter;
    h.innerHTML = `When every tenant is busy, Enterprise gets <span class="text-gradient-green">${ratio.toLocaleString("en-US", { maximumFractionDigits: 1 })}×</span> the Starter share.`;
    sub.textContent = `Each run starts with its tenant's plan weight: Enterprise ${w.Enterprise}, Pro ${w.Pro}, Starter ${w.Starter}`;
    return;
  }
  eyebrow.textContent = `Act 1 · Noisy neighbor · Same ${slots} worker slots on both sides`;
  if (p === "ready") {
    h.innerHTML = `One big upload, three small customers, and the same ${slots} worker slots on each side.`;
    sub.textContent = `Press Start bulk upload to send Acme's ${num(settings.acme_bulk_docs)} documents to both sides`;
  } else if (p === "bulk") {
    h.innerHTML = `Acme uploaded <span class="text-gradient-green">${num(settings.acme_bulk_docs)} documents</span>. Both sides now have a backlog.`;
    sub.textContent = "Small tenants haven't uploaded yet · press Start small-tenant trickle";
  } else {
    const off = view.sides.off.small.p95_wait_s, on = view.sides.on.small.p95_wait_s;
    h.innerHTML = on <= off
      ? `With fairness, small tenants wait <span class="text-gradient-green">${dur(on)}</span> instead of <span class="muted-number">${dur(off)}</span>.`
      : `Small tenants wait ${dur(on)} with fairness and ${dur(off)} without.`;
    sub.textContent = "p95 wait from upload to processing start for Birchwood, Cedar, and Dune, including documents still waiting";
  }
}

function laneState(id, t) {
  if (!t.total) return ["No uploads", "idle"];
  if (t.waiting) return [`${num(t.waiting)} waiting`, "waiting"];
  if (t.done === t.total) return ["✓ All done", "good"];
  return settings.small_tenants.includes(id) ? ["✓ Keeping up", "good"] : [`${num(t.running)} in progress`, "busy"];
}

function renderAct1() {
  for (const sideName of ["off", "on"]) {
    const side = view?.sides[sideName], el = $(`.side[data-side="${sideName}"]`);
    const small = side?.small;
    let value = "—", note = "No small-tenant uploads yet", cls = "";
    if (small?.total) {
      value = dur(small.p95_wait_s);
      if (small.waiting) { note = `${num(small.waiting)} small-tenant document${small.waiting === 1 ? "" : "s"} waiting behind Acme`; cls = "waiting"; }
      else { note = sideName === "on" ? "Small-tenant documents start as they arrive" : "No small-tenant documents waiting now"; cls = "good"; }
    }
    $('[data-slot="wait"]', el).innerHTML =
      `<span class="big-label">Small-tenant wait · p95</span><span class="big-value">${value}</span><span class="big-note ${cls}">${note}</span>`;
    $('[data-slot="lanes"]', el).innerHTML = settings.tenants.map(({ id }) => {
      const t = side?.tenants[id] || { total: 0, done: 0, running: 0, waiting: 0 };
      const pct = v => (t.total ? (v / t.total) * 100 : 0);
      const [state, scls] = laneState(id, t);
      return `<div class="lane" role="listitem" style="--c:${COLORS[id]}">
        ${tenantCell(id)}
        <span class="bar" role="img" aria-label="${num(t.done)} of ${num(t.total)} done"><span class="done" style="width:${pct(t.done)}%"></span><span class="run" style="width:${pct(t.running)}%"></span><span class="wait" style="width:${pct(t.waiting)}%"></span></span>
        <span class="state ${scls}">${state}</span>
      </div>`;
    }).join("");
  }
}

function act2Shares() {
  const side = view?.sides.on, w = runInfo.weights || weights();
  const recent = side?.recent_starts || {}, total = Object.values(recent).reduce((a, b) => a + b, 0);
  const busy = settings.tenants.filter(t => side?.tenants[t.id]?.waiting);
  const pool = busy.length ? busy : settings.tenants;
  const wsum = pool.reduce((a, t) => a + w[t.plan], 0);
  return Object.fromEntries(settings.tenants.map(t => [t.id, {
    expected: pool.includes(t) ? (w[t.plan] / wsum) * 100 : 0,
    actual: total ? (recent[t.id] / total) * 100 : 0,
    perMin: recent[t.id] || 0,
    weight: w[t.plan],
  }]));
}

function renderAct2() {
  const started = runInfo.kind === "act2" && runInfo.done.includes("tiered");
  const shares = act2Shares();
  $("#act2-scope").textContent = started
    ? "Fairness on · documents started in the last 60 s · weights set when each run starts"
    : `Press Start tiered uploads: every tenant uploads ${num(settings.tiered_docs_per_tenant)} documents at once`;
  $("#weight-rows").innerHTML = settings.tenants.map(t => {
    const s = shares[t.id];
    return `<div class="wrow" style="--c:${COLORS[t.id]}">
      <span class="tenant">${tenantCell(t.id)}<span class="tier">${t.plan}</span></span>
      <span class="weight">×${s.weight}<small>plan weight</small></span>
      <div class="wbars">
        <div class="wbar expected"><span class="track"><span class="fill" style="width:${s.expected}%"></span></span><span class="val">expected ${s.expected.toFixed(1)}%</span></div>
        <div class="wbar actual"><span class="track"><span class="fill" style="width:${started ? s.actual : 0}%"></span></span><span class="val">${started ? `actual ${s.actual.toFixed(1)}% · ${s.perMin} started/min` : "actual —"}</span></div>
      </div>
    </div>`;
  }).join("");
}

// ---------- Wrap-up, from the summaries this tab saved ----------
function renderClose() {
  const a1 = store.get("act1Summary", null), a2 = store.get("act2Summary", null);
  $("#close-eyebrow").textContent = `What this run showed · same ${a1?.slots ?? settings.worker_slots} worker slots per side`;
  $("#close-title").innerHTML = a1?.smallOff != null
    ? `Small tenants' p95 wait fell from ${dur(a1.smallOff)} to <span class="text-gradient-green">${dur(a1.smallOn)}</span>.`
    : "Run act 1 to compare small tenants' wait with and without fairness.";
  if (a1?.acmeOff != null && a1?.acmeOn != null) {
    const diff = a1.acmeOn - a1.acmeOff;
    $("#fact-acme").textContent = `${diff >= 0 ? "+" : "−"}${dur(Math.abs(diff))}`;
    $("#fact-acme-note").textContent = `${num(a1.acmeDocs)} documents in ${dur(a1.acmeOn)} with fairness, ${dur(a1.acmeOff)} without.`;
  }
  if (a2) {
    $("#fact-weights").textContent = `${a2.weights.Enterprise} / ${a2.weights.Pro} / ${a2.weights.Starter}`;
    $("#fact-weights-note").textContent = `Enterprise, Pro, Starter, from each tenant's plan. Enterprise got ${a2.actual.toFixed(0)}% of document starts; ${a2.expected.toFixed(0)}% expected.`;
  }
}

function saveSummaries() {
  if (!view) return;
  if (runInfo.kind === "act1" && view.sides.off.small.total) {
    const off = view.sides.off, on = view.sides.on;
    store.set("act1Summary", {
      smallOff: off.small.p95_wait_s, smallOn: on.small.p95_wait_s,
      acmeOff: off.tenants.acme.done === off.tenants.acme.total ? off.tenants.acme.last_finish_s : null,
      acmeOn: on.tenants.acme.done === on.tenants.acme.total ? on.tenants.acme.last_finish_s : null,
      acmeDocs: off.tenants.acme.total, slots: settings.worker_slots,
    });
  }
  if (runInfo.kind === "act2" && runInfo.done.includes("tiered")) {
    const s = act2Shares().acme;
    if (Object.values(view.sides.on.recent_starts).some(Boolean)) store.set("act2Summary", { weights: runInfo.weights, actual: s.actual, expected: s.expected });
  }
}

// ---------- Drawer ----------
function cells(id, n, cls = "") { return Array.from({ length: n }, () => `<i class="${cls}" style="--c:${COLORS[id]}"></i>`).join(""); }

function backlogTab() {
  if (act === "act2") {
    return `<section class="dsec"><div class="dsec-head"><h3>Documents waiting, by tenant</h3><span class="mono">Fairness on · every tenant uploaded ${num(settings.tiered_docs_per_tenant)} at 0:00 · fixed scale</span></div>
      <div class="pane"><div class="chart-box tall"><canvas data-chart="a2-backlog"></canvas></div>
      <p class="note">Each tenant's backlog drains at its weighted rate. Acme's drains fastest; Starter tenants still move.</p></div></section>`;
  }
  if (!view?.documents) return `<p class="note">Nothing is waiting yet. Start the bulk upload.</p>`;
  const off = view.sides.off, on = view.sides.on;
  const progress = side => settings.tenants.filter(t => side.tenants[t.id].running)
    .map(t => `${T(t.id).name.split(" ")[0]} ${num(side.tenants[t.id].running)}`).join(" · ") || "nothing";
  const head = off.line.head, rest = off.line.total - head.length;
  const behind = off.line.small_positions.filter(p => p.position > head.length);
  const offLine = head.map(id => `<i class="${id === "acme" ? "" : "small"}" style="--c:${COLORS[id]}"></i>`).join("")
    + (rest > behind.length ? `<span class="gap">… ${num(rest - behind.length)} more Acme …</span>` : "")
    + behind.map(p => `<i class="small" style="--c:${COLORS[p.tenant]}"></i>`).join("");
  const onLines = settings.tenants.map(t => {
    const n = on.tenants[t.id].waiting;
    const body = n ? cells(t.id, Math.min(n, 60)) + (n > 60 ? `<span class="gap">+${num(n - 60)}</span>` : "")
      : `<span class="empty">${on.tenants[t.id].total ? "Empty · the next document starts on the next turn" : "No uploads"}</span>`;
    return `<div class="vq">${tenantCell(t.id)}<div class="cells">${body}</div></div>`;
  }).join("");
  const cedar = off.line.small_positions.find(p => p.tenant === "cedar");
  const smallOff = off.small.waiting;
  return `
    <section class="dsec">
      <div class="dsec-head"><h3>Who is in line right now</h3><span class="mono">One square per document waiting to start · outlined squares belong to small tenants</span></div>
      <div class="pair">
        <div class="pane">
          <div class="pane-title">${sideBadge("off")}<span class="mono">${num(off.line.total)} waiting in one line</span></div>
          <span class="q-label">Started, not finished: ${progress(off)}</span>
          <span class="q-label">Line, front to back</span><div class="cells">${offLine || `<span class="empty">Nobody waiting</span>`}</div>
          <p class="note">${smallOff ? `<span class="y">${cedar ? `Cedar's oldest waiting document is #${num(cedar.position)} in line.` : `${num(smallOff)} small-tenant documents are behind Acme.`}</span> Acme's flood fills the front of the line.` : off.line.total ? "Acme's flood fills the whole line." : "Nobody is waiting."}</p>
        </div>
        <div class="pane">
          <div class="pane-title">${sideBadge("on")}<span class="mono">One virtual line per fairness key</span></div>
          <span class="q-label">Started, not finished: ${progress(on)}</span>
          <span class="q-label">Lines by fairness key, taken in turns</span>${onLines}
          <p class="note">${on.small.total ? (on.small.waiting ? `<span class="y">${num(on.small.waiting)} small-tenant documents waiting for their turn.</span>` : `<span class="g">Small tenants' lines stay empty</span>: each new document gets the next turn.`) : "Only Acme has work, so Acme gets every turn."}</p>
        </div>
      </div>
    </section>
    <section class="dsec">
      <div class="dsec-head"><h3>Documents waiting over time</h3><span class="mono">Fixed 0–${num(backlogMax())} scale on both sides</span></div>
      <div class="pair">
        <div class="pane"><div class="pane-title">${sideBadge("off")}</div><div class="chart-box"><canvas data-chart="backlog-off"></canvas></div></div>
        <div class="pane"><div class="pane-title">${sideBadge("on")}</div><div class="chart-box"><canvas data-chart="backlog-on"></canvas></div></div>
      </div>
    </section>
    <section class="dsec">
      <div class="dsec-head"><h3>Small-tenant documents waiting</h3><span class="mono">Birchwood, Cedar, Dune · fixed 0–${smallMax()} scale</span></div>
      <div class="pair">
        <div class="pane"><div class="chart-box"><canvas data-chart="small-off"></canvas></div></div>
        <div class="pane"><div class="chart-box"><canvas data-chart="small-on"></canvas></div></div>
      </div>
    </section>`;
}

function completionsTab() {
  const rate = Math.round((settings.worker_slots / perDocSeconds()) * 60);
  if (act === "act2") {
    return `<section class="dsec"><div class="dsec-head"><h3>Completions over time · fairness on</h3><span class="mono">Documents completed per ${view?.bucket_s ?? 10} s · plan weights</span></div>
      <div class="pane"><div class="chart-box tall"><canvas data-chart="a2-completions"></canvas></div>
      <p class="note">With fairness off and the same uploads, Acme would get every turn until its documents were done.</p></div></section>`;
  }
  return `<section class="dsec"><div class="dsec-head"><h3>Completions over time, stacked by tenant</h3><span class="mono">Documents completed per ${view?.bucket_s ?? 10} s · same scale on both sides</span></div>
    <div class="pane"><div class="pane-title">${sideBadge("off")}</div><div class="chart-box"><canvas data-chart="comp-off"></canvas></div></div>
    <div class="pane"><div class="pane-title">${sideBadge("on")}</div><div class="chart-box"><canvas data-chart="comp-on"></canvas></div></div>
    <p class="note">Each side can finish about ${num(rate)} documents a minute with ${settings.worker_slots} slots. Fairness changes who gets the slots, not how many there are. A document's later steps queue again behind earlier work, so completions start after the first steps drain.</p></section>`;
}

function tenantsTab() {
  if (!view?.documents) return `<p class="note">No documents yet.</p>`;
  const sides = act === "act2" ? ["on"] : ["off", "on"];
  const table = side => {
    const s = view.sides[side];
    return `<div class="pane"><div class="pane-title">${sideBadge(side)}<span class="mono">Task Queue ${settings.task_queues[side]}</span></div>
    <table class="ttable"><thead><tr><th>Tenant</th><th>Plan</th><th>Fairness key</th><th>Weight</th><th>Done</th><th>Waiting</th><th>Running</th><th>p95 wait</th></tr></thead>
    <tbody>${settings.tenants.map(t => { const r = s.tenants[t.id]; return `<tr><td>${tenantCell(t.id)}</td><td>${t.plan}</td>
      <td>${side === "off" ? "none" : t.fairness_key}</td><td>${side === "off" ? "—" : tenantWeight(t.id)}</td>
      <td>${r.total ? `${num(r.done)} / ${num(r.total)}` : "—"}</td>
      <td class="${r.waiting ? "waiting" : ""}">${num(r.waiting)}</td><td>${num(r.running)}</td><td>${dur(r.p95_wait_s)}</td></tr>`; }).join("")}</tbody></table></div>`;
  };
  return `<section class="dsec"><div class="dsec-head"><h3>Per-tenant numbers</h3><span class="mono">Wait = upload to processing start · p95 includes documents still waiting</span></div>
    ${sides.map(table).join("")}
    <p class="note">The agents never set a fairness key. The upload starts each document's agent run with the tenant's fairness key and plan weight, and every Workflow Task and Activity of that run inherits them. Both Task Queues are named from one TASK_QUEUE setting. Processing start is recorded when the model call returns, so a document can show as waiting for up to a model call after it starts; its wait is still measured from the true start time.</p></section>`;
}

function customerTab() {
  const portal = side => {
    const docs = view?.sides[side].cedar || [];
    const ready = docs.filter(d => d.state === "done").length;
    const state = d => d.state === "done" ? [`✓ Ready · took ${dur(d.took_s)}`, "good"]
      : d.state === "running" ? ["◔ Processing", "busy"]
      : d.state === "waiting" ? [`◷ Queued · ${dur(d.wait_s)}`, "waiting"] : ["Stopped", "idle"];
    return `<div class="pane"><div class="pane-title">${sideBadge(side)}</div>
    <div class="portal"><div class="portal-bar"><i class="sw" aria-hidden="true"></i><strong>Cedar Health · Document inbox</strong><span class="mono muted">Starter plan</span></div>
    ${docs.length ? `<ul class="portal-list">${docs.map(d => { const [st, cls] = state(d); return `<li><span class="doc">${d.file}</span><span class="up">Uploaded ${clock(d.uploaded_s)}</span><span class="state ${cls}">${st}</span></li>`; }).join("")}</ul>
      <div class="portal-foot">${ready} of ${docs.length} ready</div>` : `<div class="portal-foot mono muted">Cedar hasn't uploaded anything yet</div>`}</div></div>`;
  };
  return `<section class="dsec"><div class="dsec-head"><h3>What Cedar Health's staff see</h3><span class="mono">Simulated customer portal · same data as the lanes</span></div>
    <div class="pair">${portal("off")}${portal("on")}</div></section>`;
}

let drawerKey = "";
function renderDrawer() {
  const drawer = $("#drawer"), open = !!tab && act !== "close";
  drawer.hidden = !open; $("#scrim").hidden = !open;
  $("#btn-details").setAttribute("aria-expanded", String(open));
  if (!open) { drawerKey = ""; return; }
  if (act === "act2" && tab === "customer") tab = "backlog";
  $$(".drawer-tabs [data-tab]").forEach(b => b.setAttribute("aria-selected", String(b.dataset.tab === tab)));
  $("#drawer-title").textContent = { backlog: "Backlog: who is waiting", completions: "Completions over time", tenants: "Tenant table", customer: "Customer's-eye view" }[tab];
  $("#drawer-eyebrow").textContent = `Details · ${act === "act2" ? "Act 2" : "Act 1"} · same run as the main screen`;
  const html = { backlog: backlogTab, completions: completionsTab, tenants: tenantsTab, customer: customerTab }[tab]();
  // Replace the body only when its markup changed, so scroll position and focus survive a refresh.
  const key = html.replace(/<canvas[^>]*><\/canvas>/g, "");
  if (key !== drawerKey) { $("#drawer-body").innerHTML = html; drawerKey = key; }
  requestAnimationFrame(drawCharts);
}

// ---------- Charts: fixed scales, direct labels, a "now" marker ----------
const perDocSeconds = () => 3;
const backlogMax = () => act === "act2" ? niceMax(settings.tiered_docs_per_tenant) : niceMax(settings.acme_bulk_docs);
const smallMax = () => niceMax(Object.values(settings.trickle_docs).reduce((a, b) => a + b, 0), 4);
const compMax = () => niceMax(((settings.worker_slots * (view?.bucket_s ?? 10)) / perDocSeconds()) * 1.5);
function niceMax(v, step) {
  if (step) return Math.max(step, Math.ceil(v / step) * step);
  const p = 10 ** Math.floor(Math.log10(Math.max(1, v))), m = v / p;
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10) * p;
}
const windowS = () => Math.max(300, Math.ceil((view?.elapsed_s || 0) / 300) * 300);

function drawChart(canvas, series, { yMax, unit, centered = true, only } = {}) {
  if (!canvas) return;
  const wrap = canvas.parentElement, dpr = window.devicePixelRatio || 1;
  const w = wrap.clientWidth, h = wrap.clientHeight;
  if (!w || !h) return;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  const rem = parseFloat(getComputedStyle(document.documentElement).fontSize);
  ctx.font = `500 ${Math.round(rem * .64)}px "Noto Sans Mono", monospace`;
  const pad = { l: rem * 2.8, r: rem * 5.6, t: rem * 1.1, b: rem * 1.4 };
  const pw = w - pad.l - pad.r, ph = h - pad.t - pad.b, win = windowS(), bucket = view?.bucket_s ?? 10;
  const x = s => pad.l + (Math.min(s, win) / win) * pw, y = v => pad.t + ph - (Math.min(v, yMax) / yMax) * ph;
  for (const v of [0, yMax / 2, yMax]) {
    ctx.strokeStyle = v === 0 ? "rgba(97,105,237,.45)" : "rgba(97,105,237,.16)"; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.l, y(v)); ctx.lineTo(pad.l + pw, y(v)); ctx.stroke();
    ctx.fillStyle = "#92a4c3"; ctx.textAlign = "right"; ctx.textBaseline = "middle";
    ctx.fillText(v.toLocaleString("en-US"), pad.l - rem * .35, y(v));
  }
  ctx.textAlign = "center"; ctx.textBaseline = "top";
  const tick = win > 600 ? 300 : 60;
  for (let s = 0; s <= win; s += tick) ctx.fillText(`${s / 60}:00`, x(s), pad.t + ph + rem * .3);
  ctx.textAlign = "left"; ctx.fillText(unit, pad.l, 0);
  if (!series?.length || !series.some(b => Object.values(b).some(Boolean))) {
    ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.fillText("Nothing yet", pad.l + pw / 2, pad.t + ph / 2);
    return;
  }
  const tNow = view.elapsed_s;
  const pts = series.map((b, i) => ({ s: Math.min(tNow, centered ? i * bucket + bucket / 2 : i * bucket), b }));
  let base = pts.map(() => 0);
  const labels = [];
  for (const t of settings.tenants) {
    if (only && !only.includes(t.id)) continue;
    if (Math.max(...pts.map(p => p.b[t.id] || 0)) < .01) continue;
    const top = pts.map((p, i) => base[i] + (p.b[t.id] || 0));
    ctx.beginPath();
    pts.forEach((p, i) => (i ? ctx.lineTo(x(p.s), y(top[i])) : ctx.moveTo(x(p.s), y(top[i]))));
    for (let i = pts.length - 1; i >= 0; i--) ctx.lineTo(x(pts[i].s), y(base[i]));
    ctx.closePath(); ctx.fillStyle = hexA(COLORS[t.id], .5); ctx.fill();
    ctx.beginPath();
    pts.forEach((p, i) => (i ? ctx.lineTo(x(p.s), y(top[i])) : ctx.moveTo(x(p.s), y(top[i]))));
    ctx.strokeStyle = COLORS[t.id]; ctx.lineWidth = Math.max(1.5, rem * .12); ctx.stroke();
    const last = pts.length - 1;
    labels.push({ name: t.name.split(" ")[0], color: COLORS[t.id], yv: (base[last] + top[last]) / 2 });
    base = top;
  }
  const nowX = x(tNow);
  ctx.strokeStyle = "rgba(202,203,249,.35)"; ctx.setLineDash([3, 4]); ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(nowX, pad.t); ctx.lineTo(nowX, pad.t + ph); ctx.stroke(); ctx.setLineDash([]);
  labels.sort((a, b) => b.yv - a.yv);
  let prev = -Infinity;
  for (const l of labels) {
    let ly = y(l.yv); if (ly - prev < rem * .9) ly = prev + rem * .9; prev = ly;
    ctx.fillStyle = l.color; ctx.textAlign = "left"; ctx.textBaseline = "middle"; ctx.fillText(l.name, nowX + rem * .4, ly);
  }
}
function hexA(hex, a) { const n = parseInt(hex.slice(1), 16); return `rgba(${n >> 16}, ${(n >> 8) & 255}, ${n & 255}, ${a})`; }

function drawCharts() {
  if (!tab || act === "close" || !view) return;
  const c = n => $(`canvas[data-chart="${n}"]`), s = view.sides;
  const comp = { yMax: compMax(), unit: `docs / ${view.bucket_s} s` };
  const bl = { yMax: backlogMax(), unit: "docs waiting", centered: false };
  const sm = { yMax: smallMax(), unit: "small-tenant docs waiting", centered: false, only: settings.small_tenants };
  drawChart(c("comp-off"), s.off.completions, comp); drawChart(c("comp-on"), s.on.completions, comp);
  drawChart(c("backlog-off"), s.off.backlog, bl); drawChart(c("backlog-on"), s.on.backlog, bl);
  drawChart(c("small-off"), s.off.backlog, sm); drawChart(c("small-on"), s.on.backlog, sm);
  drawChart(c("a2-completions"), s.on.completions, comp);
  drawChart(c("a2-backlog"), s.on.backlog, bl);
}

// ---------- Render everything ----------
function render() {
  if (!settings) return;
  const p = phase();
  document.body.dataset.view = act;
  document.body.dataset.state = p;
  $$(".act").forEach(b => { if (b.dataset.act === act) b.setAttribute("aria-current", "step"); else b.removeAttribute("aria-current"); });
  renderHeadline(p);
  $("#btn-bulk").classList.toggle("is-done", runInfo.kind === "act1" && runInfo.done.includes("bulk"));
  $("#btn-trickle").classList.toggle("is-done", runInfo.kind === "act1" && runInfo.done.includes("trickle"));
  $("#btn-tiered").classList.toggle("is-done", runInfo.kind === "act2" && runInfo.done.includes("tiered"));
  if (act === "act1") renderAct1();
  if (act === "act2") renderAct2();
  if (act === "close") renderClose();
  renderDrawer();
  saveSummaries();
  if (window.Starfield) Starfield.setLoad({ ready: 0, bulk: .55, trickle: .7, act2: runInfo.done.includes("tiered") ? .6 : .1, close: .1 }[p] ?? 0);
}

function renderFreshness() {
  const fresh = $("#freshness"), label = fresh.lastElementChild;
  const age = viewAt ? Math.max(0, Math.round((Date.now() - viewAt) / 1000)) : null;
  const running = view && Object.values(view.sides).some(s => Object.values(s.tenants).some(t => t.waiting || t.running));
  if (failing) { fresh.dataset.state = "stale"; label.textContent = age == null ? "Reconnecting…" : `Reconnecting · last update ${age} s ago`; }
  else if (!view) { fresh.dataset.state = "idle"; label.textContent = "Connecting…"; }
  else if (!running) { fresh.dataset.state = "idle"; label.textContent = view.documents ? `Idle · updated ${age} s ago` : "Idle · nothing running"; }
  else { fresh.dataset.state = "live"; label.textContent = `Updated ${age} s ago`; }
}

// ---------- Data ----------
const api = (path, opts = {}) => fetch(path, { credentials: "same-origin", headers: { "content-type": "application/json" }, ...opts })
  .then(r => (r.ok ? r.json() : r.text().then(t => Promise.reject(new Error(t || r.statusText)))));

let polling = null;
async function poll() {
  if (document.visibilityState !== "visible") return;
  const forRun = run;
  polling?.abort(); polling = new AbortController();
  try {
    const v = await api(`/api/runs/${forRun}/view`, { signal: polling.signal });
    if (forRun !== run) return;
    view = v; viewAt = Date.now(); failing = false;
    const ui = `${settings.temporal_ui}/namespaces/${encodeURIComponent(settings.namespace)}/workflows?query=${encodeURIComponent(v.temporal_query)}`;
    $$(".temporal-ui-link").forEach(a => (a.href = ui));
    render();
  } catch (e) {
    if (e.name !== "AbortError") failing = true;
  }
  renderFreshness();
}

// ---------- Actions: acknowledged at once, outcome shown when the server answers ----------
function ack(text, done = false) {
  const el = $("#ack");
  el.className = "ack mono" + (done ? " done" : "");
  el.innerHTML = text ? `<span class="dot" aria-hidden="true"></span>${text}` : "";
}

async function action(btn, name, pending, path, body, outcome) {
  if (btn.disabled || runInfo.done.includes(name)) return;
  btn.disabled = true;
  ack(pending);
  try {
    const r = await api(path, { method: "POST", body: JSON.stringify(body || {}) });
    runInfo.done.push(name); saveRun();
    ack(outcome(r), true);
  } catch (e) {
    ack(`Couldn't start: ${e.message}. Nothing changed; try again.`);
  } finally {
    btn.disabled = false;
    render();
  }
}

function startBulk() {
  if (act !== "act1") return;
  if (runInfo.kind !== "act1") return resetRun("act1").then(startBulk);
  action($("#btn-bulk"), "bulk", `Starting ${num(settings.acme_bulk_docs)} document runs on each side…`, `/api/runs/${run}/bulk`, null,
    r => `Bulk upload accepted · ${num(r.documents_per_side)} Acme document runs starting on each side`);
}
function startTrickle() {
  if (act !== "act1" || runInfo.kind !== "act1") return;
  const n = Object.values(settings.trickle_docs).reduce((a, b) => a + b, 0);
  const every = Object.values(settings.trickle_interval_s);
  action($("#btn-trickle"), "trickle", "Starting the small-tenant trickle…", `/api/runs/${run}/trickle`, null,
    () => `Trickle running · ${n} small-tenant documents, one every ${Math.min(...every)}–${Math.max(...every)} s per tenant`);
}
async function startTiered() {
  if (act !== "act2") return;
  if (runInfo.kind !== "act2" || runInfo.done.length) await resetRun("act2");
  runInfo.weights = weights(); saveRun();
  action($("#btn-tiered"), "tiered", `Starting ${num(settings.tiered_docs_per_tenant)} documents per tenant…`, `/api/runs/${run}/tiered`, { weights: runInfo.weights },
    r => `Tiered uploads accepted · ${num(r.documents_per_tenant)} documents per tenant`);
}

// Reset switches the screen to a fresh run at once; the old run's Workflows are terminated
// in the background, and a changed slot count is applied to the Workers.
async function resetRun(kind = act === "act2" ? "act2" : "act1") {
  const old = run;
  run = newRunId();
  runInfo = { kind, done: [] };
  saveRun();
  view = null; viewAt = 0;
  const slots = slotsWanted;
  slotsWanted = null;
  ack(slots ? `Reset · stopping the previous run and changing to ${slots} worker slots…` : "Reset · stopping the previous run…");
  render();
  try {
    const r = await api(`/api/runs/${old}/reset`, { method: "POST", body: JSON.stringify({ next_run: run, worker_slots: slots }) });
    if (r.worker_slots) settings.worker_slots = r.worker_slots;
    ack(slots ? `Reset done · ${r.worker_slots} worker slots per side` : "Reset done · ready for a new run", true);
  } catch (e) {
    ack(`Reset started a new run, but stopping the old one failed: ${e.message}`);
  }
  updateSlots();
  poll();
}

function updateSlots() {
  $("#slots").textContent = slotsWanted ?? settings.worker_slots;
  $("#slots-note").textContent = slotsWanted && slotsWanted !== settings.worker_slots
    ? `Now ${settings.worker_slots} · changes to ${slotsWanted} on Reset` : "Same on both sides · applies after Reset";
}

// ---------- Navigation ----------
function go(hash) {
  const [a, t] = hash.replace(/^#/, "").split("/");
  act = ["act1", "act2", "close"].includes(a) ? a : "act1";
  tab = TABS.includes(t) ? t : null;
  render();
}
const routeFor = (a, t = tab) => (t ? `${a}/${t}` : a);
function setAct(a) { location.hash = routeFor(a); }
function setTab(t) {
  location.hash = routeFor(act, t);
  if (t) requestAnimationFrame(() => $("#drawer-close").focus({ preventScroll: true }));
}

document.addEventListener("click", e => {
  const tb = e.target.closest("[data-tab]");
  if (tb) return setTab(tb.dataset.tab);
  const a = e.target.closest("[data-act]");
  if (a) return setAct(a.dataset.act);
  if (e.target.closest("#btn-details")) return setTab(tab ? null : "backlog");
  if (e.target.closest("#drawer-close, #scrim")) { setTab(null); $("#btn-details").focus(); return; }
  const slot = e.target.closest("[data-slots]");
  if (slot) {
    const next = Math.min(settings.max_worker_slots, Math.max(1, Number($("#slots").textContent) + Number(slot.dataset.slots)));
    slotsWanted = next === settings.worker_slots ? null : next;
    return updateSlots();
  }
  if (e.target.closest("#btn-bulk")) return startBulk();
  if (e.target.closest("#btn-trickle")) return startTrickle();
  if (e.target.closest("#btn-tiered")) return startTiered();
  if (e.target.closest("#btn-reset")) return resetRun();
  if (e.target.closest("#btn-again")) { setAct("act1"); return resetRun("act1"); }
});

document.addEventListener("keydown", e => {
  if (e.target.closest("input, textarea, select, [contenteditable]") || e.metaKey || e.ctrlKey || e.altKey) return;
  const k = e.key.toLowerCase();
  if (k === "escape" && tab) { setTab(null); return; }
  if (k === "d" && act !== "close") return $("#btn-details").click();
  if (tab && "1234".includes(k)) return setTab(TABS[Number(k) - 1]);
  if (k === "b" && act === "act1") startBulk();
  else if (k === "b" && act === "act2") startTiered();
  else if (k === "t" && act === "act1") startTrickle();
  else if (k === "r") { if (act === "close") setAct("act1"); resetRun(act === "close" ? "act1" : undefined); }
  else if (k === "1") setAct("act1");
  else if (k === "2") setAct("act2");
  else if (k === "3") setAct("close");
});

const motionToggle = $("#motion-toggle");
motionToggle.addEventListener("click", () => {
  const paused = document.body.classList.toggle("motion-paused");
  Starfield.setPaused(paused);
  motionToggle.setAttribute("aria-pressed", String(paused));
  motionToggle.textContent = paused ? "Resume motion" : "Pause motion";
});

let lastSize = "";
window.addEventListener("resize", () => {
  const size = `${innerWidth}x${innerHeight}`;
  if (size !== lastSize) { lastSize = size; drawCharts(); }
});
window.addEventListener("hashchange", () => go(location.hash));
document.addEventListener("visibilitychange", poll);
Starfield.start(document.getElementById("stars"));
document.fonts?.ready.then(drawCharts);

// Settings and the first view are fetched together, not one after the other. A tab's very
// first load waits for settings, which is the request that issues the session cookie.
const firstSettings = api("/api/settings");
const firstView = (returning ? Promise.resolve() : firstSettings.catch(() => null))
  .then(() => api(`/api/runs/${run}/view`)).catch(() => null);
firstSettings.then(async s => {
  settings = s;
  $("#bulk-sub").textContent = `Acme · ${num(s.acme_bulk_docs)} docs`;
  $("#tiered-sub").textContent = `${num(s.tiered_docs_per_tenant)} docs each`;
  $("#session-chip").textContent = `Session ${s.session}`;
  for (const i of $$(".weights-edit input")) i.value = s.plan_weights[i.dataset.plan];
  updateSlots();
  const v = await firstView;
  if (v) { view = v; viewAt = Date.now(); }
  go(location.hash || "#act1");
  renderFreshness();
  setInterval(poll, POLL_MS);
  setInterval(renderFreshness, 1000);
}).catch(e => { ack(`Couldn't reach the API: ${e.message}`); failing = true; renderFreshness(); });
