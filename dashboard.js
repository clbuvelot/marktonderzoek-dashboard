'use strict';
const C = DashboardCore;
let DATA = [], searchIndex = new Map(), loaded = false;
const cl = C.cl, winnaarNamen = C.names;
const htmlTekst = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
const H = htmlTekst;
const AI = '<span class="ai-label" title="AI-geschat kenmerk">AI</span>';
const STORAGE = 'ecorys-dashboard-ux-v1';
let app = {mode: 'tenders', record: null, states: {tenders: C.defaults('tenders'), onderzoek: C.defaults('onderzoek')}};
let charts = [], drillTargets = [], lastRoute = '', focusBeforeDetail = null;
try {
  const saved = JSON.parse(localStorage.getItem(STORAGE));
  if (saved?.version === 1) {
    app.mode = saved.mode === 'onderzoek' ? 'onderzoek' : 'tenders';
    for (const mode of ['tenders', 'onderzoek']) app.states[mode] = C.sanitize(saved.states?.[mode], mode);
  }
} catch (_) { /* Een geblokkeerde of verouderde opslag verhindert gebruik niet. */ }
const state = () => app.states[app.mode];
const clone = value => JSON.parse(JSON.stringify(value));
function route(mode = app.mode, record = null, sub = app.states[mode].sub) {
  return '#' + mode + (mode === 'onderzoek' ? '/' + sub : '') + (record ? '/opdracht/' + encodeURIComponent(record) : '');
}
function parseRoute(hash) {
  const parts = hash.replace(/^#/, '').split('/');
  const mode = parts[0] === 'onderzoek' ? 'onderzoek' : 'tenders';
  const sub = parts[1] === 'markt' ? 'markt' : 'uitvragen';
  const i = parts.indexOf('opdracht');
  let record = null;
  if (i >= 0) { try { record = decodeURIComponent(parts[i + 1] || ''); } catch (_) { record = ''; } }
  return {mode, sub, record};
}
function save() {
  try { localStorage.setItem(STORAGE, JSON.stringify({version: 1, mode: app.mode, states: app.states})); } catch (_) {}
  if (loaded) history.replaceState({...history.state, ux: clone(app)}, '', location.href);
}
function capture() { if (!app.record) state().scroll = window.scrollY; save(); }
function navigate(mode, {record = null, sub = app.states[mode].sub, selection = null, back = false} = {}) {
  capture();
  if (selection) app.states[mode] = selection;
  app.mode = mode; app.record = record; state().sub = sub;
  const hash = route(mode, record, sub);
  history.pushState({ux: clone(app), canBack: back}, '', hash);
  lastRoute = hash;
  save(); renderPage(true);
}
function restoreRoute(event) {
  if (!loaded) return;
  if (event.type === 'hashchange' && lastRoute === location.hash) return;
  if (event.state?.ux) {
    for (const mode of ['tenders', 'onderzoek']) app.states[mode] = C.sanitize(event.state.ux.states?.[mode], mode);
  }
  const r = parseRoute(location.hash);
  app.mode = r.mode; app.record = r.record; state().sub = r.sub;
  lastRoute = location.hash; save(); renderPage(true);
}
window.addEventListener('popstate', restoreRoute);
window.addEventListener('hashchange', restoreRoute);
window.addEventListener('pagehide', capture);
let scrollTimer;
window.addEventListener('scroll', () => { clearTimeout(scrollTimer); scrollTimer = setTimeout(() => { if (loaded && !app.record) capture(); }, 150); }, {passive: true});

async function loadData() {
  const main = document.getElementById('content');
  main.innerHTML = '<div class="empty" role="status">Opdrachten laden…</div>';
  try {
    const resp = await fetch('data/geclassificeerd_met_winnaar.json', {cache: 'no-cache'});
    if (!resp.ok) throw new Error('data');
    const data = await resp.json();
    if (!Array.isArray(data) || data.some(r => !r || typeof r.id !== 'string')) throw new Error('vorm');
    DATA = data; searchIndex = new Map(DATA.map(r => [r.id, C.searchText(r)]));
  } catch (_) {
    main.innerHTML = '<section class="empty" role="alert"><h1>De opdrachten konden niet worden geladen</h1><p>Probeer het opnieuw. Je selecties blijven bewaard.</p><button data-action="retry" class="button primary">Opnieuw proberen</button></section>';
    return;
  }
  const years = [...new Set(DATA.map(r => C.field(r, 'jaar')).filter(Boolean))].sort();
  document.getElementById('dataset-periode').textContent = years.length ? `TenderNed ${years[0]}–${years.at(-1)}` : 'TenderNed';
  try {
    const resp = await fetch('data/update_staat.json', {cache: 'no-cache'});
    if (!resp.ok) throw new Error();
    const s = await resp.json(), date = new Date(s.laatste_update);
    if (!s.laatste_update || Number.isNaN(date.getTime())) throw new Error();
    const el = document.getElementById('dataset-update');
    el.textContent = 'Broncontrole ' + date.toLocaleDateString('nl-NL', {timeZone: 'Europe/Amsterdam'});
    el.title = date.toLocaleString('nl-NL', {timeZone: 'Europe/Amsterdam'}) + ' (Nederlandse tijd)' + (s.totaal_records !== DATA.length ? ' · telling wijkt af' : '');
  } catch (_) { document.getElementById('dataset-update').textContent = 'Broncontrole onbekend'; }
  if (location.hash && location.hash !== '#content') {
    const r = parseRoute(location.hash); app.mode = r.mode; app.record = r.record; state().sub = r.sub;
  }
  loaded = true;
  lastRoute = route(app.mode, app.record);
  history.replaceState({ux: clone(app)}, '', lastRoute);
  save(); renderPage();
}

function formatDate(value, time = false) {
  if (!value) return 'Onbekend';
  const ms = time ? C.deadline(value) : Date.parse(String(value).slice(0, 10) + 'T12:00:00Z');
  if (ms === null || !Number.isFinite(ms)) return 'Onbekend';
  return new Intl.DateTimeFormat('nl-NL', {timeZone: 'Europe/Amsterdam', day: 'numeric', month: 'short', year: 'numeric', ...(time ? {hour: '2-digit', minute: '2-digit'} : {})}).format(ms);
}
function statusPill(r) { const key = C.status(r); return `<span class="status ${key}">${H(C.LABELS[key])}</span>`; }
function winnerLabel(r) { return C.names(r).join(', ') || (C.status(r) === 'awarded' ? 'Winnaar onbekend' : 'Nog geen bekende winnaar'); }
function winnerSummary(r) {
  const selected = new Set(state().filters.winnaars?.values || []);
  const names = C.names(r).sort((a, b) => Number(selected.has(b)) - Number(selected.has(a)));
  if (names.length <= 3) return `<strong>${H(names.join(', ') || winnerLabel(r))}</strong>`;
  return `<strong>${H(names.slice(0, 3).join(', '))}</strong><details class="winner-list"><summary>Toon ${names.length - 3} andere mede-winnaars</summary><ul>${names.slice(3).map(n => `<li>${H(n)}</li>`).join('')}</ul></details>`;
}
function button(action, label, attrs = '', cls = '') { return `<button type="button" class="button ${cls}" data-action="${action}" ${attrs}>${label}</button>`; }
function destroyCharts() { charts.forEach(c => c.destroy()); charts = []; }
function renderPage(moveFocus = false) {
  if (!loaded) return;
  destroyCharts();
  for (const mode of ['tenders', 'onderzoek']) {
    const el = document.getElementById('nav-' + mode);
    if (app.mode === mode) el.setAttribute('aria-current', 'page'); else el.removeAttribute('aria-current');
    el.href = route(mode);
  }
  const main = document.getElementById('content');
  if (app.record !== null) renderDetail(main);
  else {
    const tenders = app.mode === 'tenders', s = state();
    main.innerHTML = `<section class="page-intro"><p class="eyebrow">${tenders ? 'Actuele tenders' : 'Onderzoek & markt'}</p><h1 tabindex="-1">${tenders ? 'Vind je volgende onderzoek' : 'Onderzoek uitvragen en de markt'}</h1><p>${tenders ? 'Bekijk waar je nog op kunt inschrijven en wat bij jouw team past.' : 'Zoek eerdere én actuele opdrachten. Bekijk wat gevraagd wordt en wie er wint.'}</p></section>
    ${state().teamNotice ? `<div class="context-note team-notice" role="status"><p>De selectie onderscheidt nu teams binnen Regions &amp; cities en hele units. Je herkenbare keuzes zijn behouden; onbekende labels zijn vervallen.</p>${button('dismiss-team-notice', 'Begrepen', '', 'text-button')}</div>` : ''}
      <section class="search-panel" aria-label="Zoeken en filteren">
        <label class="search-label" for="search">Zoek opdrachten</label><div class="search-wrap"><svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4.5 4.5"/></svg><input type="search" id="search" value="${H(s.query)}" placeholder="Bijvoorbeeld arbeidsmarkt, Ecorys of een ministerie" autocomplete="off"></div>
        <div id="scope-options"></div><div class="filter-row" id="primary-filters"></div>
        <details id="more-filters" ${s.more ? 'open' : ''}><summary>Meer filters <span id="more-count"></span></summary><div class="filter-row" id="extra-filters"></div></details>
        <div id="active-tags" class="active-tags" aria-label="Actieve selectie"></div>
      </section>
      ${!tenders ? `<nav class="sub-nav" aria-label="Onderzoeksweergave"><a href="#onderzoek/uitvragen" ${s.sub === 'uitvragen' ? 'aria-current="page"' : ''}>Uitvragen</a><a href="#onderzoek/markt" ${s.sub === 'markt' ? 'aria-current="page"' : ''}>Marktbeeld</a></nav>` : ''}
      <section id="results" aria-label="Resultaten"></section>`;
    buildFilters(); renderResults();
    document.getElementById('more-filters').addEventListener('toggle', e => { state().more = e.target.open; save(); });
  }
  if (moveFocus) main.querySelector('h1')?.focus({preventScroll: true});
  requestAnimationFrame(() => {
    window.scrollTo(0, app.record !== null ? 0 : state().scroll);
    if (!app.record && focusBeforeDetail) {
      main.querySelector(`[data-record="${CSS.escape(focusBeforeDetail)}"]`)?.focus({preventScroll: true});
      focusBeforeDetail = null;
    }
  });
}

function organisationOptions(selected) {
  const topics = {'Evaluaties': 'Beleid, programma’s en organisaties', 'Brede Welvaart': 'Maatschappelijke en ruimtelijke afwegingen', 'Sustainable Transitions': 'Energie, klimaat en circulaire economie', 'Visitaties': 'Maatschappelijke prestaties woningcorporaties'};
  const option = (value, label, kind) => `<label class="filter-option organisation-option ${kind === 'Team' ? 'is-team' : ''}"><input type="checkbox" data-filter="teams" value="${H(value)}" aria-label="${H(value)}" ${selected.values.includes(value) ? 'checked' : ''}><span><span>${H(label)}${topics[value] ? `<em>${H(topics[value])}</em>` : ''}</span><small>${H(kind)}</small></span></label>`;
  return `<fieldset class="organisation-group"><legend>Regions &amp; cities</legend>${option('Regions & cities', 'De hele unit', 'Unit')}${C.TEAM_NAMES.map(t => option(t, t, 'Team')).join('')}</fieldset>
    <fieldset class="organisation-group"><legend>Andere units</legend>${C.UNIT_NAMES.filter(u => u !== 'Regions & cities').sort((a, b) => Number(b === 'TIM') - Number(a === 'TIM') || a.localeCompare(b, 'nl')).map(u => option(u, u, 'Unit')).join('')}</fieldset>`;
}
function buildFilters() {
  const tenders = app.mode === 'tenders', s = state();
  if (tenders) {
    document.getElementById('scope-options').innerHTML = `<div class="scope-choices" aria-label="Inschrijfstatus">${['open', 'unknown'].map(key => button('scope', `${H(C.LABELS[key])} <span class="count">${DATA.filter(r => C.status(r) === key).length}</span>`, `data-value="${key}" aria-pressed="${s.tenderScope === key}"`)).join('')}</div>`;
  }
  const primary = C.filtersFor(app.mode).filter(f => f.primary?.includes(app.mode)).map(f => f.id);
  for (const f of C.filtersFor(app.mode)) {
    const values = (f.id === 'teams' ? [...C.ORGANISATION_NAMES] : [...new Set(DATA.flatMap(r => {const v = C.field(r, f.id); return Array.isArray(v) ? v : [v];}).filter(v => v !== '' && v != null))]).sort((a, b) => String(a).localeCompare(String(b), 'nl'));
    const selected = s.filters[f.id] || {values: []};
    const html = `<details class="filter-item" id="fi-${f.id}"><summary>${H(f.label)} ${f.ai ? AI : ''}<span class="filter-count" data-count="${f.id}">${selected.values.length || ''}</span></summary>
      <div class="filter-dropdown" id="dd-${f.id}">${f.id === 'teams' ? '<p class="team-filter-help">Kies teams binnen Regions &amp; cities of hele units. Je ziet opdrachten die bij minstens één keuze passen. Een gedeelde opdracht telt één keer.</p>' : ''}<label class="sr-only" for="find-${f.id}">Zoek ${H(f.label.toLowerCase())}</label><input id="find-${f.id}" data-option-search="${f.id}" type="search" placeholder="Zoek ${H(f.label.toLowerCase())}…">
      <div class="filter-options">${f.id === 'teams' ? organisationOptions(selected) : values.map(v => `<label class="filter-option"><input type="checkbox" data-filter="${f.id}" value="${H(v)}" ${selected.values.includes(v) ? 'checked' : ''}><span>${H(v)}</span></label>`).join('')}</div>
      ${button('clear-filter', 'Wis deze keuze', `data-filter-id="${f.id}"`, 'text-button')}</div></details>`;
    document.getElementById(primary.includes(f.id) ? 'primary-filters' : 'extra-filters').insertAdjacentHTML('beforeend', html);
  }
  if (tenders && s.tenderScope === 'open') document.getElementById('primary-filters').insertAdjacentHTML('beforeend', `<label class="inline-field">Deadline<select id="deadline-days">${[['', 'Alle deadlines'], ['7', 'Binnen 7 dagen'], ['30', 'Binnen 30 dagen']].map(([v, t]) => `<option value="${v}" ${s.deadlineDays === v ? 'selected' : ''}>${t}</option>`).join('')}</select></label>`);
  if (!tenders) {
    const period = `<fieldset class="period"><legend>Periode · uitvraagdatum</legend><label>Van<input id="date-from" type="date" value="${H(s.from)}" ${s.to ? `max="${H(s.to)}"` : ''}></label><label>Tot en met<input id="date-to" type="date" value="${H(s.to)}" ${s.from ? `min="${H(s.from)}"` : ''}></label></fieldset>`;
    document.getElementById('fi-winnaars').insertAdjacentHTML('beforebegin', period);
  }
  document.querySelectorAll('.filter-item').forEach(el => el.addEventListener('toggle', () => {
    if (el.open) {
      document.querySelectorAll('.filter-item').forEach(other => { if (other !== el) other.open = false; });
      const dropdown = el.querySelector('.filter-dropdown');
      dropdown.style.left = '0px';
      const box = dropdown.getBoundingClientRect();
      if (box.right > innerWidth - 16) dropdown.style.left = `${innerWidth - 16 - box.right}px`;
      dropdown.style.maxHeight = `${Math.max(180, innerHeight - box.top - 16)}px`;
    }
  }));
}
function updateTags() {
  const s = state(), tags = [];
  if (s.query) tags.push(button('clear-query', `Zoek: ${H(s.query)} ×`, 'aria-label="Wis zoekopdracht"', 'tag'));
  for (const f of C.filtersFor(app.mode)) {
    const selected = s.filters[f.id];
    const count = document.querySelector(`[data-count="${f.id}"]`);
    if (count) count.textContent = selected?.values?.length || '';
    for (const value of selected?.values || []) tags.push(button('remove-filter', `${H(f.label)}: ${H(value)} ${f.ai ? AI : ''} ×`, `data-filter-id="${f.id}" data-value="${H(value)}" aria-label="Verwijder ${H(f.label)}: ${H(value)}"`, 'tag'));
  }
  if (s.from || s.to) tags.push(button('clear-period', `Periode: ${H(s.from || 'begin')} t/m ${H(s.to || 'nu')} ×`, '', 'tag'));
  if (s.deadlineDays) tags.push(button('clear-deadline', `Binnen ${s.deadlineDays} dagen ×`, '', 'tag'));
  if (s.context) tags.push(button('clear-context', `${s.context.type === 'buyer' ? 'Eerdere uitvragen van' : 'Zelfde onderwerp:'} ${H(s.context.value)} ×`, '', 'tag'));
  if (s.drill) tags.push(button('clear-drill', `Uit marktbeeld: ${H(s.drill.label)} ×`, '', 'tag'));
  if (tags.length) tags.push(button('reset', 'Wis filters', '', 'text-button'));
  document.getElementById('active-tags').innerHTML = tags.join('');
  const extra = C.filtersFor(app.mode).filter(f => !f.primary?.includes(app.mode) && s.filters[f.id]?.values.length).length;
  document.getElementById('more-count').textContent = extra ? `(${extra} actief)` : '';
}
function changed({rebuild = false} = {}) {
  state().limit = 50; state().scroll = 0; save();
  if (rebuild) renderPage(); else renderResults();
}
function renderResults() {
  if (app.record !== null || !document.getElementById('results')) return;
  updateTags(); destroyCharts(); drillTargets = [];
  const records = C.select(DATA, state(), app.mode, Date.now(), searchIndex);
  const results = document.getElementById('results');
  if (app.mode === 'onderzoek' && state().sub === 'markt') renderMarket(results, records);
  else renderList(results, records);
}
function renderList(el, records) {
  const s = state(), tenders = app.mode === 'tenders';
  const ordered = C.sort(records, s.sort), shown = ordered.slice(0, s.limit);
  el.innerHTML = `${s.drill ? button('back-market', '← Terug naar marktbeeld', '', 'back-link') : ''}
    ${s.context ? `<div class="context-note"><span>${s.context.type === 'similar' ? 'Overeenkomst op AI-geclassificeerd onderwerp.' : 'Uitvragen van dezelfde opdrachtgever, gepubliceerd vóór de oorspronkelijke opdracht.'}</span>${button('context-origin', 'Terug naar oorspronkelijke opdracht')}</div>` : ''}
    <div class="results-heading"><div><h2>${tenders ? (s.tenderScope === 'open' ? 'Open voor inschrijving' : 'Controleer de inschrijfstatus') : 'Uitvragen'} <span class="result-number" id="s-totaal" aria-live="polite">${records.length}</span></h2><p id="result-caption">${tenders ? (s.tenderScope === 'unknown' ? 'De deadline ontbreekt of is ongeldig. Bekijk de officiële uitvraag.' : 'Deadlines in Nederlandse tijd. Controleer de officiële uitvraag.') : `Uit ${DATA.length} opdrachten · inclusief mede-winnaars`}</p></div><label class="sort-label">Sorteer op<select id="sort">${[...(tenders && s.tenderScope === 'open' ? [['deadline', 'Eerstvolgende deadline']] : []), ['newest', 'Nieuwste publicatie'], ['title', 'Titel A–Z']].map(([k, v]) => `<option value="${k}" ${s.sort === k ? 'selected' : ''}>${v}</option>`).join('')}</select></label></div>
    ${!records.length ? `<div class="empty"><h3>Geen ${tenders ? 'tenders' : 'uitvragen'} in deze selectie</h3><p>${tenders && s.tenderScope === 'open' ? 'Er zijn geen open inschrijvingen die passen bij je zoekopdracht en filters.' : 'Pas je zoekopdracht of filters aan om meer opdrachten te zien.'}</p>${button('reset', 'Wis filters', '', 'primary')}</div>` : `<div class="opportunity-list" id="opportunities">${shown.map(r => renderCard(r, tenders)).join('')}</div>`}
    ${records.length > shown.length ? `<div class="show-more">${button('more', 'Toon meer')}<span>${shown.length} van ${records.length} getoond</span></div>` : ''}`;
}
function renderCard(r, tenders) {
  const c = cl(r), ms = C.deadline(r.sluitingsdatum);
  const urgent = C.status(r) === 'open' && ms - Date.now() <= 7 * 86400000;
  return `<article class="opportunity ${tenders ? '' : 'research-card'}" data-id="${H(r.id)}">
    <div class="opportunity-main"><div class="card-kicker"><span>${H(r.aanbestedende_dienst || 'Opdrachtgever onbekend')}</span>${statusPill(r)}</div>
      ${tenders ? `<p class="mobile-deadline ${urgent ? 'soon' : ''}">${ms !== null ? `Inschrijfdeadline <time datetime="${H(r.sluitingsdatum)}">${H(formatDate(r.sluitingsdatum, true))}</time>` : 'Inschrijfdeadline onbekend'}</p>` : ''}
      <h3><a href="${H(route(app.mode, r.id))}" data-record="${H(r.id)}">${H(r.titel || 'Uitvraag zonder titel')}<svg class="icon card-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M4 12h15m-6-6 6 6-6 6"/></svg></a></h3>
      <p class="excerpt">${H(r.omschrijving || 'Bekijk de uitvraag voor de inhoud en voorwaarden.')}</p>
      <div class="card-tags"><span>${H(c.thema || 'Onderwerp onbekend')} ${AI}</span>${tenders ? `<span>${H(C.teamSummary(r))} ${AI}</span>` : `<span>${H(c.methodiek || 'Methodiek onbekend')} ${AI}</span>`}</div>
    </div><div class="opportunity-meta ${urgent ? 'closing-soon' : ''}">${tenders ? `<span class="meta-label">Inschrijfdeadline</span>${ms !== null ? `<time class="deadline-date" datetime="${H(r.sluitingsdatum)}">${H(new Intl.DateTimeFormat('nl-NL', {timeZone: 'Europe/Amsterdam', day: 'numeric', month: 'short', year: 'numeric'}).format(ms))}<span>${H(new Intl.DateTimeFormat('nl-NL', {timeZone: 'Europe/Amsterdam', hour: '2-digit', minute: '2-digit'}).format(ms))} uur</span></time>` : '<strong>Onbekend</strong>'}${urgent ? '<span class="urgency-note">Sluit binnen 7 dagen</span>' : ''}<span class="meta-foot">Gepubliceerd ${H(formatDate(r.publicatiedatum))}</span>` : `<span class="meta-label">Bekende winnaar${C.names(r).length > 1 ? 's' : ''}</span>${winnerSummary(r)}<span class="meta-foot">Uitvraag ${H(formatDate(r.publicatiedatum))}</span>`}</div></article>`;
}

function groupButton(group, label = group.label) {
  const index = drillTargets.push({label, ids: group.ids}) - 1;
  return button('drill', `${H(label)} <span class="count">${group.count ?? group.ids.length}</span>`, `data-index="${index}"`, 'chart-link');
}
function renderMarket(el, records) {
  const known = records.filter(r => C.names(r).length), unique = new Set(known.flatMap(C.names)).size;
  const coverage = records.length ? Math.round(100 * known.length / records.length) : 0;
  el.innerHTML = `<div class="results-heading"><div><h2>Marktbeeld</h2><p>Elke grafiek opent de bijbehorende uitvragen.</p></div>${button('view-list', 'Bekijk uitvragen →')}</div>
    <div class="metrics">${[
      ['Opdrachten', records.length, `van ${DATA.length} in de dataset`, 's-totaal'],
      ['Opdrachtgevers', new Set(records.map(r => r.aanbestedende_dienst).filter(Boolean)).size, 'binnen deze selectie', 's-opdrachtgevers'],
      ['Bekende winnaar', known.length, `${coverage}% van deze selectie`, 's-bekend'],
      ['Winnende bureaus', unique, 'inclusief mede-winnaars', 's-winnaars']
    ].map(([label, n, sub, id]) => `<div class="metric"><span>${label}</span><strong id="${id}">${n}</strong><small>${sub}</small></div>`).join('')}</div>
    <div class="market-controls"><label>Verdeel op <select id="group">${C.groups.map(f => `<option value="${f.id}" ${state().group === f.id ? 'selected' : ''}>${H(f.label)}${f.ai ? ' (AI)' : ''}</option>`).join('')}</select></label><span>Jaren verwijzen naar de uitvraag, niet de gunning. Het lopende jaar is onvolledig.</span></div>
    <div class="chart-grid"><section class="chart-card"><h3>Uitvragen per jaar</h3><div class="canvas-wrap"><canvas id="trend" aria-label="Uitvragen per jaar" role="img"></canvas></div><div id="trend-links" class="chart-links" aria-label="Uitvragen per jaar bekijken"></div></section>
    <section class="chart-card"><h3>Verdeling: ${H(C.filters.find(f => f.id === state().group)?.label || 'Onderwerp')}</h3><div class="canvas-wrap"><canvas id="distribution" aria-label="Verdeling van opdrachten" role="img"></canvas></div><div id="distribution-links" class="chart-links"></div></section>
    <section class="chart-card"><h3>Top opdrachtgevers</h3><div id="buyers"></div></section><section class="chart-card"><h3>Top winnaars</h3><div id="winners"></div><p class="note">Elke mede-winnaar telt eenmaal per opdracht.</p></section></div>
    <section class="chart-card competition"><div class="results-heading"><div><h3>Concurrentiekaart</h3><p id="conc-count"></p></div>${button('all-bureaus', state().allBureaus ? 'Toon top 20' : 'Toon alle bureaus', 'id="alle-bureaus"')}</div><div class="table-scroll"><table><thead><tr><th>Bureau</th><th>Gewonnen</th><th>Kwantitatief ${AI}</th><th>Sterkste onderwerp ${AI}</th><th>Sterkste methodiek ${AI}</th></tr></thead><tbody id="conc-body"></tbody></table></div><p class="note">Aantallen opdrachten, geen omzetmarktaandelen. Naamvarianten en ontbrekende brongegevens kunnen het beeld beïnvloeden.</p></section>`;
  const years = C.buckets(records, 'jaar').sort((a, b) => a.label.localeCompare(b.label));
  const distribution = C.buckets(records, state().group, 10);
  drawChart('trend', years, false); drawChart('distribution', distribution, true);
  document.getElementById('trend-links').innerHTML = years.map(g => groupButton(g)).join('');
  document.getElementById('distribution-links').innerHTML = distribution.map(g => groupButton(g)).join('');
  const buyers = C.buckets(records, 'aanbestedende_dienst');
  const winners = C.buckets(known, 'winnaars');
  renderBars('buyers', buyers.slice(0, 10)); renderBars('winners', winners.slice(0, 10));
  const lookup = new Map(records.map(r => [r.id, r]));
  const shown = state().allBureaus ? winners : winners.slice(0, 20);
  document.getElementById('conc-count').textContent = `${winners.length} bureaus${shown.length < winners.length ? ' · top 20' : ''}`;
  document.getElementById('conc-body').innerHTML = shown.map(g => {
    const selected = g.ids.map(id => lookup.get(id));
    const theme = C.buckets(selected, 'thema')[0]?.label || 'Onbekend', method = C.buckets(selected, 'methodiek')[0]?.label || 'Onbekend';
    const pct = Math.round(selected.filter(r => cl(r).methodiek_kwantitatief).length * 100 / selected.length);
    return `<tr><td data-label="Bureau">${groupButton(g)}</td><td data-label="Gewonnen">${g.count}</td><td data-label="Kwantitatief">${pct}%</td><td data-label="Onderwerp">${H(theme)}</td><td data-label="Methodiek">${H(method)}</td></tr>`;
  }).join('') || '<tr><td colspan="5">Geen bekende winnaars in deze selectie.</td></tr>';
}
function renderBars(id, groups) {
  document.getElementById(id).innerHTML = groups.length ? groups.map(g => `<div class="bar-row">${groupButton(g)}<span class="bar-track" aria-hidden="true"><span style="width:${Math.round(g.count * 100 / groups[0].count)}%"></span></span></div>`).join('') : '<p class="note">Geen gegevens in deze selectie.</p>';
}
function drawChart(id, groups, horizontal) {
  const canvas = document.getElementById(id);
  if (typeof Chart === 'undefined') { canvas.parentElement.innerHTML = '<p class="chart-fallback">De grafiek kon niet worden geladen. Bekijk de aantallen en uitvragen via de knoppen hieronder.</p>'; return; }
  try {
    Chart.defaults.font.family = 'Montserrat, Geneva, sans-serif';
    Chart.defaults.font.size = 11;
    Chart.defaults.color = '#505354';
    charts.push(new Chart(canvas, {type: 'bar', data: {labels: groups.map(g => g.label), datasets: [{data: groups.map(g => g.count), backgroundColor: horizontal ? '#0B7C85' : '#006EB8', hoverBackgroundColor: '#002137', borderRadius: 1, maxBarThickness: horizontal ? 18 : 38}]}, options: {
      indexAxis: horizontal ? 'y' : 'x', responsive: true, maintainAspectRatio: false, animation: false, plugins: {legend: {display: false}, tooltip: {backgroundColor: '#002137', padding: 12, cornerRadius: 2}},
      scales: {x: {border: {display: false}, grid: {color: '#E7ECEF', drawTicks: false}, ticks: {precision: 0, padding: 8}}, y: {border: {display: false}, grid: {color: horizontal ? 'transparent' : '#E7ECEF', drawTicks: false}, ticks: {precision: 0, padding: 8, callback: function (value) {const t = this.getLabelForValue(value); return horizontal && t.length > 23 ? t.slice(0, 23) + '…' : t;}}}},
      onClick: (_e, elements) => { if (elements.length) drill(groups[elements[0].index]); }, onHover: (e, elements) => {if (e.native?.target) e.native.target.style.cursor = elements.length ? 'pointer' : 'default';}
    }}));
  } catch (_) { canvas.parentElement.innerHTML = '<p class="chart-fallback">Gebruik de aantallen hieronder om uitvragen te bekijken.</p>'; }
}
function drill(group) {
  const s = clone(state()); s.drill = {label: group.label, ids: group.ids}; s.limit = 50; s.scroll = 0;
  navigate('onderzoek', {sub: 'uitvragen', selection: s, back: true});
}

function detailField(label, value, ai = false) { return `<div class="detail-field"><dt>${H(label)} ${ai ? AI : ''}</dt><dd>${H(Array.isArray(value) ? value.join(', ') || 'Onbekend' : value ?? 'Onbekend')}</dd></div>`; }
function safeUrl(value) { try { const u = new URL(value); return ['https:', 'http:'].includes(u.protocol) ? u.href : ''; } catch (_) { return ''; } }
function teamDetail(r) {
  const c = cl(r), p = c.opdrachtprofiel;
  const match = (m, kind) => `<div class="${kind === 'team' ? 'team-match' : 'unit-match team-match'}"><h3>${H(m[kind])} <small>${kind === 'team' ? 'Team · Regions &amp; cities' : 'Unit'}</small></h3><p>${H(m.reden)}</p><blockquote><span>Uit de uitvraag</span>${H(m.bewijs)}</blockquote></div>`;
  const uncertain = (c.teams_afwegingen || []).filter(a => a.oordeel === 'onduidelijk');
  return `<section class="detail-section team-detail"><h2>Passende teams en units ${AI}</h2>
    ${p ? `<dl class="detail-grid assignment-profile">${detailField('De vraag', p.vraag)}${detailField('Onderwerp', p.object)}${detailField('Gevraagde producten', p.producten)}</dl>` : ''}
    <h3 class="team-section-heading">Teams binnen Regions &amp; cities</h3><p>${H(c.teams_toelichting || C.teamSummary(r))}</p>
    ${c.teams?.length > 1 ? '<p class="note">Deze uitvraag is aan meerdere teams gekoppeld. Elk team sluit inhoudelijk aan; de volgorde geeft geen voorkeur aan.</p>' : ''}
    ${(c.teams_matches || []).map(m => match(m, 'team')).join('')}
    ${uncertain.length ? `<details class="uncertain-teams"><summary>Nog te beoordelen: ${H(uncertain.map(a => a.team).join(', '))}</summary>${uncertain.map(a => `<p><strong>${H(a.team)}</strong> — ${H(a.reden)}</p>`).join('')}</details>` : ''}
    ${c.units_matches?.length ? `<details class="unit-matches"><summary>Passende units: ${H(c.units.join(', '))}</summary>${c.units_matches.map(m => match(m, 'unit')).join('')}</details>` : ''}
    <p class="note">Geschat op basis van de uitvraag en eerder Ecorys-werk. De referenties dekken niet alle ervaring; controleer de aansluiting en beschikbaarheid bij het team.</p></section>`;
}
function renderDetail(main) {
  const r = DATA.find(r => r.id === app.record);
  if (!r) { main.innerHTML = `${button('back', '← Terug naar overzicht', '', 'back-link')}<section class="empty"><h1 tabindex="-1">Opdracht niet gevonden</h1><p>Deze opdracht is niet beschikbaar in de huidige dataset.</p></section>`; return; }
  const c = cl(r), tenders = app.mode === 'tenders';
  const source = safeUrl(r.url) || `https://www.tenderned.nl/aankondigingen/overzicht/${encodeURIComponent(r.id)}`;
  const grants = [...new Set([...(r.gunning_publicatie_ids || []), r.gunning_publicatie_id].filter(Boolean))];
  main.innerHTML = `${button('back', '← Terug naar ' + (tenders ? 'tenders' : 'onderzoek'), '', 'back-link')}
    <article class="detail-page"><div class="detail-heading"><p class="eyebrow">${H(r.aanbestedende_dienst || 'Opdrachtgever onbekend')}</p><h1 tabindex="-1">${H(r.titel)}</h1>${statusPill(r)}<div class="detail-highlight"><span>${tenders ? 'Inschrijfdeadline · Nederlandse tijd' : 'Bekende winnaar' + (C.names(r).length > 1 ? 's' : '')}</span>${tenders ? `<strong>${H(formatDate(r.sluitingsdatum, true))}</strong>` : winnerSummary(r)}</div>
    <div class="detail-actions"><a class="button primary" href="${H(source)}" target="_blank" rel="noopener">Bekijk officiële uitvraag ↗</a>${r.documenten_geladen ? button('chat', 'Vraag documenten', `data-record-id="${H(r.id)}"`) : '<span class="note">Geen documenttekst beschikbaar</span>'}</div></div>
    <div class="detail-layout"><div><section class="detail-section"><h2>De opdracht</h2><p class="description">${H(r.omschrijving || 'Geen omschrijving beschikbaar. Bekijk de officiële uitvraag.')}</p></section>${teamDetail(r)}
    <section class="detail-section"><h2>Onderzoekskenmerken ${AI}</h2><p class="note">Deze kenmerken zijn geschat. Bekijk de uitvraag voor de exacte vereisten.</p><dl class="detail-grid">${detailField('Onderwerp', c.thema, true)}${detailField('Subthema', c.subthema, true)}${detailField('Methodiek', c.methodiek, true)}${detailField('Evaluatiefase', c.evaluatiefase, true)}${detailField('Databronnen', c.databronnen, true)}${detailField('Skills', c.skills, true)}${detailField('Opdrachtgrootte', c.opdrachtgrootte, true)}${detailField('Herhalingskans', c.herhalingskans, true)}${detailField('Consortiumkans', C.field(r, 'consortiumkans'), true)}${detailField('Causaal effect gevraagd', C.field(r, 'causaliteitsvraag'), true)}${detailField('Betrouwbaarheid classificatie', typeof c.betrouwbaarheid === 'number' ? Math.round(c.betrouwbaarheid * 100) + '% (modelinschatting)' : 'Onbekend', true)}</dl>${!c.teams_versie ? `<p class="note">${H(c.toelichting || '')}</p>` : ''}</section></div>
    <aside><section class="detail-section"><h2>Publicatie en uitkomst</h2><dl>${detailField('Uitvraagdatum', formatDate(r.publicatiedatum))}${detailField('Inschrijfdeadline', formatDate(r.sluitingsdatum, true))}${detailField('Winnaars', winnerLabel(r))}${detailField('Publicatiedatum gunning', formatDate(r.gunningsdatum))}${r.datum_gunning ? detailField('Datum contractgunning', formatDate(r.datum_gunning)) : ''}${detailField('Procedure', r.procedure_type)}${detailField('Aard opdracht', r.type_opdracht_omschrijving)}${detailField('Aanbesteding', C.field(r, 'europees'))}${detailField('Waarde', r.waarde_definitief ? '€ ' + Number(r.waarde_definitief).toLocaleString('nl-NL') : r.gunningswaarde ? '€ ' + Number(r.gunningswaarde).toLocaleString('nl-NL') + ' (geschat)' : 'Onbekend')}</dl>
    ${grants.length ? `<h3>Gunningspublicaties</h3><ul class="source-links">${grants.map(id => `<li><a target="_blank" rel="noopener" href="https://www.tenderned.nl/aankondigingen/overzicht/${encodeURIComponent(id)}">Gunning ${H(id)} ↗</a></li>`).join('')}</ul>` : ''}</section>
    <section class="detail-section related"><h2>Verder onderzoeken</h2>${r.aanbestedende_dienst && r.publicatiedatum ? button('related-buyer', 'Eerdere uitvragen van deze opdrachtgever →', `data-record-id="${H(r.id)}"`) : ''}${c.thema && !/onbekend|overig|onduidelijk/i.test(c.thema) ? button('related-topic', 'Vergelijkbare opdrachten →', `data-record-id="${H(r.id)}"`) + '<p class="note">Op basis van hetzelfde AI-geclassificeerde onderwerp.</p>' : ''}</section></aside></div></article>`;
}

document.addEventListener('click', e => {
  const link = e.target.closest('a');
  if (link?.classList.contains('skip-link')) { e.preventDefault(); document.getElementById('content').focus(); return; }
  if (link?.getAttribute('href')?.startsWith('#') && !e.ctrlKey && !e.metaKey && !e.shiftKey && !e.altKey && loaded) {
    e.preventDefault();
    const r = parseRoute(link.getAttribute('href'));
    if (link.dataset.record) focusBeforeDetail = link.dataset.record;
    // Hoofdnavigatie herstelt de eerder gekozen onderzoeksweergave.
    navigate(r.mode, {record: r.record, sub: link.id?.startsWith('nav-') ? app.states[r.mode].sub : r.sub, back: r.record !== null});
    return;
  }
  const b = e.target.closest('[data-action]');
  if (!b) { if (!e.target.closest('.filter-item')) document.querySelectorAll('.filter-item[open]').forEach(d => d.open = false); return; }
  const s = state(), action = b.dataset.action;
  if (action === 'retry') { loadData(); return; }
  if (action === 'back') { if (history.state?.canBack) history.back(); else navigate(app.mode); return; }
  if (action === 'scope') { s.tenderScope = b.dataset.value; s.deadlineDays = ''; s.sort = s.tenderScope === 'open' ? 'deadline' : 'newest'; changed({rebuild: true}); return; }
  if (action === 'more') { s.limit += 50; save(); renderResults(); document.querySelectorAll('.opportunity h3 a')[s.limit - 50]?.focus({preventScroll: true}); return; }
  if (action === 'view-list') { navigate('onderzoek', {sub: 'uitvragen'}); return; }
  if (action === 'all-bureaus') { s.allBureaus = !s.allBureaus; save(); renderResults(); document.getElementById('alle-bureaus').focus({preventScroll: true}); return; }
  if (action === 'drill') { drill(drillTargets[+b.dataset.index]); return; }
  if (action === 'back-market') { if (history.state?.canBack) history.back(); else {const sel = clone(s); sel.drill = null; navigate('onderzoek', {sub: 'markt', selection: sel});} return; }
  if (action === 'context-origin') { if (history.state?.canBack) history.back(); else navigate('onderzoek', {record: s.context.id, back: true}); return; }
  if (action === 'chat') { const r = DATA.find(r => r.id === b.dataset.recordId); if (r) openDocChat(r.id, r.titel); return; }
  if (action.startsWith('related-')) {
    const r = DATA.find(r => r.id === b.dataset.recordId), sel = C.defaults('onderzoek');
    sel.context = action === 'related-buyer' ? {type: 'buyer', id: r.id, value: r.aanbestedende_dienst, before: r.publicatiedatum.slice(0, 10)} : {type: 'similar', id: r.id, value: cl(r).thema};
    navigate('onderzoek', {selection: sel, sub: 'uitvragen', back: true}); return;
  }
  if (action === 'dismiss-team-notice') { state().teamNotice = false; save(); renderPage(); return; }
  if (action === 'reset') {
    const fresh = C.defaults(app.mode); fresh.sub = s.sub; fresh.sort = s.sort; fresh.tenderScope = s.tenderScope;
    app.states[app.mode] = fresh;
  } else if (action === 'remove-filter') s.filters[b.dataset.filterId].values = s.filters[b.dataset.filterId].values.filter(v => v !== b.dataset.value);
  else if (action === 'clear-filter') delete s.filters[b.dataset.filterId];
  else if (action === 'clear-query') s.query = '';
  else if (action === 'clear-period') s.from = s.to = '';
  else if (action === 'clear-deadline') s.deadlineDays = '';
  else if (action === 'clear-context') s.context = null;
  else if (action === 'clear-drill') s.drill = null;
  else return;
  changed({rebuild: true}); document.getElementById('search')?.focus({preventScroll: true});
});
document.addEventListener('input', e => {
  if (e.target.id === 'search') { state().query = e.target.value; changed(); }
  if (e.target.dataset.optionSearch) {
    const word = C.norm(e.target.value);
    const dropdown = e.target.closest('.filter-dropdown');
    dropdown.querySelectorAll('.filter-option').forEach(el => el.hidden = !C.norm(el.textContent + ' ' + (el.querySelector('input')?.value || '')).includes(word));
    dropdown.querySelectorAll('.organisation-group').forEach(el => el.hidden = !el.querySelector('.filter-option:not([hidden])'));
  }
});
document.addEventListener('change', e => {
  const el = e.target, s = state();
  if (el.dataset.filter) {
    const selected = s.filters[el.dataset.filter] ||= {values: []};
    selected.values = el.checked ? [...new Set([...selected.values, el.value])] : selected.values.filter(v => v !== el.value);
  } else if (el.id === 'deadline-days') s.deadlineDays = el.value;
  else if (el.id === 'sort') s.sort = el.value;
  else if (el.id === 'group') s.group = el.value;
  else if (el.id === 'date-from' || el.id === 'date-to') {
    if (!el.validity.valid) { el.reportValidity(); return; }
    s[el.id === 'date-from' ? 'from' : 'to'] = el.value;
    document.getElementById('date-from').max = s.to; document.getElementById('date-to').min = s.from;
  } else return;
  changed();
});
document.addEventListener('keydown', e => {
  const modal = document.querySelector('.login-overlay.open, .modal-overlay.open');
  if (modal) {
    if (e.key === 'Escape' && modal.id === 'chat-overlay') { sluitModal(); return; }
    if (e.key === 'Tab') {
      const controls = [...modal.querySelectorAll('button:not(:disabled), input, textarea, a[href]')].filter(el => el.offsetParent !== null);
      if (e.shiftKey && document.activeElement === controls[0]) { e.preventDefault(); controls.at(-1).focus(); }
      else if (!e.shiftKey && document.activeElement === controls.at(-1)) { e.preventDefault(); controls[0].focus(); }
    }
  } else if (e.key === 'Escape') {
    const opened = document.querySelector('.filter-item[open]');
    if (opened) { opened.open = false; opened.querySelector('summary').focus(); }
  }
});
function refreshClock() { if (loaded && document.visibilityState !== 'hidden') {
  if (app.record) {
    const r = DATA.find(r => r.id === app.record);
    if (r) document.querySelectorAll('.detail-heading > .status').forEach(el => el.outerHTML = statusPill(r));
    return;
  }
  if (app.mode === 'tenders') document.querySelectorAll('[data-action="scope"]').forEach(b => b.querySelector('.count').textContent = DATA.filter(r => C.status(r) === b.dataset.value).length);
  renderResults();
} }
document.addEventListener('visibilitychange', refreshClock);
setInterval(refreshClock, 60000);
