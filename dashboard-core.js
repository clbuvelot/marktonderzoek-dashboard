/* Pure selectielogica, gedeeld door browser en regressietests. */
(function (root) {
  'use strict';
  const norm = value => String(value ?? '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const cl = r => r.classificatie || {};
  const TEAM_NAMES = ['Evaluaties', 'Brede Welvaart', 'Sustainable Transitions', 'Visitaties'];
  const UNIT_NAMES = ['Resilient Economies', 'Social and Health', 'Natural Resources', 'Regions & cities', 'TIM', 'Security & Justice'];
  const ORGANISATION_NAMES = [...TEAM_NAMES, ...UNIT_NAMES];
  const organisationNames = r => [...new Set([...(cl(r).teams || []), ...(cl(r).units || [])])];
  function teamSummary(r) {
    const c = cl(r);
    const labels = [...(c.teams || []), ...(c.units || []).filter(u => u !== 'Regions & cities' || !c.teams?.length).map(u => u + ' (unit)')];
    return labels.length ? labels.join(' · ') : (c.units_status || c.teams_status) === 'buiten_scope' ? 'Geen aansluiting op dit aanbod' : 'Aansluiting nog te beoordelen';
  }
  const names = r => [...new Set((r.winnaars ?? [r.winnaar, ...(r.extra_winnaars || [])]).filter(Boolean))];
  const nlClock = new Intl.DateTimeFormat('en-GB', {timeZone: 'Europe/Amsterdam', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'});
  function wallTime(ms) {
    const p = Object.fromEntries(nlClock.formatToParts(ms).map(p => [p.type, p.value]));
    return Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
  }
  function deadline(value) {
    if (typeof value !== 'string') return null;
    const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})?$/.exec(value);
    if (!m) return null;
    const wall = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
    if (new Date(wall).toISOString().slice(0, 19) !== value.slice(0, 19)) return null;
    if (m[7]) { const ms = Date.parse(value); return Number.isFinite(ms) ? ms : null; }
    // Zonder offset geeft TenderNed Nederlandse lokale tijd, niet de browsertijdzone.
    const possibilities = [wall - 3600000, wall - 7200000].filter(ms => wallTime(ms) === wall);
    return possibilities.length ? Math.min(...possibilities) : null;
  }
  const LABELS = {open: 'Inschrijving open', closed: 'Inschrijving gesloten — uitkomst onbekend', unknown: 'Inschrijfstatus onbekend', awarded: 'Gegund', ended: 'Beëindigd'};
  function status(r, now = Date.now()) {
    if (r.publicatie_type === 'gegund') return 'awarded';
    if (r.is_vroegtijdige_beeindiging || r.koppeling_metadata?.vroegtijdig_beeindigd) return 'ended';
    const ms = deadline(r.sluitingsdatum);
    return ms === null ? 'unknown' : ms > now ? 'open' : 'closed';
  }
  const filters = [
    {id: 'teams', label: 'Team of unit', ai: true, primary: ['tenders']},
    {id: 'thema', label: 'Onderwerp', ai: true, primary: ['tenders', 'onderzoek']},
    {id: 'aanbestedende_dienst', label: 'Opdrachtgever', record: true, primary: ['tenders', 'onderzoek']},
    {id: 'winnaars', label: 'Winnaar', modes: ['onderzoek'], primary: ['onderzoek']},
    {id: 'status', label: 'Inschrijf- en gunningsstatus', modes: ['onderzoek']},
    {id: 'methodiek', label: 'Methodiek', ai: true, modes: ['onderzoek']},
    {id: 'procedure_type', label: 'Procedure', record: true, modes: ['tenders']},
    {id: 'evaluatiefase', label: 'Evaluatiefase', ai: true, modes: ['onderzoek']},
    {id: 'causaliteitsvraag', label: 'Causaal effect', ai: true, modes: ['onderzoek']}
  ];
  const filtersFor = mode => filters.filter(f => !f.modes || f.modes.includes(mode));
  const groups = filtersFor('onderzoek').filter(f => !['teams', 'winnaars'].includes(f.id));
  function field(r, id, now = Date.now()) {
    if (id === 'teams') return organisationNames(r);
    if (id === 'winnaars') return names(r);
    if (id === 'status') return LABELS[status(r, now)];
    if (id === 'jaar') return (r.publicatiedatum || '').slice(0, 4);
    if (id === 'europees') return r.europees === true ? 'Europees' : r.europees === false ? 'Nationaal' : 'Onbekend';
    const f = filters.find(f => f.id === id);
    const v = f?.record ? r[id] : cl(r)[id];
    return typeof v === 'boolean' ? (v ? 'Ja' : 'Nee') : v ?? 'Onbekend';
  }
  function searchText(r) {
    const c = cl(r);
    return norm([r.titel, r.omschrijving, r.aanbestedende_dienst, ...names(r), c.thema, c.subthema, c.methodiek, ...organisationNames(r), c.opdrachtprofiel?.vraag, c.opdrachtprofiel?.object, ...(c.opdrachtprofiel?.producten || [])].filter(Boolean).join(' '));
  }
  function defaults(mode) {
    return {filterRevision: 4, teamNotice: false, query: '', filters: {}, from: '', to: '', deadlineDays: '', tenderScope: 'open', sort: mode === 'tenders' ? 'deadline' : 'newest', limit: 50, scroll: 0, sub: 'uitvragen', group: 'thema', more: false, allBureaus: false, context: null, drill: null};
  }
  function sanitize(raw, mode) {
    const s = defaults(mode);
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return s;
    for (const key of ['query', 'from', 'to']) if (typeof raw[key] === 'string') s[key] = raw[key];
    for (const key of ['from', 'to']) if (s[key] && !/^\d{4}-\d{2}-\d{2}$/.test(s[key])) s[key] = '';
    if (s.from && s.to && s.from > s.to) s.from = s.to = '';
    if (['deadline', 'newest', 'title'].includes(raw.sort)) s.sort = raw.sort;
    if (mode === 'onderzoek' && s.sort === 'deadline') s.sort = 'newest';
    if (raw.tenderScope === 'unknown') s.tenderScope = 'unknown';
    if (s.tenderScope === 'unknown' && s.sort === 'deadline') s.sort = 'newest';
    if (['', '7', '30'].includes(raw.deadlineDays) && s.tenderScope === 'open') s.deadlineDays = raw.deadlineDays;
    if (['uitvragen', 'markt'].includes(raw.sub)) s.sub = raw.sub;
    const validGroup = groups.some(f => f.id === raw.group);
    if (validGroup) s.group = raw.group;
    for (const key of ['more', 'allBureaus']) if (typeof raw[key] === 'boolean') s[key] = raw[key];
    if (Number.isFinite(raw.limit)) s.limit = Math.max(50, Math.min(100000, Math.floor(raw.limit / 50) * 50));
    if (Number.isFinite(raw.scroll)) s.scroll = Math.max(0, raw.scroll);
    for (const f of filtersFor(mode)) {
      const v = raw.filters?.[f.id];
      if (v && Array.isArray(v.values)) {
        let values = [...new Set(v.values.filter(x => typeof x === 'string'))];
        if (f.id === 'teams') {
          const valid = values.filter(x => ORGANISATION_NAMES.includes(x));
          s.teamNotice = valid.length !== values.length || (raw.filterRevision !== 4 && values.length > 0);
          values = valid;
        }
        s.filters[f.id] = {values};
      }
    }
    s.teamNotice = s.teamNotice || raw.teamNotice === true;
    if (mode === 'onderzoek' && raw.context && typeof raw.context === 'object') {
      const c = raw.context;
      if (['buyer', 'similar'].includes(c.type) && typeof c.id === 'string' && typeof c.value === 'string' && (c.type !== 'buyer' || /^\d{4}-\d{2}-\d{2}$/.test(c.before))) {
        s.context = {type: c.type, id: c.id, value: c.value, before: c.before || ''};
      }
    }
    // Oude grafiekselecties kunnen nog de inmiddels verwijderde filters bevatten.
    if (raw.filterRevision === s.filterRevision && validGroup && mode === 'onderzoek' && raw.drill && typeof raw.drill.label === 'string' && Array.isArray(raw.drill.ids)) s.drill = {label: raw.drill.label, ids: raw.drill.ids.filter(id => typeof id === 'string')};
    return s;
  }
  function select(records, s, mode, now = Date.now(), textIndex = null) {
    const words = norm(s.query).trim().split(/\s+/).filter(Boolean);
    const ids = s.drill ? new Set(s.drill.ids) : null;
    return records.filter(r => {
      if (mode === 'tenders') {
        if (status(r, now) !== s.tenderScope) return false;
        if (s.deadlineDays && s.tenderScope === 'open' && deadline(r.sluitingsdatum) > now + +s.deadlineDays * 86400000) return false;
      }
      if (words.length && !words.every(w => (textIndex?.get(r.id) ?? searchText(r)).includes(w))) return false;
      if (mode === 'onderzoek') {
        const date = (r.publicatiedatum || '').slice(0, 10);
        if (s.from && (!date || date < s.from) || s.to && (!date || date > s.to)) return false;
        const c = s.context;
        if (c && (r.id === c.id || (c.type === 'buyer' ? r.aanbestedende_dienst !== c.value || !date || date >= c.before : cl(r).thema !== c.value))) return false;
        if (ids && !ids.has(r.id)) return false;
      }
      for (const f of filtersFor(mode)) {
        const filter = s.filters[f.id];
        if (!filter?.values?.length) continue;
        const value = field(r, f.id, now);
        if (!filter.values.some(v => (Array.isArray(value) ? value : [String(value)]).includes(v))) return false;
      }
      return true;
    });
  }
  function sort(records, choice) {
    return [...records].sort((a, b) => {
      let delta = 0;
      if (choice === 'deadline') delta = (deadline(a.sluitingsdatum) ?? Infinity) - (deadline(b.sluitingsdatum) ?? Infinity);
      else if (choice === 'title') delta = (a.titel || '').localeCompare(b.titel || '', 'nl');
      else delta = (b.publicatiedatum || '').localeCompare(a.publicatiedatum || '');
      return delta || String(a.id).localeCompare(String(b.id));
    });
  }
  function buckets(records, key, max = Infinity, now = Date.now()) {
    const map = new Map();
    for (const r of records) {
      let values = field(r, key, now);
      values = Array.isArray(values) ? values : [values];
      if (!values.length) values = ['Onbekend'];
      for (const v of new Set(values)) {
        const label = String(v || 'Onbekend');
        if (!map.has(label)) map.set(label, []);
        map.get(label).push(r.id);
      }
    }
    let entries = [...map].map(([label, ids]) => ({label, ids, count: ids.length})).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label, 'nl'));
    if (entries.length > max) entries = [...entries.slice(0, max), {label: 'Overig', ids: [...new Set(entries.slice(max).flatMap(e => e.ids))], count: entries.slice(max).reduce((n, e) => n + e.count, 0)}];
    return entries;
  }
  const api = {norm, cl, names, deadline, status, LABELS, TEAM_NAMES, UNIT_NAMES, ORGANISATION_NAMES, organisationNames, teamSummary, filters, filtersFor, groups, field, searchText, defaults, sanitize, select, sort, buckets};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.DashboardCore = api;
})(globalThis);
