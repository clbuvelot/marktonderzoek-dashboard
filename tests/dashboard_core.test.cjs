const {test} = require('node:test');
const assert = require('node:assert/strict');
const C = require('../dashboard-core.js');
const now = Date.parse('2026-11-08T12:00:00Z');
const row = (id, extra = {}) => ({id, titel: 'Évaluatie arbeidsmarkt', publicatie_type: 'lopend', publicatiedatum: '2026-10-01', sluitingsdatum: '2026-11-09T13:00:00', classificatie: {thema: 'Arbeidsmarkt', teams: ['Evaluaties'], units: ['Regions & cities', 'Social and Health'], thema_betrouwbaarheid: .8}, ...extra});

test('Nederlandse winter- en zomertijd zijn onafhankelijk van browsertijdzone', () => {
  assert.equal(C.deadline('2026-01-15T09:00:00'), Date.parse('2026-01-15T08:00:00Z'));
  assert.equal(C.deadline('2026-07-15T09:00:00'), Date.parse('2026-07-15T07:00:00Z'));
  assert.equal(C.deadline('2026-07-15T09:00:00+03:00'), Date.parse('2026-07-15T06:00:00Z'));
  assert.equal(C.deadline('2026-10-25T02:30:00'), Date.parse('2026-10-25T00:30:00Z'));
});
test('ontbrekende en ongeldige deadlines worden niet gesloten genoemd', () => {
  for (const value of ['', undefined, 'fout', '2026-02-30T09:00:00', '2026-03-29T02:30:00']) {
    assert.equal(C.deadline(value), null);
    assert.equal(C.status(row('1', {sluitingsdatum: value}), now), 'unknown');
  }
});
test('gunning en beëindiging gaan vóór deadline; exact op deadline is gesloten', () => {
  assert.equal(C.status(row('1'), now), 'open');
  assert.equal(C.status(row('1', {sluitingsdatum: '2026-11-08T13:00:00'}), now), 'closed');
  assert.equal(C.status(row('1', {is_vroegtijdige_beeindiging: true}), now), 'ended');
  assert.equal(C.status(row('1', {koppeling_metadata: {vroegtijdig_beeindigd: true}}), now), 'ended');
  assert.equal(C.status(row('1', {publicatie_type: 'gegund', is_vroegtijdige_beeindiging: true}), now), 'awarded');
});
test('zoekwoorden combineren verschillende velden en vinden mede-winnaars', () => {
  const records = [row('1', {aanbestedende_dienst: 'Ministerie BZK', winnaars: ['Partner', 'Ecorys Nederland B.V.']})];
  const s = C.defaults('onderzoek'); s.query = 'ecorys EVALUATIE bzk';
  assert.equal(C.select(records, s, 'onderzoek', now).length, 1);
  s.query += ' ontbreekt'; assert.equal(C.select(records, s, 'onderzoek', now).length, 0);
});
test('actuele tenders sluiten gesloten, gegunde en beëindigde uitvragen uit', () => {
  const records = [row('open'), row('closed', {sluitingsdatum: '2020-01-01T10:00:00'}), row('unknown', {sluitingsdatum: ''}), row('ended', {is_vroegtijdige_beeindiging: true}), row('awarded', {publicatie_type: 'gegund'})];
  const s = C.defaults('tenders');
  assert.deepEqual(C.select(records, s, 'tenders', now).map(r => r.id), ['open']);
  s.tenderScope = 'unknown'; assert.deepEqual(C.select(records, s, 'tenders', now).map(r => r.id), ['unknown']);
  assert.equal(C.select(records, C.defaults('onderzoek'), 'onderzoek', now).length, 5);
});
test('deadlinevensters zijn beperkt tot de gekozen open inschrijvingen', () => {
  const s = C.defaults('tenders'); s.deadlineDays = '7';
  assert.deepEqual(C.select([row('soon'), row('later', {sluitingsdatum: '2026-12-01T10:00:00'})], s, 'tenders', now).map(r => r.id), ['soon']);
});
test('filters combineren OR en AND zonder oude AI-scorekeuzes toe te passen', () => {
  const s = C.defaults('onderzoek');
  s.filters = {thema: {values: ['Arbeidsmarkt', 'Wonen'], confidence: 'hoog'}, winnaars: {values: ['Ecorys']}};
  const records = [row('1', {winnaars: ['Partner', 'Ecorys']}), row('2', {winnaars: ['Ecorys'], classificatie: {thema: 'Arbeidsmarkt', thema_betrouwbaarheid: .2}}), row('3', {winnaars: ['Partner']})];
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['1', '2']);
  // Een oud winnaarfilter mag nooit onzichtbaar de tenderselectie beperken.
  assert.equal(C.select([row('1')], s, 'tenders', now).length, 1);
});
test('onderzoek beperkt op uitvraagdatum en behoudt de bronrecords', () => {
  const records = [row('1', {publicatiedatum: '2025-12-31'}), row('2'), row('3', {publicatiedatum: '2026-10-31'})];
  const before = JSON.stringify(records), s = C.defaults('onderzoek'); s.from = '2026-10-01'; s.to = '2026-10-31';
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['2', '3']);
  C.sort(records, 'newest'); assert.equal(JSON.stringify(records), before);
});
test('contextacties zoeken eerdere opdrachten van dezelfde opdrachtgever en hetzelfde onderwerp', () => {
  const records = [row('origin', {aanbestedende_dienst: 'BZK'}), row('earlier', {aanbestedende_dienst: 'BZK', publicatiedatum: '2025-01-01'}), row('other', {aanbestedende_dienst: 'VWS'})];
  const s = C.defaults('onderzoek'); s.context = {type: 'buyer', id: 'origin', value: 'BZK', before: '2026-10-01'};
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['earlier']);
  s.context = {type: 'similar', id: 'origin', value: 'Arbeidsmarkt'};
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['earlier', 'other']);
});
test('grafiek Overig verwijst naar exact de onderliggende records', () => {
  const records = Array.from({length: 14}, (_, i) => row(String(i), {classificatie: {thema: 'Thema ' + i}}));
  const groups = C.buckets(records, 'thema', 10), other = groups.at(-1);
  assert.equal(other.label, 'Overig'); assert.equal(other.count, 4);
  const s = C.defaults('onderzoek'); s.drill = other;
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id).sort(), [...other.ids].sort());
});
test('mede-winnaars tellen eenmaal per opdracht en blijven individueel doorzoekbaar', () => {
  const r = row('1', {winnaars: ['Ecorys', 'Partner', 'Ecorys']});
  assert.deepEqual(C.buckets([r], 'winnaars').map(g => g.count), [1, 1]);
  assert.deepEqual(C.names({winnaar: 'Ecorys', extra_winnaars: ['Partner']}), ['Ecorys', 'Partner']);
});
test('beschadigde of oude browserinstellingen vallen terug op bruikbare waarden', () => {
  assert.deepEqual(C.sanitize(null, 'tenders'), C.defaults('tenders'));
  const s = C.sanitize({query: 'Ecorys', limit: -1, sort: 'magic', from: 'fout', filters: {winnaars: {values: ['Ecorys']}, teams: {values: ['Evaluaties', 4]}}}, 'tenders');
  assert.equal(s.limit, 50); assert.equal(s.sort, 'deadline'); assert.equal(s.from, '');
  assert.equal(s.filters.winnaars, undefined); assert.deepEqual(s.filters.teams.values, ['Evaluaties']); assert.equal(s.teamNotice, true);
});

test('bewaarde verwijderde filters en oude grafiekselecties verbergen geen opdrachten', () => {
  const raw = {...C.defaults('onderzoek'), query: 'evaluatie', group: 'consortiumkans', filterRevision: 1,
    filters: {teams: {values: ['Evaluaties']}, thema: {values: ['Arbeidsmarkt'], confidence: 'laag'},
      consortiumkans: {values: ['Ja']}, herhalingskans: {values: ['hoog']}, opdrachtgrootte: {values: ['Groot']},
      methodiek_kwantitatief: {values: ['Ja']}, europees: {values: ['Europees']}, type_opdracht_omschrijving: {values: ['Diensten']}},
    drill: {label: 'Oude selectie', ids: ['1']}};
  const before = JSON.stringify(raw), clean = C.sanitize(raw, 'onderzoek');
  assert.deepEqual(Object.keys(clean.filters), ['teams', 'thema']);
  assert.deepEqual(clean.filters.teams.values, ['Evaluaties']); assert.equal(clean.teamNotice, true);
  assert.equal(clean.filters.thema.confidence, undefined);
  assert.equal(clean.query, 'evaluatie'); assert.equal(clean.group, 'thema'); assert.equal(clean.drill, null);
  assert.equal(C.select([row('1'), row('2')], clean, 'onderzoek', now).length, 2);
  assert.equal(JSON.stringify(raw), before);
});

test('actuele grafiekselecties blijven na migratie intact bij terugnavigeren', () => {
  const current = {...C.defaults('onderzoek'), group: 'thema', drill: {label: 'Arbeidsmarkt', ids: ['1']}};
  const clean = C.sanitize(current, 'onderzoek');
  assert.deepEqual(clean.drill, current.drill);
  assert.deepEqual(C.select([row('1'), row('2')], clean, 'onderzoek', now).map(r => r.id), ['1']);
});

test('specialistische filters gelden alleen in de toepasselijke werkwijze', () => {
  const records = [row('1', {procedure_type: 'Openbaar', classificatie: {...row('1').classificatie, methodiek: 'Mixed methods'}}),
    row('2', {procedure_type: 'Niet-openbaar', classificatie: {...row('2').classificatie, methodiek: 'Procesevaluatie'}})];
  const s = C.defaults('tenders'); s.filters = {procedure_type: {values: ['Openbaar']}, methodiek: {values: ['Procesevaluatie']}};
  assert.deepEqual(C.select(records, s, 'tenders', now).map(r => r.id), ['1']);
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['2']);
  assert.equal(C.sanitize(s, 'tenders').filters.methodiek, undefined);
  assert.equal(C.sanitize(s, 'onderzoek').filters.procedure_type, undefined);
});

test('teamcatalogus en dashboard onderscheiden vier teams en zes units', () => {
  const catalog = require('../teamprofielen.json');
  assert.deepEqual(C.TEAM_NAMES, catalog.teams.map(t => t.naam));
  assert.deepEqual(C.UNIT_NAMES, catalog.units.map(u => u.naam));
  assert.ok(!C.TEAM_NAMES.includes('TIM'));
  assert.ok(!C.TEAM_NAMES.includes('Regions & cities'));
});

test('herkenbare teams en units blijven behouden; onbekende labels vervallen', () => {
  const raw = {...C.defaults('tenders'), filters: {teams: {values: ['TIM', 'Evaluaties', 'Brede Welvaart', 'Social and Health', 'Onbekend label']}}};
  const s = C.sanitize(raw, 'tenders');
  assert.deepEqual(s.filters.teams.values, ['TIM', 'Evaluaties', 'Brede Welvaart', 'Social and Health']);
  assert.equal(s.teamNotice, true);
  s.teamNotice = false;
  assert.equal(C.sanitize(s, 'tenders').teamNotice, false);
});

test('meerdere teams zijn vindbaar en lege toewijzingen houden hun betekenis', () => {
  const records = [row('1', {classificatie: {teams: ['Brede Welvaart', 'Sustainable Transitions'], units: ['TIM', 'Regions & cities']}}), row('2')];
  const s = C.defaults('onderzoek'); s.filters.teams = {values: ['Brede Welvaart']};
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['1']);
  s.filters.teams.values = ['TIM'];
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['1']);
  s.filters.teams.values = ['TIM', 'Brede Welvaart', 'Sustainable Transitions'];
  assert.deepEqual(C.select(records, s, 'onderzoek', now).map(r => r.id), ['1']);
  assert.equal(C.teamSummary({classificatie: {teams: [], teams_status: 'buiten_scope'}}), 'Geen aansluiting op dit aanbod');
  assert.equal(C.teamSummary({classificatie: {teams: [], teams_status: 'onduidelijk'}}), 'Aansluiting nog te beoordelen');
});

test('alle vier teams blijven selecteerbaar zonder dubbeltelling van een gedeelde uitvraag', () => {
  const records = [row('shared', {classificatie: {teams: C.TEAM_NAMES}}), row('other', {classificatie: {teams: []}})];
  const s = C.defaults('tenders'); s.filters.teams = {values: C.TEAM_NAMES};
  assert.deepEqual(C.select(records, s, 'tenders', now).map(r => r.id), ['shared']);
  for (const team of C.TEAM_NAMES) assert.ok(C.teamSummary(records[0]).includes(team));
});
