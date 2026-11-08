"""UX-regressies in Edge. Lokale brondata; alle AI-verzoeken worden gesimuleerd."""
import functools
import os
import json
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / 'data/.cache/ux'
ARTIFACTS.mkdir(parents=True, exist_ok=True)
DATA = json.loads(Path(os.environ.get('DASHBOARD_TEST_DATA', str(ROOT / 'data/geclassificeerd_met_winnaar.json'))).read_text(encoding='utf-8'))

class Handler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass

def count(page, number):
    expect(page.locator('#s-totaal')).to_have_text(str(number))

def choose(page, field, value):
    detail = page.locator(f'#fi-{field}')
    if not detail.is_visible():
        page.locator('#more-filters > summary').click()
    if detail.get_attribute('open') is None:
        detail.locator('summary').click()
    detail.get_by_role('checkbox', name=value, exact=True).check()
    page.locator('h1').click()

def fixture_records():
    base = {'titel': 'Evaluatie arbeidsmarkt', 'aanbestedende_dienst': 'Ministerie BZK', 'publicatie_type': 'lopend', 'publicatiedatum': '2026-11-01', 'sluitingsdatum': '2026-11-09T13:00:00', 'winnaars': [], 'documenten_geladen': False, 'omschrijving': 'Onderzoek naar beleid en uitvoering.', 'classificatie': {'teams': ['Evaluaties'], 'units': ['Social and Health', 'Regions & cities'], 'units_matches': [], 'teams_status': 'passend', 'teams_toelichting': 'Onderzoek naar arbeidsmarktbeleid.', 'teams_matches': [{'team': 'Evaluaties', 'reden': 'Arbeidsmarkt en re-integratie.', 'bewijs': 'Onderzoek naar beleid en uitvoering.'}], 'thema': 'Arbeidsmarkt', 'methodiek': 'Interviews', 'thema_betrouwbaarheid': .8}}
    def r(rid, **extra):
        return {**base, 'id': rid, **extra}
    return [r('101', documenten_geladen=True), r('102', sluitingsdatum=''), r('103', sluitingsdatum='2026-11-08T13:00:00'), r('104', publicatie_type='gegund', winnaars=['Partner', 'Ecorys Nederland B.V.'], publicatiedatum='2025-01-01'), r('105', is_vroegtijdige_beeindiging=True), r('106', sluitingsdatum='2026-12-24T13:00:00'), r('107', publicatie_type='gegund'), *[r(str(200+i), titel='Onderzoek '+str(i), publicatie_type='gegund', classificatie={'thema': 'Onderwerp '+str(i)}) for i in range(13)]]

server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Handler, directory=str(ROOT)))
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f'http://127.0.0.1:{server.server_port}/dashboard.html'
errors = []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(channel='msedge', headless=True)
        ctx = browser.new_context(viewport={'width': 1440, 'height': 1050}, timezone_id='Europe/Amsterdam')
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(json=DATA))
        ctx.route('**/anthropic/v1/messages', lambda route: route.fulfill(json={'content': [{'type': 'text', 'text': 'Gesimuleerde documentsamenvatting.'}]}))
        page = ctx.new_page()
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.goto(BASE)
        expect(page.locator('#login-overlay')).to_have_class('login-overlay open')
        page.evaluate('document.fonts.ready')
        assert page.evaluate("document.fonts.check('600 16px Montserrat')")
        page.screenshot(path=str(ARTIFACTS / 'login-desktop.png'))
        page.evaluate("sessionStorage.setItem('azure_key', 'lokale-browsertest')")
        page.reload()
        expect(page.locator('h1')).to_have_text('Vind je volgende onderzoek')
        assert page.locator('.logo img').evaluate('(el) => el.complete && el.naturalWidth > 0')
        expected_open = page.evaluate("DATA.filter(r => C.status(r) === 'open').length")
        count(page, expected_open)
        expect(page.locator('#primary-filters #fi-aanbestedende_dienst')).to_be_visible()
        expect(page.locator('#primary-filters #fi-teams')).to_be_visible()
        assert page.locator('#dd-teams input[type=checkbox]').count() == 10
        assert page.locator('#dd-teams .is-team').count() == 4
        page.locator('#fi-teams > summary').click()
        page.locator('#find-teams').fill('energie')
        expect(page.locator('#dd-teams .filter-option:visible')).to_have_count(1)
        expect(page.locator('#dd-teams .filter-option:visible')).to_contain_text('Sustainable Transitions')
        page.locator('#find-teams').fill('')
        page.screenshot(path=str(ARTIFACTS / 'teamselectie-v4.png'))
        page.locator('#fi-teams > summary').click()
        choose(page, 'teams', 'Social and Health')
        count(page, page.evaluate("DATA.filter(r => C.status(r) === 'open' && (cl(r).units || []).includes('Social and Health')).length"))
        page.locator('#active-tags [data-action=reset]').click()
        count(page, expected_open)
        assert page.locator('#extra-filters .filter-item').evaluate_all('(els) => els.map(el => el.id)') == ['fi-procedure_type']
        for removed in ['methodiek_kwantitatief', 'europees', 'type_opdracht_omschrijving', 'herhalingskans', 'consortiumkans', 'opdrachtgrootte', 'methodiek', 'evaluatiefase', 'causaliteitsvraag']:
            assert page.locator('#fi-' + removed).count() == 0
        assert page.locator('[data-confidence]').count() == 0
        assert page.locator('.chart-card').count() == 0
        page.screenshot(path=str(ARTIFACTS / 'tenders-desktop.png'))
        first_id = page.locator('[data-record]').first.get_attribute('data-record')
        page.locator('[data-record]').first.click()
        expect(page.locator('.detail-page')).to_be_visible()
        expect(page.locator('.team-detail')).to_be_visible()
        page.locator('[data-action=related-buyer]').click()
        expect(page.locator('.context-note')).to_be_visible()
        assert page.evaluate("C.select(DATA, state(), app.mode).every(r => r.id !== state().context.id && r.publicatiedatum.slice(0,10) < state().context.before)")
        page.go_back()
        expect(page.locator('.detail-page')).to_be_visible()
        page.locator('[data-action=back]').click()
        expect(page.locator(f'[data-record="{first_id}"]')).to_be_visible()
        count(page, expected_open)
        page.locator('#nav-onderzoek').click()
        count(page, len(DATA))
        assert page.locator('#extra-filters .filter-item').evaluate_all('(els) => els.map(el => el.id)') == ['fi-teams', 'fi-status', 'fi-methodiek', 'fi-evaluatiefase', 'fi-causaliteitsvraag']
        assert page.locator('#fi-procedure_type').count() == 0
        # Dezelfde warmte-uitvraag is bij beide teams vindbaar en telt gezamenlijk één keer.
        for team in ['Sustainable Transitions', 'Regions & cities']:
            choose(page, 'teams', team)
            page.locator('#search').fill('Haalbaarheidsonderzoek aanleg warmtenet Kort Haarlem')
            count(page, 1)
            expect(page.locator('[data-record="337554"]')).to_be_visible()
            page.locator('#active-tags [data-action=reset]').click()
        choose(page, 'teams', 'Sustainable Transitions')
        choose(page, 'teams', 'Regions & cities')
        page.locator('#search').fill('Haalbaarheidsonderzoek aanleg warmtenet Kort Haarlem')
        count(page, 1)
        page.locator('[data-record="337554"]').click()
        expect(page.locator('.team-detail > .team-match h3')).to_contain_text(['Sustainable Transitions'])
        page.locator('.unit-matches > summary').click()
        expect(page.locator('.unit-match h3')).to_contain_text(['Natural Resources', 'Regions & cities'])
        expect(page.locator('.assignment-profile')).to_contain_text('De vraag')
        page.locator('.team-detail').screenshot(path=str(ARTIFACTS / 'gedeelde-teamaansluiting.png'))
        page.go_back()
        count(page, 1)
        page.locator('#active-tags [data-action=reset]').click()
        page.locator('#search').fill('Ecorys')
        count(page, page.evaluate("DATA.filter(r => C.searchText(r).includes('ecorys')).length"))
        page.locator('[data-action=reset]').click()
        choose(page, 'winnaars', 'Ecorys Nederland B.V.')
        count(page, 6)
        page.locator('[data-record="335914"]').click()
        expect(page.locator('a[href*="/overzicht/365375"]')).to_be_visible()
        expect(page.locator('.detail-highlight > strong')).to_contain_text('Ecorys')
        page.locator('.detail-highlight .winner-list > summary').press('Enter')
        expect(page.locator('.detail-highlight .winner-list ul')).to_be_visible()
        page.locator('.detail-highlight .winner-list > summary').click()
        page.screenshot(path=str(ARTIFACTS / 'opdracht-desktop.png'))
        page.go_back()
        count(page, 6)
        page.locator('#nav-tenders').click()
        count(page, expected_open)
        assert page.locator('#fi-winnaars').count() == 0
        page.locator('#nav-onderzoek').click()
        count(page, 6)
        page.reload()
        count(page, 6)
        expect(page.locator('#nav-onderzoek')).to_have_attribute('aria-current', 'page')
        page.goto(BASE)
        count(page, 6)
        expect(page.locator('#nav-onderzoek')).to_have_attribute('aria-current', 'page')
        page.locator('[data-action=reset]').click()
        choose(page, 'winnaars', 'Panteia B.V.')
        count(page, 17)
        page.locator('.sub-nav a[href="#onderzoek/markt"]').click()
        count(page, 17)
        group = page.locator('#distribution-links [data-action=drill]').first
        expected_drill = int(group.locator('.count').inner_text())
        group.click()
        count(page, expected_drill)
        page.locator('[data-action=back-market]').click()
        count(page, 17)
        expect(page.locator('#distribution')).to_be_visible()
        page.locator('[data-action=reset]').click()
        count(page, len(DATA))
        page.locator('#alle-bureaus').click()
        assert page.locator('#conc-body tr').count() == len({n for r in DATA for n in r.get('winnaars', [])})
        page.evaluate('window.scrollTo(0, 0)')
        page.screenshot(path=str(ARTIFACTS / 'markt-desktop.png'))
        page.locator('[data-action=view-list]').click()
        page.locator('[data-action=more]').click()
        expect(page.locator('.opportunity')).to_have_count(100)
        page.locator('.opportunity').nth(60).locator('[data-record]').click()
        page.go_back()
        expect(page.locator('.opportunity')).to_have_count(100)
        page.wait_for_function('window.scrollY > 0')
        page.set_viewport_size({'width': 390, 'height': 844})
        page.locator('#nav-tenders').click()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        expect(page.locator('#dataset-update')).to_be_visible()
        expect(page.locator('.mobile-deadline').first).to_be_visible()
        page.screenshot(path=str(ARTIFACTS / 'tenders-mobiel.png'))
        page.locator('[data-record]').first.click()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.screenshot(path=str(ARTIFACTS / 'opdracht-mobiel.png'))
        page.go_back()
        page.locator('#nav-onderzoek').click()
        page.locator('.sub-nav a[href="#onderzoek/markt"]').click()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        for width in [320, 768]:
            page.set_viewport_size({'width': width, 'height': 900})
            # Chart.js past de canvasbreedte asynchroon aan na een viewportwissel.
            page.wait_for_function('document.documentElement.scrollWidth <= innerWidth')
        page.set_viewport_size({'width': 390, 'height': 844})
        page.evaluate('window.scrollTo(0, 0)')
        page.screenshot(path=str(ARTIFACTS / 'markt-mobiel.png'))
        ctx.close()
        print('Actuele data: navigatie, 6 Ecorys / 17 Panteia, grafieken, alle bureaus, terugkeren en mobiel OK', flush=True)

        fixture = fixture_records()
        ctx = browser.new_context(viewport={'width': 1280, 'height': 900}, timezone_id='America/New_York')
        ctx.add_init_script("sessionStorage.setItem('azure_key', 'test'); localStorage.setItem('ecorys-dashboard-ux-v1', '{ongeldig');")
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(json=fixture))
        ctx.route('**/data/documenten/101.txt', lambda route: route.fulfill(body='Onderzoek naar beleidsuitvoering.', content_type='text/plain'))
        ctx.route('**/anthropic/v1/messages', lambda route: route.fulfill(json={'content': [{'type': 'text', 'text': 'Geteste synthese <script>ongeldig</script>'}]}))
        page = ctx.new_page()
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.clock.install(time=datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc))
        page.goto(BASE)
        count(page, 2)
        page.locator('#deadline-days').select_option('7')
        count(page, 1)
        page.locator('#fi-teams > summary').focus()
        page.keyboard.press('Enter')
        expect(page.locator('#dd-teams')).to_be_visible()
        page.keyboard.press('Escape')
        expect(page.locator('#dd-teams')).not_to_be_visible()
        expect(page.locator('#fi-teams > summary')).to_be_focused()
        page.locator('[data-action=scope][data-value=unknown]').click()
        count(page, 1)
        assert page.locator('[data-record="102"]').count() == 1
        assert page.locator('#deadline-days').count() == 0
        page.locator('#nav-onderzoek').click()
        count(page, len(fixture))
        page.locator('#search').fill('ecorys evaluatie')
        count(page, 1)
        page.locator('[data-action=reset]').click()
        page.locator('#date-from').fill('2025-01-01')
        page.locator('#date-to').fill('2025-12-31')
        count(page, 1)
        page.locator('[data-action=reset]').click()
        page.locator('.sub-nav a[href="#onderzoek/markt"]').click()
        other = page.locator('#distribution-links [data-action=drill]').filter(has_text='Overig')
        other_count = int(other.locator('.count').inner_text())
        other.click()
        count(page, other_count)
        assert sorted(page.evaluate('state().drill.ids')) == sorted(page.locator('.opportunity').evaluate_all('(els) => els.map(el => el.dataset.id)'))
        page.goto(BASE + '#tenders/opdracht/101')
        expect(page.locator('.team-match h3')).to_contain_text('Evaluaties')
        expect(page.locator('.team-match blockquote')).to_contain_text('Onderzoek naar beleid en uitvoering.')
        page.locator('.team-detail').screenshot(path=str(ARTIFACTS / 'teamaansluiting.png'))
        page.locator('[data-action=chat]').click()
        expect(page.locator('#chat-synthese')).to_contain_text('Geteste synthese', timeout=10000)
        assert page.locator('#chat-synthese script').count() == 0
        page.keyboard.press('Escape')
        expect(page.locator('#chat-overlay')).not_to_have_class('modal-overlay open')
        expect(page.locator('[data-action=chat]')).to_be_focused()
        ctx.route('**/data/documenten/101.txt', lambda route: route.fulfill(status=404))
        page.locator('[data-action=chat]').click()
        expect(page.locator('#chat-synthese')).to_contain_text('Geen aanbestedingsdocumenten beschikbaar')
        page.keyboard.press('Escape')
        page.clock.set_system_time(datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc))
        page.clock.run_for(60000)
        expect(page.locator('.detail-heading > .status')).to_have_text('Inschrijving gesloten — uitkomst onbekend')
        page.goto(BASE + '#onderzoek/uitvragen/opdracht/999999')
        expect(page.locator('h1')).to_have_text('Opdracht niet gevonden')
        page.locator('[data-action=back]').click()
        count(page, other_count)
        ctx.close()
        print('Fixtures: deadlines, onbekend, periode, Overig, directe links en gesimuleerde documentvragen OK', flush=True)

        # Een bestaande gebruiker mag niet door verwijderde filters beperkt blijven.
        ctx = browser.new_context()
        legacy = {'version': 1, 'mode': 'onderzoek', 'states': {'onderzoek': {
            'query': 'evaluatie', 'group': 'consortiumkans',
            'filters': {'teams': {'values': ['Evaluaties']}, 'thema': {'values': ['Arbeidsmarkt'], 'confidence': 'laag'},
                        'consortiumkans': {'values': ['Ja']}, 'herhalingskans': {'values': ['hoog']}},
            'drill': {'label': 'Consortiumkans Ja', 'ids': ['101']}}}}
        ctx.add_init_script("sessionStorage.setItem('azure_key', 'test'); localStorage.setItem('ecorys-dashboard-ux-v1', JSON.stringify(" + json.dumps(legacy) + "));")
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(json=fixture))
        page = ctx.new_page()
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.goto(BASE)
        count(page, 7)
        assert page.evaluate("state().filters.teams.values") == ['Evaluaties']
        expect(page.locator('.team-notice')).to_contain_text('teams binnen Regions')
        page.locator('[data-action=dismiss-team-notice]').click()
        expect(page.locator('.team-notice')).to_have_count(0)
        choose(page, 'teams', 'Social and Health')
        count(page, 7)
        assert page.evaluate('state().drill') is None
        assert page.locator('[data-confidence]').count() == 0
        assert 'Consortiumkans' not in page.locator('#active-tags').inner_text()
        page.locator('.sub-nav a[href="#onderzoek/markt"]').click()
        count(page, 7)
        group_values = page.locator('#group option').evaluate_all('(els) => els.map(el => el.value)')
        assert not set(group_values) & {'consortiumkans', 'herhalingskans', 'opdrachtgrootte', 'methodiek_kwantitatief', 'europees', 'type_opdracht_omschrijving', 'procedure_type'}
        ctx.close()
        print('Teams en units behouden bij migratie; onbekende labels vervallen', flush=True)

        ctx = browser.new_context()
        ctx.add_init_script("sessionStorage.setItem('azure_key', 'test'); Object.defineProperty(window, 'localStorage', {get() {throw new Error('opslag geblokkeerd')}});")
        ctx.route('**/chart.umd.js', lambda route: route.abort())
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(json=fixture))
        page = ctx.new_page()
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.goto(BASE + '#onderzoek/markt')
        count(page, len(fixture))
        expect(page.locator('.chart-fallback').first).to_be_visible()
        page.locator('#distribution-links [data-action=drill]').first.click()
        expect(page.locator('.opportunity').first).to_be_visible()
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(status=503))
        page.reload()
        expect(page.locator('h1')).to_have_text('De opdrachten konden niet worden geladen')
        ctx.route('**/data/geclassificeerd_met_winnaar.json', lambda route: route.fulfill(json=fixture))
        page.locator('[data-action=retry]').click()
        expect(page.locator('#results')).to_be_visible()
        ctx.close()
        assert not errors, errors
        browser.close()
        print(json.dumps({'browser': 'Edge', 'javascript_fouten': errors, 'screenshots': str(ARTIFACTS), 'resultaat': 'OK'}))
finally:
    server.shutdown()
