import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import classify
import teamclassificatie as teams
from herclassificeer_teams import pas_toe, fingerprint, herbeoordeel
from leer_teamprofielen import laad_profielen
from documentfragmenten import selecteer_document
from controleer_teamherindeling import controleer as controleer_herindeling


def passend(team='Evaluaties', bewijs='Verkeersonderzoek'):
    return teams.verwerk_teamafwegingen(beoordeling([team], bewijs))


def beoordeling(gekozen, bewijs='Verkeersonderzoek', units=()):
    return {'teams_afwegingen': afwegingen(gekozen, bewijs),
            'units_afwegingen': [{'unit': u, 'oordeel': 'passend' if u in units else 'niet_passend',
                                 'reden': 'Inhoudelijke scope.', 'bewijs': bewijs if u in units else ''}
                                for u in teams.unitnamen()],
            'opdrachtprofiel': {'vraag': bewijs, 'object': 'Onderzoek', 'producten': ['Rapport']},
            'teams_toelichting': 'Aansluiting op concrete onderzoeksvragen.'}


def afwegingen(gekozen, bewijs='Verkeersonderzoek'):
    return [{'team': t, 'oordeel': 'passend' if t in gekozen else 'niet_passend',
             'reden': 'Aansluiting op concrete expertise.' if t in gekozen else 'Dit onderwerp ontbreekt in het aanbod.',
             'bewijs': bewijs if t in gekozen else ''} for t in teams.teamnamen()]


class TeamTests(unittest.TestCase):
    def test_onbekende_of_dubbele_teams_worden_geweigerd(self):
        for namen in [['TIM'], ['Regions & cities'], ['Economie & innovatie'], ['Evaluaties', 'Evaluaties'], [1]]:
            with self.subTest(namen=namen), self.assertRaises(ValueError):
                teams.valideer_teams({**passend(), 'teams': namen})

    def test_status_en_bewijs_moeten_bij_teamkeuze_horen(self):
        for wijziging in [{'teams_status': 'buiten_scope'}, {'teams_matches': []},
                          {'teams_matches': [{'team': 'Social and Health', 'reden': 'x', 'bewijs': 'x'}]},
                          {'teams_toelichting': ''}]:
            with self.subTest(wijziging=wijziging), self.assertRaises(ValueError):
                teams.valideer_teams({**passend(), **wijziging})
        for status in ['onduidelijk', 'buiten_scope']:
            teams.valideer_teams({'teams': [], 'teams_status': status, 'teams_matches': [],
                                 'teams_toelichting': 'Geen inhoudelijke aansluiting vastgesteld.'})

    def test_letterlijk_bronbewijs_geen_verzonnen_of_ander_citaat(self):
        teams.valideer_teams(passend(bewijs='Verkeersonderzoek met tellingen'), 'Verkeersonderzoek\n  met tellingen')
        with self.assertRaises(ValueError):
            teams.valideer_teams(passend(), 'Raamovereenkomst catering')

    def test_herbeoordeling_verandert_alleen_teamvelden(self):
        records = [{'id': '1', 'titel': 'Verkeersonderzoek', 'winnaars': ['Ecorys', 'Partner'],
                    'gunning_koppelingen': [{'bron': 'officieel'}], 'documenten_geladen': True,
                    'classificatie': {'teams': [], 'teams_review': {'datum': 'oud'}, 'thema': 'Mobiliteit', 'methodiek': 'MKBA', 'toelichting': 'Oud'}},
                   {'id': '2', 'classificatie': {'teams': ['TIM']}}]
        origineel = copy.deepcopy(records)
        result = pas_toe(records, {'1': passend()})
        self.assertEqual(records, origineel)
        self.assertEqual(result[1], records[1])
        self.assertNotIn('teams_review', result[0]['classificatie'])
        for k, v in records[0].items():
            if k != 'classificatie':
                self.assertEqual(v, result[0][k])
        for k in ['thema', 'methodiek', 'toelichting']:
            self.assertEqual(records[0]['classificatie'][k], result[0]['classificatie'][k])
        with self.assertRaises(ValueError):
            pas_toe(records, {'1': {**passend(), 'winnaars': []}})
        with self.assertRaises(ValueError):
            pas_toe(records, {'3': passend()})

    def test_cache_vervalt_bij_gewijzigde_bron_of_teamregels(self):
        r = {'titel': 'Verkeersonderzoek', 'omschrijving': 'Tellingen'}
        a = fingerprint(r, 'document')
        self.assertNotEqual(a, fingerprint(r, 'gewijzigd document'))
        self.assertNotEqual(a, fingerprint({**r, 'omschrijving': 'Aanleg'}, 'document'))
        with patch.object(teams, 'TEAM_REGELS', teams.TEAM_REGELS + ' extra regel'):
            self.assertNotEqual(a, fingerprint(r, 'document'))

    def test_batch_behoudt_juiste_record_bij_omgekeerde_indices(self):
        records = [{'id': '1', 'titel': 'Verkeersonderzoek'}, {'id': '2', 'titel': 'Onderwijsmonitor'}]
        outputs = [{**passend('Brede Welvaart', 'Onderwijsmonitor'), 'index': 1,
                    'teams_afwegingen': afwegingen(['Brede Welvaart'], 'Onderwijsmonitor')},
                   {**passend(), 'index': 0, 'teams_afwegingen': afwegingen(['Evaluaties'])}]
        client = Mock()
        client.messages.create.return_value = SimpleNamespace(stop_reason='end_turn',
            content=[SimpleNamespace(type='text', text=json.dumps(outputs))])
        result = teams.classificeer_teams(client, records, {}, 'testmodel')
        self.assertEqual([r['teams'] for r in result], [['Evaluaties'], ['Brede Welvaart']])
        outputs[0]['teams_afwegingen'][1]['bewijs'] = 'Verkeersonderzoek'
        client.messages.create.return_value.content[0].text = json.dumps(outputs)
        with self.assertRaises(ValueError):
            teams.classificeer_teams(client, records, {}, 'testmodel')

    def test_nieuwe_pipeline_gebruikt_dezelfde_regels_en_geen_oude_aannames(self):
        prompt = classify.bouw_classificatieprompt([{'id': '1', 'titel': 'Test'}])
        self.assertIn(teams.TEAM_REGELS, prompt)
        self.assertIn(teams.TEAM_SCHEMA, prompt)
        for naam in teams.teamnamen():
            self.assertIn(naam, prompt)
        self.assertNotIn('ALTIJD voor: inhuurcontracten', prompt)
        self.assertNotIn('inhoudelijk best passende team', prompt)

    def test_alle_passende_teams_tellen_ook_bij_hetzelfde_onderdeel(self):
        for aantal in (2, 3, 4):
            with self.subTest(aantal=aantal):
                gekozen = teams.teamnamen()[:aantal]
                raw = {**beoordeling(gekozen, 'Gedeelde onderzoeksvraag'), 'teams_afwegingen': list(reversed(afwegingen(gekozen, 'Gedeelde onderzoeksvraag'))),
                       'teams_status': 'passend', 'teams_toelichting': 'Meerdere afdelingen sluiten aan.'}
                result = teams.verwerk_teamafwegingen(raw)
                teams.valideer_teams(result, 'Gedeelde onderzoeksvraag')
                self.assertEqual(result['teams'], gekozen)
                self.assertEqual(len(result['teams_matches']), aantal)

    def test_iedere_afdeling_moet_een_eigen_beoordeling_hebben(self):
        alle = afwegingen(['Evaluaties'])
        for waarden in [alle[:-1], alle + [alle[0]], alle[:-1] + [alle[0]],
                        [{**a, 'oordeel': 'beste'} for a in alle]]:
            with self.subTest(waarden=waarden), self.assertRaises(ValueError):
                teams.verwerk_teamafwegingen({**beoordeling(['Evaluaties']), 'teams_afwegingen': waarden})

    def test_passend_team_mag_niet_uit_de_uiteindelijke_selectie_verdwijnen(self):
        c = teams.verwerk_teamafwegingen({**passend(), 'teams_afwegingen': afwegingen(['Evaluaties', 'Brede Welvaart'])})
        c['teams'] = ['Evaluaties']
        c['teams_matches'] = [m for m in c['teams_matches'] if m['team'] == 'Evaluaties']
        with self.assertRaises(ValueError):
            teams.valideer_teams(c)

    def test_lege_administratieve_en_placeholderprofielen_tellen_niet_mee(self):
        with tempfile.TemporaryDirectory() as d:
            basis = {'team': 'NL5400', 'team_naam': 'TIM', 'samenvatting': 'Inhoudelijk onderzoek. ' * 20}
            for i, aanvulling in enumerate([
                {'titel': 'Verkeersonderzoek'}, {'titel': 'Indieningsrapport van een aanbesteding'},
                {'titel': 'Rapport', 'samenvatting': ''},
                {'titel': 'Rapport', 'samenvatting': 'Deze tekst bevat geen inhoudelijke samenvatting. ' * 10}]):
                (Path(d) / f'{i}.json').write_text(json.dumps({**basis, 'project_id': str(i), **aanvulling}), encoding='utf-8')
            geldig, ongeldig = laad_profielen(d)
            self.assertEqual([r['project_id'] for r in geldig], ['0'])
            self.assertEqual(len(ongeldig), 3)

    def test_inhoud_voorbij_technische_bijlagen_wordt_gevonden(self):
        techniek = ('De opdrachtnemer moet een beveiligd surveyplatform leveren.\n' * 400)
        voorblad = ('Algemene informatie over de aanbestedingsprocedure.\n' * 300)
        relevant = ('Onderzoeksvragen: hoe ervaren cliënten en werkgevers de menselijke maat? '
                    'Doelgroep: uitkeringsgerechtigden. Meting van vertrouwen en dienstverlening.\n')
        bron = ('=== Document: technische contractbijlage ===\n' + techniek
                + '\n=== Document: beschrijvend document ===\n' + voorblad + relevant)
        selectie = selecteer_document(bron, 'Menselijke Maat Monitor', 8000)
        self.assertIn(relevant.strip(), selectie)
        self.assertIn('beschrijvend document', selectie)
        self.assertLessEqual(len(selectie), 8000)

    def test_korte_bron_blijft_letterlijk_en_selectie_is_deterministisch(self):
        bron = 'Een korte opdrachtomschrijving.'
        self.assertEqual(selecteer_document(bron, 'opdracht'), bron)
        lang = 'Nog een paragraaf over mobiliteit. ' * 1000
        self.assertEqual(selecteer_document(lang, 'mobiliteit'), selecteer_document(lang, 'mobiliteit'))

    def test_logistieke_trefwoorden_verdringen_inhoudelijke_opdrachtbeschrijving_niet(self):
        inhoud = ('Inhoudsopgave\n2.1 Beschrijving en doel van de opdracht ........ 9\n'
                  + 'Algemene aanbestedingsinformatie.\n' * 200
                  + '\n2.1 Beschrijving en doel van de opdracht\n'
                  + 'Ontwikkel inhoudelijke training over energiebeleid, systeemintegratie en energiemarkten.\n' * 35
                  + '\n3. Uitvoering\n'
                  + 'Raamovereenkomst trainingsprogramma inschrijver economisch voordelige inschrijving logistieke organisatie.\n' * 600)
        bron = '=== Document: beschrijvend document ===\n' + inhoud
        selectie = selecteer_document(bron, 'Raamovereenkomst trainingsprogramma economisch voordelige inschrijving logistieke organisatie', 16000)
        self.assertIn('Ontwikkel inhoudelijke training over energiebeleid', selectie)
        self.assertLessEqual(len(selectie), 16000)

    def test_bronreview_geldt_alleen_voor_de_gecontroleerde_bron_en_catalogus(self):
        r = {'id': '1', 'titel': 'Verkeersonderzoek'}
        review = {'bronhash': teams.bronhash(r, ''), 'teamversie': teams.versie(),
                  'datum': '2026-11-08', 'reden': 'Broncontrole', 'classificatie': passend()}
        oud = {'teams': [], 'thema': 'Mobiliteit'}
        with patch.object(teams, 'bronreviews', return_value={'1': review}):
            nieuw = teams.pas_bronreview_toe(r, oud, '')
            self.assertEqual(nieuw['teams'], ['Evaluaties'])
            self.assertEqual(nieuw['thema'], 'Mobiliteit')
            self.assertEqual(teams.pas_bronreview_toe(r, oud, 'Nieuwe scope'), oud)
            review['teamversie'] = 'verouderd'
            self.assertEqual(teams.pas_bronreview_toe(r, oud, ''), oud)

    def test_ongewijzigde_scope_mag_oude_versie_houden_gewijzigde_beoordeling_niet(self):
        r = {'id': 'synthetische-scope', 'titel': 'Verkeersonderzoek',
             'classificatie': {**passend(), 'teams_versie': 'teams-v2-historisch'}}
        uitslag = controleer_herindeling([r], [copy.deepcopy(r)])
        self.assertEqual(uitslag['ongewijzigd_overgenomen'], 1)
        gewijzigd = copy.deepcopy(r)
        gewijzigd['classificatie']['teams_toelichting'] = 'Een nieuwe afweging.'
        with self.assertRaises(ValueError):
            controleer_herindeling([r], [gewijzigd])

    def test_nieuwe_versie_vergt_volledige_beoordeling_en_vraagprofiel(self):
        for veld in ('teams_afwegingen', 'units_afwegingen', 'opdrachtprofiel'):
            c = {**passend(), 'teams_versie': teams.versie()}
            del c[veld]
            with self.subTest(veld=veld), self.assertRaises(ValueError):
                teams.valideer_teams(c)

    def test_team_impliceert_unit_en_unit_impliceert_geen_team(self):
        c = teams.verwerk_teamafwegingen(beoordeling(['Evaluaties']))
        self.assertEqual(c['units'], ['Regions & cities'])
        teams.valideer_teams(c, 'Verkeersonderzoek')
        c['units'] = []
        with self.assertRaises(ValueError):
            teams.valideer_teams(c)
        c = teams.verwerk_teamafwegingen(beoordeling([], units=['TIM']))
        self.assertEqual(c['teams'], [])
        self.assertEqual(c['teams_status'], 'buiten_scope')
        self.assertEqual(c['units_status'], 'passend')
        teams.valideer_teams(c, 'Verkeersonderzoek')

    def test_onduidelijk_blijft_onderscheiden_van_niet_passend(self):
        c = beoordeling([])
        c['teams_afwegingen'][1]['oordeel'] = 'onduidelijk'
        result = teams.verwerk_teamafwegingen(c)
        self.assertEqual(result['teams'], [])
        self.assertEqual(result['teams_status'], 'onduidelijk')
        self.assertEqual(result['units_status'], 'buiten_scope')

    def test_ieder_unit_oordeel_en_letterlijk_unitbewijs_zijn_verplicht(self):
        raw = beoordeling([], units=['TIM'])
        with self.assertRaises(ValueError):
            teams.verwerk_teamafwegingen({**raw, 'units_afwegingen': raw['units_afwegingen'][:-1]})
        result = teams.verwerk_teamafwegingen(raw)
        with self.assertRaises(ValueError):
            teams.valideer_teams(result, 'Andere bron')
        result['units'] = ['Evaluaties']
        with self.assertRaises(ValueError):
            teams.valideer_teams(result)

    def test_tien_matches_kunnen_naast_elkaar_bestaan(self):
        result = teams.verwerk_teamafwegingen(beoordeling(teams.teamnamen(), units=teams.unitnamen()))
        self.assertEqual(len(result['teams']), 4)
        self.assertEqual(len(result['units']), 6)
        teams.valideer_teams(result, 'Verkeersonderzoek')

    def test_lastige_batch_wordt_kleiner_en_geldige_resultaten_zijn_hervatbaar(self):
        records = [{'id': str(i), 'titel': 'Verkeersonderzoek'} for i in range(2)]
        def antwoord(_client, batch, _documenten, _model):
            if len(batch) > 1:
                raise ValueError('Citaat ongeldig in grote batch.')
            return [{**passend(), 'teams_versie': teams.versie()}]
        with tempfile.TemporaryDirectory() as d, patch('herclassificeer_teams.laad_document_tekst', return_value=''), \
                patch('herclassificeer_teams.classificeer_teams', side_effect=antwoord) as api, patch('builtins.print'):
            result = herbeoordeel(None, records, d, batch_grootte=2, parallel=1)
            self.assertEqual(set(result), {'0', '1'})
            api.reset_mock()
            self.assertEqual(herbeoordeel(None, records, d), result)
            api.assert_not_called()

    def test_mislukte_batch_verliest_geen_geslaagde_beoordelingen(self):
        records = [{'id': str(i), 'titel': 'Verkeersonderzoek'} for i in range(2)]
        def antwoord(_client, batch, _documenten, _model):
            if batch[0]['id'] == '0':
                raise ValueError('Onvoldoende bronbewijs.')
            return [{**passend(), 'teams_versie': teams.versie()}]
        with tempfile.TemporaryDirectory() as d, patch('herclassificeer_teams.laad_document_tekst', return_value=''), \
                patch('herclassificeer_teams.classificeer_teams', side_effect=antwoord), patch('builtins.print'):
            with self.assertRaises(RuntimeError):
                herbeoordeel(None, records, d, batch_grootte=1, parallel=2)
            self.assertTrue((Path(d) / (fingerprint(records[1], '') + '.json')).exists())


if __name__ == '__main__':
    unittest.main()
