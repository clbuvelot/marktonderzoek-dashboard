"""Controleer broncitaten en behoud van niet-teamgegevens; schrijf een volledige vergelijking."""
import argparse
import copy
import csv
import json
from collections import Counter
from pathlib import Path

from opslag import schrijf_json
from teamclassificatie import valideer_teams, versie, organisatieveld


def zonder_teams(record):
    r = copy.deepcopy(record)
    c = r.get('classificatie', {})
    for k in list(c):
        if organisatieveld(k):
            del c[k]
    return r


def controleer(oud, nieuw, root=Path('.')):
    if [r['id'] for r in oud] != [r['id'] for r in nieuw]:
        raise ValueError('Records of volgorde gewijzigd.')
    for a, b in zip(oud, nieuw):
        if zonder_teams(a) != zonder_teams(b):
            raise ValueError(f"Niet-teamgegevens gewijzigd voor {a['id']}.")
        c = b['classificatie']
        if c.get('teams_versie') != versie() and c != a['classificatie']:
            raise ValueError(f"Verouderde teamregels voor {b['id']}.")
        doc = root / 'data' / 'documenten' / f"{b['id']}.txt"
        bron = b['titel'] + '\n' + (b.get('omschrijving') or '')
        if doc.exists():
            bron += '\n' + doc.read_text(encoding='utf-8')
        # Tegen de volledige oorspronkelijke bron: geen fragmentkoppen als bewijs.
        valideer_teams(c, bron)
    return {
        'opdrachten': len(nieuw), 'teamversie': versie(),
        'versies': dict(Counter(r['classificatie']['teams_versie'] for r in nieuw)),
        'ongewijzigd_overgenomen': sum(a['classificatie'] == b['classificatie'] for a, b in zip(oud, nieuw)),
        'status': dict(Counter(r['classificatie']['teams_status'] for r in nieuw)),
        'teams': dict(Counter(t for r in nieuw for t in r['classificatie']['teams'])),
        'units': dict(Counter(u for r in nieuw for u in r['classificatie'].get('units', []))),
        'unitstatus': dict(Counter(r['classificatie'].get('units_status') for r in nieuw)),
        'meerdere_units': sum(len(r['classificatie'].get('units', [])) > 1 for r in nieuw),
        'voorheen_zonder_aansluiting_nu_met': sum(not (a['classificatie'].get('units') or a['classificatie']['teams'])
                                                and bool(b['classificatie'].get('units')) for a, b in zip(oud, nieuw)),
        'voorheen_met_aansluiting_nu_zonder': sum(bool(a['classificatie'].get('units') or a['classificatie']['teams'])
                                                and not b['classificatie'].get('units') for a, b in zip(oud, nieuw)),
        'voorheen_zonder_nu_met_team': sum(not a['classificatie']['teams'] and bool(b['classificatie']['teams']) for a, b in zip(oud, nieuw)),
        'voorheen_met_nu_zonder_team': sum(bool(a['classificatie']['teams']) and not b['classificatie']['teams'] for a, b in zip(oud, nieuw)),
        'meerdere_teams': sum(len(r['classificatie']['teams']) > 1 for r in nieuw),
        'aantal_teams_per_uitvraag': dict(Counter(len(r['classificatie']['teams']) for r in nieuw)),
        'niet_teamvelden_ongewijzigd': True, 'alle_teamcitaten_in_oorspronkelijke_bron': True,
    }


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('voor')
    p.add_argument('na')
    args = p.parse_args()
    oud = json.loads(Path(args.voor).read_text(encoding='utf-8'))
    nieuw = json.loads(Path(args.na).read_text(encoding='utf-8'))
    uitslag = controleer(oud, nieuw)
    schrijf_json('data/teamherindeling.json', uitslag)
    pad = Path('docs/teamherindeling.csv')
    with pad.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'titel', 'voor', 'na', 'units', 'status', 'unitstatus', 'vraag', 'producten', 'toelichting'])
        writer.writeheader()
        for a, b in zip(oud, nieuw):
            c = b['classificatie']
            writer.writerow({'id': b['id'], 'titel': b['titel'], 'voor': ' | '.join(a['classificatie']['teams']),
                             'na': ' | '.join(c['teams']), 'units': ' | '.join(c.get('units', [])),
                             'status': c['teams_status'], 'unitstatus': c.get('units_status', ''),
                             'vraag': c.get('opdrachtprofiel', {}).get('vraag', ''),
                             'producten': ' | '.join(c.get('opdrachtprofiel', {}).get('producten', [])),
                             'toelichting': c['teams_toelichting']})
    print(json.dumps(uitslag, ensure_ascii=False, indent=2))
