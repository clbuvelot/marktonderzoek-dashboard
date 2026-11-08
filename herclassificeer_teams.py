"""Herbeoordeel alleen teamvelden, met hervatbare cache en controle vóór toepassing."""

import argparse
import copy
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
from classify import MODEL, FOUNDRY_RESOURCE, laad_document_tekst
from opslag import schrijf_json
from teamclassificatie import classificeer_teams, opdrachttekst, valideer_teams, versie, pas_bronreview_toe, organisatieveld


def fingerprint(record, document):
    inhoud = MODEL + '\n' + versie() + '\n' + opdrachttekst(record, document)
    return hashlib.sha256(inhoud.encode()).hexdigest()


def pas_toe(records, resultaten):
    """Houd alle aanbestedings-, koppel-, winnaars- en overige classificatievelden intact."""
    if set(resultaten) - {r['id'] for r in records}:
        raise ValueError('Teamresultaat voor een onbekende opdracht.')
    vernieuwd = copy.deepcopy(records)
    for r in vernieuwd:
        if r['id'] in resultaten:
            resultaat = resultaten[r['id']]
            valideer_teams(resultaat)
            if any(not organisatieveld(k) for k in resultaat):
                raise ValueError('Herclassificatie mag uitsluitend teamvelden wijzigen.')
            cl = r.setdefault('classificatie', {})
            # Oude review- en versiemetadata mogen geen nieuwe beoordeling beschrijven.
            for sleutel in list(cl):
                if organisatieveld(sleutel):
                    del cl[sleutel]
            cl.update(resultaat)
    return vernieuwd


def herbeoordeel(client, records, cache, batch_grootte=8, parallel=3):
    cache = Path(cache)
    documenten = {r['id']: laad_document_tekst(r['id'], vraag=r.get('titel', '') + ' ' + (r.get('omschrijving') or '')) for r in records}
    hashes = {r['id']: fingerprint(r, documenten[r['id']]) for r in records}
    resultaten, te_doen = {}, []
    for r in records:
        pad = cache / (hashes[r['id']] + '.json')
        if pad.exists():
            cl = json.loads(pad.read_text(encoding='utf-8'))
            valideer_teams(cl, opdrachttekst(r, documenten[r['id']]))
            resultaten[r['id']] = cl
        else:
            te_doen.append(r)
    print(f'{len(resultaten)} uit cache; {len(te_doen)} opnieuw beoordelen.', flush=True)

    def batchwerk(batch):
        for poging in range(3):
            try:
                uitslag = classificeer_teams(client, batch, documenten, MODEL)
                break
            except (ValueError, StopIteration) as exc:
                if poging == 2:
                    if len(batch) > 1:
                        print(f'Broncontrole vraagt een kleinere batch voor {len(batch)} opdrachten.', flush=True)
                        midden = len(batch) // 2
                        return {**batchwerk(batch[:midden]), **batchwerk(batch[midden:])}
                    raise
                print(f'Broncontrole: {type(exc).__name__}; batch van {len(batch)} wordt opnieuw beoordeeld.', flush=True)
            except anthropic.RateLimitError:
                if poging == 2:
                    raise
                print('API-limiet bereikt; deze batch wordt na 60 seconden hervat.', flush=True)
                time.sleep(60)
        result = {}
        for record, cl in zip(batch, uitslag):
            schrijf_json(str(cache / (hashes[record['id']] + '.json')), cl)
            result[record['id']] = cl
        return result

    batches = [te_doen[i:i + batch_grootte] for i in range(0, len(te_doen), batch_grootte)]
    fouten = []
    with ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = {pool.submit(batchwerk, b): b for b in batches}
        for klaar in as_completed(futures):
            try:
                resultaten.update(klaar.result())
            except Exception as exc:
                ids = [r['id'] for r in futures[klaar]]
                fouten.extend(ids)
                print(f'Batch nog niet afgerond ({type(exc).__name__}): {", ".join(ids)}. Andere batches gaan door.', flush=True)
            print(f'{len(resultaten)}/{len(records)} gecontroleerde teambeoordelingen.', flush=True)
    if fouten:
        raise RuntimeError(f'{len(fouten)} opdrachten nog niet afgerond; hervat de run vanuit de bewaarde cache.')
    return {r['id']: pas_bronreview_toe(r, resultaten[r['id']], documenten[r['id']]) for r in records}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--invoer', default='data/geclassificeerd_met_winnaar.json')
    p.add_argument('--uitvoer', default='data/.cache/teams/dataset-voorstel.json')
    p.add_argument('--ids', help='Optionele proefselectie van komma-gescheiden publicatie-id\'s')
    p.add_argument('--batch-grootte', type=int, default=8)
    p.add_argument('--parallel', type=int, default=3)
    p.add_argument('--alleen-aansluiting', action='store_true',
                   help='Herbeoordeel passende en onduidelijke uitvragen; behoud ongewijzigde buiten-scopebeslissingen')
    p.add_argument('--toepassen', action='store_true', help='Pas de volledige herbeoordeling toe en vernieuw exports')
    args = p.parse_args()
    if args.toepassen and args.ids:
        p.error('Pas alleen een volledige herbeoordeling toe.')
    if args.ids and args.alleen_aansluiting:
        p.error('Combineer geen proefselectie met alleen-aansluiting.')
    if not 1 <= args.batch_grootte <= 12 or not 1 <= args.parallel <= 8:
        p.error('Batchgrootte moet 1-12 zijn en parallel 1-8.')
    pad = Path(args.invoer)
    origineel = pad.read_bytes()
    records = json.loads(origineel)
    selectie = records
    if args.alleen_aansluiting:
        selectie = [r for r in records if (r.get('classificatie') or {}).get('units_status',
                     (r.get('classificatie') or {}).get('teams_status')) != 'buiten_scope']
        if any((r.get('classificatie') or {}).get('teams_versie') != versie() for r in records):
            p.error('Gewijzigde team- of unitregels vereisen een volledige herbeoordeling; laat --alleen-aansluiting weg.')
    if args.ids:
        ids = set(args.ids.split(','))
        selectie = [r for r in records if r['id'] in ids]
        if len(selectie) != len(ids):
            p.error('Proefselectie bevat onbekende id\'s.')
    client = anthropic.AnthropicFoundry(resource=FOUNDRY_RESOURCE,
                                       api_key=os.environ['AZURE_FOUNDRY_API_KEY'], timeout=240)
    resultaten = herbeoordeel(client, selectie, 'data/.cache/teams/antwoorden', args.batch_grootte, args.parallel)
    vernieuwd = pas_toe(records, resultaten)
    if not args.ids:
        from controleer_teamherindeling import controleer
        controleer(records, vernieuwd)
    schrijf_json(args.uitvoer, vernieuwd)
    print(f'Voorstel opgeslagen: {args.uitvoer}', flush=True)
    if args.toepassen:
        if len(resultaten) != len(selectie) or pad.read_bytes() != origineel:
            raise ValueError('Dataset veranderde of niet alle opdrachten zijn beoordeeld.')
        digest = hashlib.sha256(origineel).hexdigest()[:12]
        backup = Path('data/.cache/teams') / f'dataset-voor-herindeling-{digest}.json'
        if not backup.exists():
            schrijf_json(str(backup), records)
        schrijf_json(str(pad), vernieuwd)
        from analyse import vernieuw_exports
        vernieuw_exports(vernieuwd)
        print('Teamvelden toegepast; exports vernieuwd.', flush=True)


if __name__ == '__main__':
    main()
