"""
update.py
---------
Incrementele update van de aanbestedingspipeline.

Controleert welke TenderNed publicaties nieuw zijn sinds de vorige run,
verwerkt alleen die records door de pipeline, en mergt ze in de bestaande
JSON-bestanden.

Gebruik:
    python update.py
    python update.py --dry-run          # toon wat nieuw is, sla niets op

Leest:
    data/update_staat.json              # metadata vorige run
    data/geclassificeerd_met_winnaar.json

Schrijft:
    data/geclassificeerd.json
    data/geclassificeerd_met_winnaar.json
    data/update_staat.json              # bijgewerkt na succesvolle run
"""

import json
import os
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta

from collect import normalize_tenderned, dedupliceer, RELEVANTE_CPV_CODES, TENDERNED_TNS_BASE
from classify import classificeer_alle, BATCH_GROOTTE
from opschonen import verwerk as opschoon_verwerk

DATA_DIR = "data"
STAAT_BESTAND        = os.path.join(DATA_DIR, "update_staat.json")
GECLASSIFICEERD      = os.path.join(DATA_DIR, "geclassificeerd.json")
MET_WINNAAR          = os.path.join(DATA_DIR, "geclassificeerd_met_winnaar.json")


def laad_staat() -> dict:
    if os.path.exists(STAAT_BESTAND):
        with open(STAAT_BESTAND, encoding="utf-8") as f:
            return json.load(f)
    return {"laatste_update": None, "bekende_ids": [], "totaal_records": 0}


def sla_staat_op(staat: dict):
    with open(STAAT_BESTAND, "w", encoding="utf-8") as f:
        json.dump(staat, f, ensure_ascii=False, indent=2)
    print(f"  → {STAAT_BESTAND}")


def laad_bestaand(pad: str) -> list[dict]:
    if not os.path.exists(pad):
        return []
    with open(pad, encoding="utf-8") as f:
        return json.load(f)


def sla_op(records: list[dict], pad: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(pad, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  → {pad} ({len(records)} records)")


def fetch_nieuw(datum_filter: str | None, bekende_ids: set) -> list[dict]:
    if not datum_filter:
        datum_filter = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        print(f"  Eerste run — ophalen vanaf {datum_filter}")

    print(f"\n[TenderNed] Ophalen publicaties vanaf {datum_filter}")
    resultaten = {}

    for cpv in RELEVANTE_CPV_CODES:
        for pub_type in ["AGO", "AAO"]:
            pagina = 0
            while True:
                params = {
                    "cpvCodes": cpv,
                    "publicatieDatumVanaf": datum_filter,
                    "publicatieType": pub_type,
                    "page": pagina,
                    "size": 50,
                }
                try:
                    r = requests.get(TENDERNED_TNS_BASE, params=params, timeout=30)
                    r.raise_for_status()
                    data = r.json()
                    batch = data.get("contents", data.get("content", []))
                    if not batch:
                        break
                    for item in batch:
                        pub_id = str(item.get("publicatieId", item.get("id", "")))
                        if pub_id and pub_id not in bekende_ids:
                            item["_publicatie_type"] = "gegund" if pub_type == "AGO" else "lopend"
                            resultaten[pub_id] = item
                    if len(batch) < 50:
                        break
                    pagina += 1
                    time.sleep(0.3)
                except Exception as e:
                    print(f"  [FOUT] CPV {cpv} {pub_type} p{pagina}: {e}")
                    break

    records = list(resultaten.values())
    print(f"  {len(records)} nieuwe publicaties gevonden")
    return records


def verwerk_nieuw(ruw: list[dict]) -> list[dict]:
    genormaliseerd = [normalize_tenderned(r) for r in ruw]
    genormaliseerd = dedupliceer(genormaliseerd)
    genormaliseerd = [r for r in genormaliseerd if r.get("titel") or r.get("omschrijving")]
    if not genormaliseerd:
        return []
    print(f"\n[Classificatie] {len(genormaliseerd)} records...")
    geclassificeerd = classificeer_alle(genormaliseerd, BATCH_GROOTTE)
    geclassificeerd, tellers = opschoon_verwerk(geclassificeerd)
    print(f"  Opgeschoond: {tellers}")
    return geclassificeerd


def merge(bestaand: list[dict], nieuw: list[dict]) -> list[dict]:
    lookup = {r.get("id"): r for r in bestaand}
    overschreven = sum(1 for r in nieuw if r.get("id") in lookup)
    for r in nieuw:
        lookup[r.get("id")] = r
    if overschreven:
        print(f"  {overschreven} records bijgewerkt (bijv. AAO → AGO)")
    resultaat = list(lookup.values())
    resultaat.sort(key=lambda r: r.get("publicatiedatum", ""), reverse=True)
    return resultaat


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    nu = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=== Incrementele update ===")
    staat = laad_staat()
    bekende_ids = set(staat.get("bekende_ids", []))
    datum_filter = staat["laatste_update"][:10] if staat.get("laatste_update") else None

    print(f"Vorige update : {staat.get('laatste_update') or 'nog nooit'}")
    print(f"Bekende IDs   : {len(bekende_ids)}")

    nieuwe_ruw = fetch_nieuw(datum_filter, bekende_ids)

    if not nieuwe_ruw:
        print("\nGeen nieuwe publicaties. Dataset is up-to-date.")
        staat["laatste_update"] = nu
        if not args.dry_run:
            sla_staat_op(staat)
        return

    if args.dry_run:
        print(f"\n[DRY RUN] {len(nieuwe_ruw)} nieuwe publicaties gevonden, niets opgeslagen.")
        for r in nieuwe_ruw[:10]:
            print(f"  - {r.get('aanbestedingNaam', '?')[:80]}")
        return

    nieuw_verwerkt = verwerk_nieuw(nieuwe_ruw)
    if not nieuw_verwerkt:
        print("Geen bruikbare records na verwerking.")
        return

    print(f"\n[Merge]")
    bestaand = laad_bestaand(MET_WINNAAR) or laad_bestaand(GECLASSIFICEERD)
    gemerged = merge(bestaand, nieuw_verwerkt)

    print(f"\n[Opslaan]")
    sla_op(gemerged, MET_WINNAAR)
    sla_op(gemerged, GECLASSIFICEERD)

    staat["bekende_ids"] = list(bekende_ids | {r.get("id") for r in nieuw_verwerkt if r.get("id")})
    staat["laatste_update"] = nu
    staat["totaal_records"] = len(gemerged)
    sla_staat_op(staat)

    print(f"\n=== Klaar ===")
    print(f"Nieuw verwerkt  : {len(nieuw_verwerkt)}")
    print(f"Totaal dataset  : {len(gemerged)}")
    print(f"Timestamp       : {nu}")


if __name__ == "__main__":
    main()
