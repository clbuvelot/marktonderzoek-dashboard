"""
update.py
---------
Incrementele dagelijkse update van de aanbestedingspipeline.

Checkt de laatste updatedatum, haalt nieuwe publicaties op sinds dat moment,
draait de volledige pipeline (collect → classify → opschonen) voor die records,
en mergt ze in de bestaande dataset.

Gebruik:
    python update.py
    python update.py --dry-run

Leest/schrijft:
    data/update_staat.json
    data/geclassificeerd_met_winnaar.json
    data/documenten/{id}.txt
"""

import json
import os
import time
import argparse
import requests
from datetime import datetime, timezone, timedelta

from collect import (
    normalize_tenderned,
    haal_alle_documenten_op,
    RELEVANTE_CPV_CODES,
    TENDERNED_TNS_BASE,
    _tenderned_request,
    DOCUMENTEN_DIR,
)
from classify import classificeer_alle, BATCH_GROOTTE
from opschonen import verwerk as opschoon_verwerk

DATA_DIR = "data"
STAAT_BESTAND = os.path.join(DATA_DIR, "update_staat.json")
DATASET = os.path.join(DATA_DIR, "geclassificeerd_met_winnaar.json")


# ── Staat ─────────────────────────────────────────────────────────────────────

def laad_staat():
    if os.path.exists(STAAT_BESTAND):
        with open(STAAT_BESTAND, encoding="utf-8") as f:
            return json.load(f)
    return {"laatste_update": None, "totaal_records": 0}


def sla_staat_op(staat):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STAAT_BESTAND, "w", encoding="utf-8") as f:
        json.dump(staat, f, ensure_ascii=False, indent=2)


def laad_dataset():
    if not os.path.exists(DATASET):
        return []
    with open(DATASET, encoding="utf-8") as f:
        return json.load(f)


def sla_dataset_op(records):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DATASET, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


# ── Ophalen nieuwe publicaties ────────────────────────────────────────────────

def fetch_nieuw(datum_vanaf, bekende_kenmerken):
    """
    Haalt nieuwe publicaties op:
    1. AAOs (uitvragen) sinds datum_vanaf
    2. AGOs (gunningen) sinds datum_vanaf, alleen als er een matching AAO is
    """
    print(f"\n[1/5] Ophalen nieuwe uitvragen (AAO) vanaf {datum_vanaf}")
    aao_per_kenmerk = {}

    for cpv in RELEVANTE_CPV_CODES:
        pagina = 0
        while True:
            params = {
                "cpvCodes": cpv,
                "publicatieDatumVanaf": datum_vanaf,
                "publicatieType": "AAO",
                "page": pagina,
                "size": 50,
            }
            try:
                batch, totaal = _tenderned_request(params, label=f"AAO {cpv} p{pagina}")
            except Exception as e:
                print(f"  [FOUT] {e}")
                break
            if not batch:
                break
            for item in batch:
                pub_id = str(item.get("id", item.get("publicatieId", "")))
                kenmerk = str(item.get("kenmerk", "") or pub_id)
                if kenmerk not in bekende_kenmerken:
                    item["_publicatie_type"] = "lopend"
                    aao_per_kenmerk[kenmerk] = item
            if len(batch) < 50:
                break
            pagina += 1
            time.sleep(0.3)

    print(f"  {len(aao_per_kenmerk)} nieuwe uitvragen gevonden")

    if not aao_per_kenmerk:
        return []

    # AGOs ophalen en koppelen aan nieuwe AAOs
    print(f"\n[2/5] Ophalen gunningen (AGO) vanaf {datum_vanaf}")
    gekoppeld = 0

    for cpv in RELEVANTE_CPV_CODES:
        pagina = 0
        while True:
            params = {
                "cpvCodes": cpv,
                "publicatieDatumVanaf": datum_vanaf,
                "publicatieType": "AGO",
                "page": pagina,
                "size": 50,
            }
            try:
                batch, totaal = _tenderned_request(params, label=f"AGO {cpv} p{pagina}")
            except Exception as e:
                print(f"  [FOUT] {e}")
                break
            if not batch:
                break
            for item in batch:
                kenmerk = str(item.get("kenmerk", "") or "")
                if kenmerk and kenmerk in aao_per_kenmerk:
                    item["_publicatie_type"] = "gegund"
                    aao = aao_per_kenmerk[kenmerk]
                    item["_aao_publicatiedatum"] = aao.get("publicatieDatum", "")
                    aao_per_kenmerk[kenmerk] = item
                    gekoppeld += 1
            if len(batch) < 50:
                break
            pagina += 1
            time.sleep(0.3)

    print(f"  {gekoppeld} gunningen gekoppeld")

    records = list(aao_per_kenmerk.values())
    print(f"  Totaal nieuwe records: {len(records)}")
    return records


# ── Pipeline ──────────────────────────────────────────────────────────────────

def verwerk_nieuw(ruw):
    """Normaliseert, haalt documenten op, classificeert en schoont op."""
    genormaliseerd = [normalize_tenderned(r) for r in ruw]
    genormaliseerd = [r for r in genormaliseerd if r.get("titel") or r.get("omschrijving")]
    print(f"  {len(genormaliseerd)} records na normalisatie")

    if not genormaliseerd:
        return []

    # Documenten ophalen
    print(f"\n[3/5] Documenten ophalen voor {len(genormaliseerd)} records...")
    os.makedirs(DOCUMENTEN_DIR, exist_ok=True)
    for i, r in enumerate(genormaliseerd):
        pub_id = r.get("id", "")
        if not pub_id:
            continue
        pad = os.path.join(DOCUMENTEN_DIR, f"{pub_id}.txt")
        if os.path.exists(pad):
            r["documenten_geladen"] = True
            continue
        tekst = haal_alle_documenten_op(pub_id)
        r["documenten_geladen"] = bool(tekst.strip())
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(genormaliseerd)} verwerkt...")
        time.sleep(0.3)

    # Classificeren
    print(f"\n[4/5] Classificeren ({len(genormaliseerd)} records)...")
    geclassificeerd = classificeer_alle(genormaliseerd, BATCH_GROOTTE)

    # Opschonen
    print(f"\n[5/5] Opschonen...")
    geclassificeerd, tellers = opschoon_verwerk(geclassificeerd)
    print(f"  {tellers}")

    return geclassificeerd


# ── Merge ─────────────────────────────────────────────────────────────────────

def merge(bestaand, nieuw):
    """Voegt nieuwe records toe. Bij kenmerk-conflict wint nieuw."""
    lookup = {}
    for r in bestaand:
        key = r.get("tenderned_kenmerk") or r.get("id")
        lookup[key] = r

    bijgewerkt = 0
    toegevoegd = 0
    for r in nieuw:
        key = r.get("tenderned_kenmerk") or r.get("id")
        if key in lookup:
            bijgewerkt += 1
        else:
            toegevoegd += 1
        lookup[key] = r

    resultaat = sorted(lookup.values(), key=lambda r: r.get("publicatiedatum", ""), reverse=True)
    print(f"  {toegevoegd} nieuwe, {bijgewerkt} bijgewerkt")
    return resultaat


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    nu = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=== Incrementele update ===")

    staat = laad_staat()
    laatste = staat.get("laatste_update")

    if laatste:
        datum_vanaf = laatste[:10]
    else:
        datum_vanaf = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    print(f"Vorige update: {laatste or 'nog nooit'}")
    print(f"Ophalen vanaf: {datum_vanaf}")

    bestaand = laad_dataset()
    bekende_kenmerken = {
        r.get("tenderned_kenmerk") for r in bestaand if r.get("tenderned_kenmerk")
    }
    print(f"Bestaande dataset: {len(bestaand)} records")

    nieuwe_ruw = fetch_nieuw(datum_vanaf, bekende_kenmerken)

    if not nieuwe_ruw:
        print("\nGeen nieuwe publicaties. Dataset is up-to-date.")
        staat["laatste_update"] = nu
        if not args.dry_run:
            sla_staat_op(staat)
        return

    if args.dry_run:
        print(f"\n[DRY RUN] {len(nieuwe_ruw)} nieuwe publicaties, niets opgeslagen.")
        return

    nieuw_verwerkt = verwerk_nieuw(nieuwe_ruw)

    if not nieuw_verwerkt:
        print("Geen bruikbare records na verwerking.")
        staat["laatste_update"] = nu
        sla_staat_op(staat)
        return

    print(f"\nMerge...")
    gemerged = merge(bestaand, nieuw_verwerkt)

    print(f"\nOpslaan...")
    sla_dataset_op(gemerged)
    print(f"  → {DATASET} ({len(gemerged)} records)")

    staat["laatste_update"] = nu
    staat["totaal_records"] = len(gemerged)
    sla_staat_op(staat)

    print(f"""
=== Klaar ===
Nieuwe records  : {len(nieuw_verwerkt)}
Totaal dataset  : {len(gemerged)}
Timestamp       : {nu}
""")


if __name__ == "__main__":
    main()
