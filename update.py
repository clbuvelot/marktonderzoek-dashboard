"""
update.py
---------
Incrementele dagelijkse update van de aanbestedingspipeline.

Checkt de laatste updatedatum, haalt nieuwe publicaties op sinds dat moment,
draait de volledige pipeline (collect → classify → opschonen) voor die records,
en mergt ze in de bestaande dataset.

Haalt daarnaast los van de nieuwe uitvragen de gunningen van de afgelopen
30 dagen op en zet bestaande records daarmee op gegund. De winnaarsnaam zit
niet in de TNS API — die komt uit de bulk Excel via vul_winnaars_aan.py.

Gebruik:
    python update.py
    python update.py --dry-run
    python update.py --gunningen-vanaf 2026-01-01   # inhaalslag gunningen

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

# Hoever elke run terugkijkt naar gunningen. Ruimer dan het venster voor
# uitvragen, zodat een late publicatie of een mislukte run zich vanzelf herstelt.
GUNNING_TERUGBLIK_DAGEN = 30


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

def _haal_publicaties(datum_vanaf, publicatie_type, label):
    """Haalt alle publicaties van één type op sinds datum_vanaf, op kenmerk."""
    per_kenmerk = {}
    for cpv in RELEVANTE_CPV_CODES:
        pagina = 0
        while True:
            params = {
                "cpvCodes": cpv,
                "publicatieDatumVanaf": datum_vanaf,
                "publicatieType": publicatie_type,
                "page": pagina,
                "size": 50,
            }
            try:
                batch, totaal = _tenderned_request(params, label=f"{label} {cpv} p{pagina}")
            except Exception as e:
                print(f"  [FOUT] {e}")
                break
            if not batch:
                break
            for item in batch:
                pub_id = str(item.get("id", item.get("publicatieId", "")))
                kenmerk = str(item.get("kenmerk", "") or pub_id)
                if kenmerk:
                    per_kenmerk.setdefault(kenmerk, item)
            if len(batch) < 50:
                break
            pagina += 1
            time.sleep(0.3)
    return per_kenmerk


def fetch_nieuwe_uitvragen(datum_vanaf, bekende_kenmerken):
    """AAOs sinds datum_vanaf die nog niet in de dataset zitten."""
    print(f"\n[1/6] Ophalen nieuwe uitvragen (AAO) vanaf {datum_vanaf}")
    alle = _haal_publicaties(datum_vanaf, "AAO", "AAO")
    nieuw = {}
    for kenmerk, item in alle.items():
        if kenmerk not in bekende_kenmerken:
            item["_publicatie_type"] = "lopend"
            nieuw[kenmerk] = item
    print(f"  {len(alle)} uitvragen gezien, {len(nieuw)} daarvan nieuw")
    return nieuw


def fetch_gunningen(datum_vanaf):
    """
    Alle AGOs sinds datum_vanaf, op kenmerk.

    Bewust zonder filter op de nieuwe uitvragen van deze run: een gunning volgt
    vrijwel altijd maanden na de uitvraag, dus hij hoort bij een record dat al
    lang in de dataset zit. Het oude filter liet die gunningen allemaal vallen.
    """
    print(f"\n[2/6] Ophalen gunningen (AGO) vanaf {datum_vanaf}")
    gunningen = _haal_publicaties(datum_vanaf, "AGO", "AGO")
    print(f"  {len(gunningen)} gunningen gevonden")
    return gunningen


def werk_gunningen_bij(bestaand, gunningen):
    """
    Markeert bestaande records als gegund zodra er een AGO voor is.

    Laat id en publicatiedatum met rust: id verwijst naar het opgeslagen
    document en publicatiedatum is altijd de datum van de oorspronkelijke
    uitvraag. De classificatie blijft ook staan — die verandert niet door een
    gunning, en opnieuw classificeren zou alleen geld kosten.
    """
    bijgewerkt = 0
    for r in bestaand:
        kenmerk = str(r.get("tenderned_kenmerk") or "")
        ago = gunningen.get(kenmerk)
        if not kenmerk or not ago:
            continue
        if r.get("publicatie_type") == "gegund" and r.get("gunningsdatum"):
            continue
        r["publicatie_type"] = "gegund"
        r["gunningsdatum"] = ago.get("publicatieDatum", "")
        r["gunning_publicatie_id"] = str(ago.get("publicatieId", "") or "")
        bijgewerkt += 1
    return bijgewerkt


# ── Pipeline ──────────────────────────────────────────────────────────────────

def verwerk_nieuw(ruw):
    """Normaliseert, haalt documenten op, classificeert en schoont op."""
    genormaliseerd = [normalize_tenderned(r) for r in ruw]
    genormaliseerd = [r for r in genormaliseerd if r.get("titel") or r.get("omschrijving")]
    print(f"  {len(genormaliseerd)} records na normalisatie")

    if not genormaliseerd:
        return []

    # Documenten ophalen
    print(f"\n[3/6] Documenten ophalen voor {len(genormaliseerd)} records...")
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
    print(f"\n[4/6] Classificeren ({len(genormaliseerd)} records)...")
    geclassificeerd = classificeer_alle(genormaliseerd, BATCH_GROOTTE)

    # Opschonen
    print(f"\n[5/6] Opschonen...")
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
    parser.add_argument(
        "--gunningen-vanaf",
        metavar="JJJJ-MM-DD",
        help="Kijk vanaf deze datum terug naar gunningen in plaats van de "
             f"standaard {GUNNING_TERUGBLIK_DAGEN} dagen. Voor een inhaalslag.",
    )
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

    # Gunningen kijken standaard verder terug dan de uitvragen. Een gunning kan
    # dagen na de aankondiging in de API verschijnen, en een mislukte run mag
    # geen gunningen definitief laten wegvallen.
    gunningen_vanaf = args.gunningen_vanaf or (
        datetime.now(timezone.utc) - timedelta(days=GUNNING_TERUGBLIK_DAGEN)
    ).strftime("%Y-%m-%d")

    print(f"Vorige update: {laatste or 'nog nooit'}")
    print(f"Uitvragen vanaf: {datum_vanaf}")
    print(f"Gunningen vanaf: {gunningen_vanaf}")

    bestaand = laad_dataset()
    bekende_kenmerken = {
        r.get("tenderned_kenmerk") for r in bestaand if r.get("tenderned_kenmerk")
    }
    print(f"Bestaande dataset: {len(bestaand)} records")

    nieuwe_aao = fetch_nieuwe_uitvragen(datum_vanaf, bekende_kenmerken)
    gunningen = fetch_gunningen(gunningen_vanaf)

    # Nieuwe uitvraag die al in dezelfde periode gegund is: dan is het
    # gunningsrecord het volledigere record en gaat dat de pipeline in.
    direct_gegund = 0
    for kenmerk, ago in gunningen.items():
        if kenmerk in nieuwe_aao:
            ago["_publicatie_type"] = "gegund"
            ago["_aao_publicatiedatum"] = nieuwe_aao[kenmerk].get("publicatieDatum", "")
            nieuwe_aao[kenmerk] = ago
            direct_gegund += 1
    if direct_gegund:
        print(f"  {direct_gegund} nieuwe uitvragen waren al gegund")

    # Bestaande records die inmiddels gegund zijn.
    bijgewerkt_gegund = werk_gunningen_bij(bestaand, gunningen)
    print(f"  {bijgewerkt_gegund} bestaande records op gegund gezet")

    nieuwe_ruw = list(nieuwe_aao.values())

    if args.dry_run:
        print(f"\n[DRY RUN] {len(nieuwe_ruw)} nieuwe publicaties, "
              f"{bijgewerkt_gegund} gunningen. Niets opgeslagen.")
        return

    if not nieuwe_ruw and not bijgewerkt_gegund:
        print("\nGeen nieuwe publicaties of gunningen. Dataset is up-to-date.")
        staat["laatste_update"] = nu
        sla_staat_op(staat)
        return

    nieuw_verwerkt = verwerk_nieuw(nieuwe_ruw) if nieuwe_ruw else []

    print(f"\n[6/6] Merge en opslaan...")
    gemerged = merge(bestaand, nieuw_verwerkt) if nieuw_verwerkt else bestaand

    sla_dataset_op(gemerged)
    print(f"  → {DATASET} ({len(gemerged)} records)")

    staat["laatste_update"] = nu
    staat["totaal_records"] = len(gemerged)
    sla_staat_op(staat)

    print(f"""
=== Klaar ===
Nieuwe records     : {len(nieuw_verwerkt)}
Gunningen verwerkt : {bijgewerkt_gegund}
Totaal dataset     : {len(gemerged)}
Timestamp          : {nu}
""")


if __name__ == "__main__":
    main()
