"""
vul_winnaars_aan.py
-------------------
Vult winnaarsnamen aan in de bestaande dataset, uit de TenderNed bulk Excel.

Waarom een apart script: de TNS API geeft geen winnaar terug. Niet in de
lijst-respons en niet op het detail-endpoint — daar staat alleen isGegund.
De naam van de gegunde onderneming staat uitsluitend in de bulk Excel die
TenderNed periodiek publiceert.

Controleert de bestaande dataset met publicatie-id's, procedure-id's,
bronverwijzingen en projectreferenties. Alle bevestigde winnaars worden vernieuwd.
Onzekere koppelingen worden vastgelegd voor controle.

Gebruik:
    python vul_winnaars_aan.py
    python vul_winnaars_aan.py --excel pad/naar/dataset.xlsx   # zelf aanleveren
    python vul_winnaars_aan.py --dry-run

Leest/schrijft:
    data/geclassificeerd_met_winnaar.json   (bijgewerkt op z'n plek)
    data/concurrentiekaart.csv              (opnieuw opgebouwd)
"""

import argparse
import json
import os
import re
import urllib.request
import hashlib
from pathlib import Path
from urllib.parse import urljoin

from analyse import vernieuw_exports
from opslag import schrijf_json
from tenderned_bulk import lees_bulk
from herstel_gunningen import herstel
from datetime import datetime, timezone

DATASET = "data/geclassificeerd_met_winnaar.json"
OUTPUT_DIR = "data"
DATASETS_PAGINA = "https://www.tenderned.nl/cms/aanbesteden-in-cijfers/datasets-aanbestedingen"


# ── Excel vinden en ophalen ───────────────────────────────────────────────────

def zoek_nieuwste_excel_url() -> str:
    """
    Pakt van de TenderNed-datasetpagina het bestand met de laatste einddatum.
    De bestandsnaam bevat die datum, bijvoorbeeld:
    Dataset_Tenderned-compleet-2021-01-01-2026-10-01.xlsx
    """
    print("[1/5] Nieuwste bulk Excel zoeken op tenderned.nl...")
    with urllib.request.urlopen(DATASETS_PAGINA, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    kandidaten = []
    for url in set(re.findall(r'href="([^"]+\.xlsx)"', html, re.I)):
        datums = re.findall(r"(\d{4}-\d{2}-\d{2})", url)
        if datums:
            kandidaten.append((max(datums), url))

    if not kandidaten:
        raise RuntimeError("Geen .xlsx-bestanden gevonden op de datasetpagina")

    einddatum, url = max(kandidaten)
    url = urljoin(DATASETS_PAGINA, url)
    print(f"  gevonden, loopt tot {einddatum}")
    return url


def download_excel(url: str) -> str:
    # Een nieuwe URL moet een nieuwe download opleveren, niet een oude cache.
    cache = Path(OUTPUT_DIR) / ".cache"
    cache.mkdir(parents=True, exist_ok=True)
    sleutel = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    doel = str(cache / f"tenderned-{sleutel}.xlsx")
    if os.path.exists(doel) and datetime.now(timezone.utc).timestamp() - os.path.getmtime(doel) < 7 * 86400:
        print(f"[2/5] Excel al gedownload: {doel}")
        return doel
    print(f"[2/5] Downloaden (ongeveer 90 MB, dit duurt even)...")
    gedeeltelijk = doel + ".part"
    urllib.request.urlretrieve(url, gedeeltelijk)
    os.replace(gedeeltelijk, doel)
    print(f"  {round(os.path.getsize(doel) / 1e6, 1)} MB opgeslagen")
    return doel


def main():
    parser = argparse.ArgumentParser(description="Gunningen koppelen en alle winnaars vernieuwen")
    parser.add_argument("--excel", nargs="+", help="Bulkbestanden van oud naar nieuw; standaard nieuwste download")
    parser.add_argument("--dry-run", action="store_true", help="Schrijf alleen een controlerapport; dataset blijft gelijk")
    args = parser.parse_args()
    records = json.loads(Path(DATASET).read_text(encoding="utf-8"))
    bestanden = args.excel or [download_excel(zoek_nieuwste_excel_url())]
    publicaties = {}
    for bestand in bestanden:
        for p in lees_bulk(bestand):
            publicaties[p["id"]] = p
    print(f"Gunningen vergelijken met {len(records)} opdrachten...", flush=True)
    rapport = herstel(records, list(publicaties.values()))
    rapport["gecontroleerd_op"] = datetime.now(timezone.utc).isoformat()
    rapport["bronbestanden"] = [Path(p).name for p in bestanden]
    if args.dry_run:
        schrijf_json("data/.cache/koppelrapport-proef.json", rapport)
        print("Proefcontrole opgeslagen; dashboarddataset niet gewijzigd.")
    else:
        backup = Path("data/.cache/herstel") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-voor-herstel.json")
        schrijf_json(backup, json.loads(Path(DATASET).read_text(encoding="utf-8")))
        schrijf_json(DATASET, records)
        schrijf_json("data/koppelrapport.json", rapport)
        vernieuw_exports(records)
    print(json.dumps({k: v for k, v in rapport.items() if k not in
                      ("wijzigingen", "controlelijst", "afgewezen_titelkandidaten", "identiteitsconflicten")},
                     ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
