"""
koppel_winnaar.py
-----------------
Koppelt winnaarsinformatie uit de TenderNed bulk Excel aan de geclassificeerde dataset.

Gebruik:
    python koppel_winnaar.py

Verwacht:
    data/geclassificeerd.json
    Dataset_Tenderned-2016-01-01_tm_2025-12-31_Leeswijzer.xlsx  (in huidige map)

Output:
    data/geclassificeerd_met_winnaar.json
    data/geclassificeerd_met_winnaar.csv
    data/concurrentiekaart.csv
"""

import json
import csv
import os
import pandas as pd
from collections import defaultdict, Counter

EXCEL_BESTAND = "Dataset_Tenderned-2016-01-01 tm 2025-12-31_Leeswijzer.xlsx"
GECLASSIFICEERD = "data/geclassificeerd.json"
OUTPUT_DIR = "data"


def laad_excel(pad: str) -> pd.DataFrame:
    print(f"[1/4] Laden bulk Excel (dit duurt even)...")
    df = pd.read_excel(pad, sheet_name="OpenData sheet", dtype={"ID publicatie": str})
    print(f"  {len(df)} rijen geladen")

    # Houd alleen gunningsrijen met een winnaar
    df_gunning = df[df["Naam gegunde onderneming"].notna()].copy()
    print(f"  {len(df_gunning)} rijen met winnaarsinformatie")
    return df_gunning


def bouw_winnaar_lookup(df: pd.DataFrame) -> dict:
    """
    Bouwt een opzoektabel van publicatieId -> winnaar en waarde.
    Bij meerdere winnaars (percelen) worden ze samengevoegd.
    """
    lookup = {}
    for _, rij in df.iterrows():
        pub_id = str(rij.get("ID publicatie", "")).strip()
        if not pub_id:
            continue

        winnaar = str(rij.get("Naam gegunde onderneming", "") or "").strip()
        waarde = rij.get("Definitieve waarde - bedrag", None)
        datum_gunning = str(rij.get("Datum gunning", "") or "").strip()

        if pub_id not in lookup:
            lookup[pub_id] = {
                "winnaar": winnaar,
                "waarde_definitief": float(waarde) if pd.notna(waarde) else None,
                "datum_gunning": datum_gunning,
                "extra_winnaars": [],
            }
        else:
            # Meerdere winnaars (percelen) — voeg samen
            if winnaar and winnaar != lookup[pub_id]["winnaar"]:
                lookup[pub_id]["extra_winnaars"].append(winnaar)

    print(f"  {len(lookup)} unieke publicaties met winnaar")
    return lookup


def koppel(records: list[dict], lookup: dict) -> tuple[list[dict], int]:
    """Voegt winnaarsinformatie toe aan elk geclassificeerd record."""
    gekoppeld = 0
    for record in records:
        pub_id = str(record.get("id", "")).strip()
        if pub_id in lookup:
            info = lookup[pub_id]
            record["winnaar"] = info["winnaar"]
            record["waarde_definitief"] = info["waarde_definitief"]
            record["datum_gunning"] = info["datum_gunning"]
            if info["extra_winnaars"]:
                record["extra_winnaars"] = info["extra_winnaars"]
            gekoppeld += 1
    return records, gekoppeld


def bouw_concurrentiekaart(records: list[dict]) -> list[dict]:
    """Maakt een overzicht per bureau van gewonnen opdrachten."""
    per_bureau: dict[str, dict] = defaultdict(lambda: {
        "aantal": 0,
        "kwantitatief": 0,
        "totaalwaarde": 0.0,
        "themas": Counter(),
        "methodieken": Counter(),
    })

    for r in records:
        bureau = r.get("winnaar", "") or ""
        if not bureau or bureau.lower() in ("", "nan", "onbekend"):
            continue

        cl = r.get("classificatie") or {}
        per_bureau[bureau]["aantal"] += 1

        if cl.get("methodiek_kwantitatief"):
            per_bureau[bureau]["kwantitatief"] += 1

        waarde = r.get("waarde_definitief")
        if waarde:
            try:
                per_bureau[bureau]["totaalwaarde"] += float(waarde)
            except (ValueError, TypeError):
                pass

        thema = cl.get("thema", "")
        methodiek = cl.get("methodiek", "")
        if thema:
            per_bureau[bureau]["themas"][thema] += 1
        if methodiek:
            per_bureau[bureau]["methodieken"][methodiek] += 1

    resultaat = []
    for bureau, data in per_bureau.items():
        top_thema = data["themas"].most_common(1)
        top_methodiek = data["methodieken"].most_common(1)
        resultaat.append({
            "bureau": bureau,
            "aantal_gewonnen": data["aantal"],
            "waarvan_kwantitatief": data["kwantitatief"],
            "kwantitatief_pct": round(100 * data["kwantitatief"] / data["aantal"], 1) if data["aantal"] else 0,
            "totaalwaarde_euro": round(data["totaalwaarde"]),
            "sterkste_thema": top_thema[0][0] if top_thema else "",
            "sterkste_methodiek": top_methodiek[0][0] if top_methodiek else "",
        })

    return sorted(resultaat, key=lambda x: x["aantal_gewonnen"], reverse=True)


def sla_op_json(data, pad):
    with open(pad, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  → {pad}")


def sla_op_csv(data: list[dict], pad: str, velden: list[str]):
    with open(pad, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=velden, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    print(f"  → {pad}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Controleer bestanden
    for pad in [EXCEL_BESTAND, GECLASSIFICEERD]:
        if not os.path.exists(pad):
            print(f"[FOUT] {pad} niet gevonden.")
            return

    # Laad data
    df_gunning = laad_excel(EXCEL_BESTAND)
    lookup = bouw_winnaar_lookup(df_gunning)

    print(f"\n[2/4] Laden geclassificeerde data...")
    with open(GECLASSIFICEERD, encoding="utf-8") as f:
        records = json.load(f)
    print(f"  {len(records)} records geladen")

    print(f"\n[3/4] Koppelen...")
    records, gekoppeld = koppel(records, lookup)
    print(f"  {gekoppeld} van {len(records)} records gekoppeld aan winnaar")

    print(f"\n[4/4] Opslaan...")

    # Volledige dataset
    sla_op_json(records, f"{OUTPUT_DIR}/geclassificeerd_met_winnaar.json")

    # CSV met platte structuur
    velden_csv = [
        "id", "bron", "titel", "aanbestedende_dienst", "publicatiedatum",
        "winnaar", "waarde_definitief", "datum_gunning", "procedure_type", "url",
        "thema", "subthema", "methodiek", "methodiek_kwantitatief", "skills",
        "betrouwbaarheid",
    ]
    records_plat = []
    for r in records:
        cl = r.get("classificatie") or {}
        records_plat.append({
            **{k: r.get(k, "") for k in velden_csv},
            "thema": cl.get("thema", ""),
            "subthema": cl.get("subthema", ""),
            "methodiek": cl.get("methodiek", ""),
            "methodiek_kwantitatief": cl.get("methodiek_kwantitatief", False),
            "skills": " | ".join(cl.get("skills", [])),
            "betrouwbaarheid": cl.get("betrouwbaarheid", ""),
        })
    sla_op_csv(records_plat, f"{OUTPUT_DIR}/geclassificeerd_met_winnaar.csv", velden_csv)

    # Concurrentiekaart
    concurrentiekaart = bouw_concurrentiekaart(records)
    sla_op_csv(
        concurrentiekaart,
        f"{OUTPUT_DIR}/concurrentiekaart.csv",
        ["bureau", "aantal_gewonnen", "waarvan_kwantitatief", "kwantitatief_pct",
         "totaalwaarde_euro", "sterkste_thema", "sterkste_methodiek"]
    )

    # Samenvatting
    print(f"""
=== Samenvatting concurrentiekaart (top 10) ===
{'Bureau':<45} {'Gewonnen':>8} {'Kwant%':>7} {'Waarde (€)':>12}""")
    for rij in concurrentiekaart[:10]:
        print(f"  {rij['bureau']:<43} {rij['aantal_gewonnen']:>8} {rij['kwantitatief_pct']:>6}% {rij['totaalwaarde_euro']:>12,.0f}")

    print(f"\nKlaar. Bestanden staan in {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
