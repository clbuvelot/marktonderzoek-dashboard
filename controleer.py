"""Controleer de actuele dataset, documenten, updatestatus en CSV-export."""

import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
from winnaars import namen as winnaar_namen, onzeker
from teamclassificatie import valideer_teams


def controleer(root=Path(".")):
    root = Path(root)
    data = root / "data"
    records = json.loads((data / "geclassificeerd_met_winnaar.json").read_text(encoding="utf-8"))
    staat = json.loads((data / "update_staat.json").read_text(encoding="utf-8"))
    fouten = []
    if staat.get("totaal_records") != len(records):
        fouten.append("Totaal in updatestatus wijkt af van de dataset.")
    for sleutel in ("id", "tenderned_kenmerk"):
        waarden = [str(r.get(sleutel) or "") for r in records]
        if "" in waarden or len(set(waarden)) != len(waarden):
            fouten.append(f"Ontbrekende of dubbele {sleutel}.")
    for r in records:
        rid = str(r.get("id", ""))
        if not rid.isdigit():
            fouten.append(f"Ongeldig TenderNed-id: {rid!r}.")
            continue
        if r.get("publicatie_type") not in {"lopend", "gegund"}:
            fouten.append(f"{rid}: ongeldige status.")
        try:
            date.fromisoformat(r.get("publicatiedatum", "")[:10])
        except ValueError:
            fouten.append(f"{rid}: ongeldige publicatiedatum.")
        if not isinstance(r.get("classificatie"), dict) or not r["classificatie"]:
            fouten.append(f"{rid}: classificatie ontbreekt.")
        else:
            try:
                valideer_teams(r['classificatie'])
            except ValueError as exc:
                fouten.append(f'{rid}: {exc}')
        doc = data / "documenten" / f"{rid}.txt"
        beschikbaar = doc.exists() and doc.stat().st_size > 0
        if bool(r.get("documenten_geladen")) != beschikbaar:
            fouten.append(f"{rid}: documentstatus klopt niet met het bestand.")
        if "winnaars" in r:
            if len(set(r["winnaars"])) != len(r["winnaars"]) or any(onzeker(n) for n in r["winnaars"]):
                fouten.append(f"{rid}: dubbele of onzekere bevestigde winnaars.")
            if r["winnaars"] != [n for n in [r.get("winnaar"), *r.get("extra_winnaars", [])] if n]:
                fouten.append(f"{rid}: winnaarsvelden spreken elkaar tegen.")
        for koppeling in r.get("gunning_koppelingen", []):
            if not koppeling.get("methoden") or koppeling.get("record_id") != rid:
                fouten.append(f"{rid}: onvolledig koppelbewijs.")
            if koppeling.get("gunning_publicatie_id") not in r.get("gunning_publicatie_ids", []):
                fouten.append(f"{rid}: koppelbewijs zonder gunningspublicatie.")
    with (data / "geclassificeerd_met_winnaar.csv").open(encoding="utf-8", newline="") as f:
        export = list(csv.DictReader(f))
    if Counter(r["id"] for r in export) != Counter(r["id"] for r in records):
        fouten.append("De CSV-export bevat andere opdrachten dan de dataset.")
    per_id = {r["id"]: r for r in records}
    if any(r.get("winnaars", "") != " | ".join(winnaar_namen(per_id[r["id"]])) for r in export if r["id"] in per_id):
        fouten.append("De CSV-export bevat afwijkende winnaars.")
    if any(r.get('teams', '') != ' | '.join(per_id[r['id']]['classificatie'].get('teams', []))
           or r.get('teams_status') != per_id[r['id']]['classificatie'].get('teams_status')
           or r.get('units', '') != ' | '.join(per_id[r['id']]['classificatie'].get('units', []))
           or r.get('units_status', '') != per_id[r['id']]['classificatie'].get('units_status', '')
           or r.get('opdrachtvraag', '') != per_id[r['id']]['classificatie'].get('opdrachtprofiel', {}).get('vraag', '')
           for r in export if r['id'] in per_id):
        fouten.append('De CSV-export bevat afwijkende teams.')
    with (data / "concurrentiekaart.csv").open(encoding="utf-8", newline="") as f:
        concurrenten = {r["bureau"]: int(r["aantal_gewonnen"]) for r in csv.DictReader(f)}
    verwacht = dict(Counter(n for r in records for n in winnaar_namen(r)))
    if concurrenten != verwacht:
        fouten.append("De concurrentiekaart telt mede-winnaars niet correct.")
    with (data / "analyse_trend.csv").open(encoding="utf-8", newline="") as f:
        trend = {r["jaar"]: int(r["totaal_opdrachten"]) for r in csv.DictReader(f)}
    if trend != dict(Counter(r["publicatiedatum"][:4] for r in records)):
        fouten.append("De trendanalyse wijkt af van de dataset.")
    if fouten:
        raise ValueError("\n".join(fouten))
    return {
        "opdrachten": len(records),
        "met_winnaar": sum(bool(winnaar_namen(r)) for r in records),
        "met_documenten": sum(bool(r.get("documenten_geladen")) for r in records),
        "laatste_broncontrole": staat.get("laatste_update"),
    }


if __name__ == "__main__":
    print(json.dumps(controleer(), indent=2, ensure_ascii=False))
