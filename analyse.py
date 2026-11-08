"""
analyse.py
----------
Genereert marktinzichten uit de geclassificeerde dataset.
Produceert tabellen en een samenvattend rapport als basis voor positionering.

Gebruik:
    python analyse.py
    python analyse.py --invoer data/geclassificeerd.json

Output:
    data/analyse_themas.csv         marktaandeel per thema
    data/analyse_methodieken.csv    verdeling evaluatiemethodieken
    data/analyse_opdrachtgevers.csv top opdrachtgevers
    data/concurrentiekaart.csv      wie wint wat
    data/analyse_trend.csv          ontwikkeling per jaar
    data/marktrapport.md            samenvattend tekstrapport
    data/geclassificeerd_met_winnaar.csv  actuele platte dataset
    data/twijfelgevallen.json       actuele records met lage betrouwbaarheid
"""

import json
import csv
import os
import argparse
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from opslag import schrijf_json
from winnaars import namen as winnaar_namen

INPUT_BESTAND = "data/geclassificeerd_met_winnaar.json"
OUTPUT_DIR = "data"


# ── Laden ─────────────────────────────────────────────────────────────────────

def laad(pad: str) -> list[dict]:
    with open(pad, encoding="utf-8") as f:
        return json.load(f)


def cl(record: dict) -> dict:
    """Geeft classificatieobject terug, of leeg dict bij ontbrekende classificatie."""
    return record.get("classificatie") or {}


def jaar(record: dict) -> str | None:
    datum = record.get("publicatiedatum", "")
    if datum and len(datum) >= 4:
        return datum[:4]
    return None


# ── Analyses ──────────────────────────────────────────────────────────────────

def analyse_themas(records: list[dict]) -> list[dict]:
    teller = Counter(cl(r).get("thema", "Onbekend") for r in records)
    kwantitatief_per_thema = Counter(
        cl(r).get("thema", "Onbekend")
        for r in records
        if cl(r).get("methodiek_kwantitatief", False)
    )
    totaal = len(records)
    return [
        {
            "thema": thema,
            "aantal": aantal,
            "aandeel_pct": round(100 * aantal / totaal, 1),
            "waarvan_kwantitatief": kwantitatief_per_thema.get(thema, 0),
            "kwantitatief_pct": round(
                100 * kwantitatief_per_thema.get(thema, 0) / aantal, 1
            ) if aantal else 0,
        }
        for thema, aantal in teller.most_common()
    ]


def analyse_methodieken(records: list[dict]) -> list[dict]:
    teller = Counter(cl(r).get("methodiek", "Onbekend") for r in records)
    totaal = len(records)
    return [
        {
            "methodiek": m,
            "aantal": n,
            "aandeel_pct": round(100 * n / totaal, 1),
            "waarvan_kwantitatief": sum(
                1 for r in records if cl(r).get("methodiek", "Onbekend") == m
                and cl(r).get("methodiek_kwantitatief")
            ),
        }
        for m, n in teller.most_common()
    ]




def analyse_opdrachtgevers(records: list[dict]) -> list[dict]:
    per_dienst: dict[str, dict] = defaultdict(lambda: {
        "aantal": 0, "kwantitatief": 0, "totaalwaarde": 0.0, "themas": Counter()
    })
    for r in records:
        dienst = r.get("aanbestedende_dienst", "Onbekend") or "Onbekend"
        per_dienst[dienst]["aantal"] += 1
        if cl(r).get("methodiek_kwantitatief"):
            per_dienst[dienst]["kwantitatief"] += 1
        waarde = r.get("gunningswaarde")
        if waarde:
            try:
                per_dienst[dienst]["totaalwaarde"] += float(waarde)
            except (ValueError, TypeError):
                pass
        thema = cl(r).get("thema", "")
        if thema:
            per_dienst[dienst]["themas"][thema] += 1

    resultaat = []
    for dienst, data in per_dienst.items():
        top_thema = data["themas"].most_common(1)
        resultaat.append({
            "aanbestedende_dienst": dienst,
            "aantal_opdrachten": data["aantal"],
            "waarvan_kwantitatief": data["kwantitatief"],
            "totaalwaarde_euro": round(data["totaalwaarde"]),
            "voornaamste_thema": top_thema[0][0] if top_thema else "",
        })

    return sorted(resultaat, key=lambda x: x["aantal_opdrachten"], reverse=True)[:30]


def analyse_concurrenten(records: list[dict]) -> list[dict]:
    per_bureau: dict[str, dict] = defaultdict(lambda: {
        "aantal": 0, "kwantitatief": 0, "waarde": 0.0, "themas": Counter(), "methodieken": Counter()
    })
    for r, bureau in ((r, n) for r in records for n in winnaar_namen(r)):
        if not bureau or bureau.lower() in ("", "onbekend", "unknown"):
            continue
        per_bureau[bureau]["aantal"] += 1
        try:
            if len(winnaar_namen(r)) == 1:
                per_bureau[bureau]["waarde"] += float(r.get("gunningswaarde") or r.get("waarde_definitief") or 0)
        except (ValueError, TypeError):
            pass
        if cl(r).get("methodiek_kwantitatief"):
            per_bureau[bureau]["kwantitatief"] += 1
        thema = cl(r).get("thema", "")
        methodiek = cl(r).get("methodiek", "")
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
            "kwantitatief_pct": round(
                100 * data["kwantitatief"] / data["aantal"], 1
            ) if data["aantal"] else 0,
            "bekende_waarde_als_enige_winnaar_euro": round(data["waarde"]),
            "sterkste_thema": top_thema[0][0] if top_thema else "",
            "sterkste_methodiek": top_methodiek[0][0] if top_methodiek else "",
        })

    return sorted(resultaat, key=lambda x: x["aantal_gewonnen"], reverse=True)


def analyse_trend(records: list[dict]) -> list[dict]:
    per_jaar: dict[str, dict] = defaultdict(lambda: {
        "totaal": 0, "kwantitatief": 0, "waarde": 0.0
    })
    for r in records:
        j = jaar(r)
        if not j:
            continue
        per_jaar[j]["totaal"] += 1
        if cl(r).get("methodiek_kwantitatief"):
            per_jaar[j]["kwantitatief"] += 1
        waarde = r.get("gunningswaarde")
        if waarde:
            try:
                per_jaar[j]["waarde"] += float(waarde)
            except (ValueError, TypeError):
                pass

    return [
        {
            "jaar": j,
            "totaal_opdrachten": data["totaal"],
            "kwantitatief": data["kwantitatief"],
            "kwantitatief_pct": round(
                100 * data["kwantitatief"] / data["totaal"], 1
            ) if data["totaal"] else 0,
            "totaalwaarde_euro": round(data["waarde"]),
        }
        for j, data in sorted(per_jaar.items())
    ]


# ── Rapport ───────────────────────────────────────────────────────────────────

def schrijf_rapport(
    records: list[dict],
    themas: list[dict],
    methodieken: list[dict],
    opdrachtgevers: list[dict],
    concurrenten: list[dict],
    trend: list[dict],
) -> str:
    totaal = len(records)
    kwantitatief = sum(1 for r in records if cl(r).get("methodiek_kwantitatief", False))
    datums = sorted(r["publicatiedatum"][:10] for r in records if r.get("publicatiedatum"))
    periode = f"{datums[0]} t/m {datums[-1]}" if datums else "geen publicatiedatums"
    met_winnaar = sum(bool(winnaar_namen(r)) for r in records)
    nationaal = sum(r.get("europees") is False for r in records)
    europees = sum(r.get("europees") is True for r in records)

    rapport = f"""# Marktrapport: Kwantitatieve beleidsevaluatie Nederland
*Gegenereerd op {datetime.now().strftime("%d-%m-%Y")} op basis van {totaal} geclassificeerde aanbestedingen*

---

## 1. Omvang van de geselecteerde markt

Totaal geanalyseerde aanbestedingen: **{totaal}**
Waarvan door AI als kwantitatief ingedeeld: **{kwantitatief}** ({round(100*kwantitatief/max(totaal, 1))}%)

Publicatiedatums: **{periode}**. Dit is de dekking van de records, niet de datum van de laatste broncontrole.
Europees: **{europees}**; nationaal: **{nationaal}**; onbekend: **{totaal-europees-nationaal}**.
Met bekende (mede)winnaars: **{met_winnaar}** van **{totaal}**.

De selectie is gebaseerd op onderzoek- en adviescodes in TenderNed. Niet alle opdrachten zijn beleidsevaluaties.
Onderhandse opdrachten en nadere opdrachten onder raamovereenkomsten zijn niet volledig zichtbaar.
AI-classificaties zijn inschattingen; betrouwbaarheidsscores zijn geen gekalibreerde kansen.
Het laatste jaar is onvolledig. Vergelijk jaartotalen daarom voorzichtig.

### Trend
"""
    for rij in trend:
        rapport += f"- {rij['jaar']}: {rij['totaal_opdrachten']} opdrachten, {rij['kwantitatief_pct']}% kwantitatief\n"

    rapport += "\n## 2. Verdeling naar thema\n\n"
    for rij in themas[:10]:
        rapport += (
            f"- **{rij['thema']}**: {rij['aantal']} opdrachten ({rij['aandeel_pct']}%), "
            f"waarvan {rij['kwantitatief_pct']}% kwantitatief\n"
        )

    rapport += "\n## 3. Verdeling naar methodiek\n\n"
    for rij in methodieken:
        rapport += (f"- **{rij['methodiek']}**: {rij['aantal']} ({rij['aandeel_pct']}%), "
                    f"waarvan {rij['waarvan_kwantitatief']} als kwantitatief ingedeeld\n")

    rapport += "\n## 4. Top opdrachtgevers\n\n"
    for rij in opdrachtgevers[:10]:
        rapport += (
            f"- **{rij['aanbestedende_dienst']}**: {rij['aantal_opdrachten']} opdrachten, "
            f"{rij['waarvan_kwantitatief']} kwantitatief, "
            f"voornaamste thema: {rij['voornaamste_thema']}\n"
        )

    rapport += "\n## 5. Concurrentiekaart\n\n"
    rapport += "| Bureau | Gewonnen | Kwant. % | Sterkste thema | Sterkste methodiek |\n"
    rapport += "|---|---|---|---|---|\n"
    for rij in concurrenten[:20]:
        rapport += (
            f"| {rij['bureau']} "
            f"| {rij['aantal_gewonnen']} "
            f"| {rij['kwantitatief_pct']}% "
            f"| {rij['sterkste_thema']} "
            f"| {rij['sterkste_methodiek']} |\n"
        )

    # Ecorys positie
    ecorys = next((r for r in concurrenten if "ECORYS" in r.get("bureau", "").upper()), None)
    if ecorys:
        rapport += f"\n**Positie Ecorys:** {ecorys['aantal_gewonnen']} gewonnen opdrachten, "
        rapport += f"{ecorys['kwantitatief_pct']}% kwantitatief, "
        rapport += f"sterkste thema: {ecorys['sterkste_thema']}\n"
    else:
        rapport += "\n*Geen winnaar met Ecorys in de naam aangetroffen in deze dataset.*\n"

    rapport += "\n> **Let op scope:** winnaarsinformatie is alleen beschikbaar voor aanbestedingen "
    rapport += "die succesvol gekoppeld zijn aan de TenderNed bulk Excel. "
    rapport += "De kaart telt iedere bekende (mede)winnaar eenmaal per opdracht. "
    rapport += "Dit is geen volledig overzicht van de markt of van marktaandelen.\n"

    rapport += """
## 6. Kansen voor Ecorys

*Vragen voor verdere commerciële analyse; de dataset bewijst deze kansen niet zelfstandig:*

- Thema's met hoog kwantitatief aandeel maar beperkte concurrentie
- Opdrachtgevers die regelmatig aanbesteden maar nog niet structureel bediend worden
- Methodieken waar de markt groeit maar het aanbod beperkt is

*Vul dit onderdeel handmatig aan op basis van de data en opdrachtgeversgesprekken.*
"""
    return rapport


# ── Opslaan ───────────────────────────────────────────────────────────────────

def schrijf_csv(data: list[dict], bestandsnaam: str):
    if not data:
        # Verwijder oude resultaten als de nieuwe selectie leeg is.
        Path(OUTPUT_DIR, bestandsnaam).write_text("", encoding="utf-8")
        return
    pad = os.path.join(OUTPUT_DIR, bestandsnaam)
    with open(pad, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  -> {pad}")


# ── Afgeleide bestanden ───────────────────────────────────────────────────────


def vernieuw_exports(records):
    """Bouw alle afgeleide bestanden uit dezelfde actuele dashboarddataset."""
    from classify import sla_op_csv, TWIJFEL_DREMPEL

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    themas = analyse_themas(records)
    methodieken = analyse_methodieken(records)
    opdrachtgevers = analyse_opdrachtgevers(records)
    concurrenten = analyse_concurrenten(records)
    trend = analyse_trend(records)
    for naam, inhoud in (
        ("analyse_themas.csv", themas), ("analyse_methodieken.csv", methodieken),
        ("analyse_opdrachtgevers.csv", opdrachtgevers), ("concurrentiekaart.csv", concurrenten),
        ("analyse_trend.csv", trend),
    ):
        schrijf_csv(inhoud, naam)
    sla_op_csv(records, "geclassificeerd_met_winnaar.csv")
    schrijf_json(Path(OUTPUT_DIR) / "twijfelgevallen.json", [
        r for r in records if cl(r).get("betrouwbaarheid", 0) < TWIJFEL_DREMPEL
    ])
    Path(OUTPUT_DIR, "marktrapport.md").write_text(
        schrijf_rapport(records, themas, methodieken, opdrachtgevers, concurrenten, trend),
        encoding="utf-8",
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Analyseer geclassificeerde aanbestedingsdata")
    parser.add_argument("--invoer", default=INPUT_BESTAND)
    args = parser.parse_args()

    if not os.path.exists(args.invoer):
        print(f"[FOUT] {args.invoer} niet gevonden. Draai eerst classify.py.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"[1/3] Laden {args.invoer}...")
    records = laad(args.invoer)
    print(f"  {len(records)} records geladen")

    print("\n[2/3] Analyses en exports vernieuwen...")
    vernieuw_exports(records)

    print(f"""
=== Klaar ===
Alle outputbestanden staan in {OUTPUT_DIR}/
Bekijk marktrapport.md voor een eerste samenvatting.
""")


if __name__ == "__main__":
    main()
