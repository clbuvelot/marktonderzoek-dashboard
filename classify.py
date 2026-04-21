"""
classify.py
-----------
Classificeert aanbestedingen naar meerdere dimensies via de Anthropic API.
Per dimensie wordt een aparte betrouwbaarheidsscore opgeslagen.

Gebruik:
    python classify.py
    python classify.py --invoer data/raw_tenderned.json
    python classify.py --batch-grootte 15

Output:
    data/geclassificeerd.json      volledige dataset met classificaties
    data/geclassificeerd.csv       platte tabel voor analyse in Excel/R/Python
    data/twijfelgevallen.json      records met lage gemiddelde betrouwbaarheid (<0.6)
"""

import json
import csv
import os
import time
import argparse
import anthropic

# ── Configuratie ──────────────────────────────────────────────────────────────

INPUT_BESTANDEN = [
    "data/raw_tenderned.json",
    "data/raw_kamerstukken.json",
]
OUTPUT_DIR = "data"
MODEL = "claude-haiku-4-5-20251001"
TWIJFEL_DREMPEL = 0.6
BATCH_GROOTTE = 15  # kleiner vanwege grotere output per record


# ── Taxonomie ─────────────────────────────────────────────────────────────────

TAXONOMIE = {
    "themas": [
        "Arbeidsmarkt & sociale zekerheid",
        "Onderwijs & talentontwikkeling",
        "Zorg & welzijn",
        "Economie & innovatie",
        "Energie & klimaat",
        "Wonen & ruimte",
        "Veiligheid & justitie",
        "Bestuur & uitvoering",
        "Internationaal & EU-fondsen",
        "Overig / onduidelijk",
    ],
    "methodieken": [
        "Procesevaluatie",
        "Doeltreffendheidsevaluatie (beschrijvend)",
        "Quasi-experimenteel",
        "RCT / experiment",
        "MKBA",
        "Statistisch / econometrisch",
        "Systeemevaluatie / beleidsdoorlichting",
        "Mixed methods",
        "Onduidelijk / niet gespecificeerd",
    ],
    "skills": [
        "CBS microdata",
        "Survey / enquête",
        "Econometrie",
        "GIS / ruimtelijke analyse",
        "Literatuuronderzoek / meta-analyse",
        "Interviews / kwalitatief",
        "Financiële / begrotingsanalyse",
        "UWV / DUO / RVO data",
    ],
    "evaluatiefases": [
        "Ex-ante",
        "Mid-term",
        "Ex-post",
        "Monitor / periodieke meting",
        "Onduidelijk",
    ],
    "databronnen": [
        "CBS microdata",
        "UWV administratie",
        "DUO data",
        "RVO data",
        "Eigen survey / enquête",
        "Administratieve data (niet nader gespecificeerd)",
        "Interviews / kwalitatief veldwerk",
        "Literatuur / bestaande studies",
        "Geen specifieke databron vermeld",
    ],
    "opdrachtgrootte": [
        "Klein (< €50k)",
        "Middel (€50k - €200k)",
        "Groot (> €200k)",
        "Onduidelijk",
    ],
}


# ── Systeem prompt ────────────────────────────────────────────────────────────

SYSTEEM_PROMPT = """Je bent een expert in Nederlandse beleidsevaluatie en aanbestedingen.
Je classificeert aanbestedingsteksten naar meerdere dimensies tegelijk.
Antwoord UITSLUITEND met geldig JSON. Geen uitleg, geen markdown, geen inleiding."""


# ── Classificatieprompt ───────────────────────────────────────────────────────

def bouw_classificatieprompt(records: list[dict]) -> str:
    themas = "\n".join(f"  - {t}" for t in TAXONOMIE["themas"])
    methodieken = "\n".join(f"  - {m}" for m in TAXONOMIE["methodieken"])
    skills = "\n".join(f"  - {s}" for s in TAXONOMIE["skills"])
    evaluatiefases = "\n".join(f"  - {e}" for e in TAXONOMIE["evaluatiefases"])
    databronnen = "\n".join(f"  - {d}" for d in TAXONOMIE["databronnen"])
    opdrachtgroottes = "\n".join(f"  - {o}" for o in TAXONOMIE["opdrachtgrootte"])

    records_tekst = ""
    for i, r in enumerate(records):
        tekst = (r.get("titel", "") + " " + r.get("omschrijving", "")).strip()
        tekst = tekst[:2000]
        records_tekst += f'\nRECORD {i}:\n"""\n{tekst}\n"""\n'

    return f"""Classificeer de onderstaande aanbestedingsteksten naar alle opgegeven dimensies.

TAXONOMIE:

Thema (kies precies 1):
{themas}

Methodiek (kies precies 1):
{methodieken}

Skills (kies 1 of meer):
{skills}

Evaluatiefase (kies precies 1):
{evaluatiefases}

Databronnen (kies 1 of meer die expliciet genoemd of sterk geïmpliceerd worden):
{databronnen}

Opdrachtgrootte (kies precies 1, schat op basis van omvang en complexiteit):
{opdrachtgroottes}

RECORDS:
{records_tekst}

Retourneer een JSON-array. Elk object heeft deze structuur:
[
  {{
    "index": 0,

    "thema": "<kies uit lijst>",
    "thema_betrouwbaarheid": 0.0-1.0,
    "subthema": "<eigen beschrijving max 5 woorden>",

    "methodiek": "<kies uit lijst>",
    "methodiek_betrouwbaarheid": 0.0-1.0,
    "methodiek_kwantitatief": true/false,

    "skills": ["<skill>"],
    "skills_betrouwbaarheid": 0.0-1.0,

    "evaluatiefase": "<kies uit lijst>",
    "evaluatiefase_betrouwbaarheid": 0.0-1.0,

    "databronnen": ["<bron>"],
    "databronnen_betrouwbaarheid": 0.0-1.0,

    "opdrachtgrootte": "<kies uit lijst>",
    "opdrachtgrootte_betrouwbaarheid": 0.0-1.0,

    "causaliteitsvraag": true/false,
    "herhalingskans": "hoog/laag/onduidelijk",
    "consortiumkans": true/false,

    "toelichting": "<max 25 woorden>"
  }},
  ...
]

Regels:
- methodiek_kwantitatief is true voor: Quasi-experimenteel, RCT, Statistisch/econometrisch, MKBA
- causaliteitsvraag is true als de tekst expliciet vraagt om een causaal effect aan te tonen
- herhalingskans is hoog als het een monitor, panel of periodieke meting betreft
- consortiumkans is true als de opdracht zowel kwalitatief als kwantitatief werk vereist
- betrouwbaarheid per dimensie: 1.0 = expliciete vermelding in tekst, 0.5 = redelijke afleiding, 0.2 = gok
- Geef altijd exact evenveel objecten terug als records
- Antwoord ALLEEN met de JSON-array"""


# ── Classificatie uitvoeren ───────────────────────────────────────────────────

def classificeer_batch(client: anthropic.Anthropic, records: list[dict], batch_nr: int) -> list[dict]:
    prompt = bouw_classificatieprompt(records)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=12000,
            system=SYSTEEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        ruwe_output = response.content[0].text.strip()

        if ruwe_output.startswith("```"):
            ruwe_output = ruwe_output.split("```")[1]
            if ruwe_output.startswith("json"):
                ruwe_output = ruwe_output[4:]

        classificaties = json.loads(ruwe_output)
        print(f"  Batch {batch_nr}: {len(classificaties)} classificaties ontvangen")
        return classificaties

    except json.JSONDecodeError as e:
        print(f"  [FOUT] Batch {batch_nr}: ongeldige JSON — {e}")
        return _lege_classificaties(len(records))
    except anthropic.APIError as e:
        print(f"  [FOUT] Batch {batch_nr}: API-fout — {e}")
        return _lege_classificaties(len(records))


def _lege_classificaties(n: int) -> list[dict]:
    return [
        {
            "index": i,
            "thema": "Overig / onduidelijk",
            "thema_betrouwbaarheid": 0.0,
            "subthema": "",
            "methodiek": "Onduidelijk / niet gespecificeerd",
            "methodiek_betrouwbaarheid": 0.0,
            "methodiek_kwantitatief": False,
            "skills": [],
            "skills_betrouwbaarheid": 0.0,
            "evaluatiefase": "Onduidelijk",
            "evaluatiefase_betrouwbaarheid": 0.0,
            "databronnen": [],
            "databronnen_betrouwbaarheid": 0.0,
            "opdrachtgrootte": "Onduidelijk",
            "opdrachtgrootte_betrouwbaarheid": 0.0,
            "causaliteitsvraag": False,
            "herhalingskans": "onduidelijk",
            "consortiumkans": False,
            "toelichting": "Classificatie mislukt door API-fout",
        }
        for i in range(n)
    ]


def gemiddelde_betrouwbaarheid(cl: dict) -> float:
    scores = [
        cl.get("thema_betrouwbaarheid", 0),
        cl.get("methodiek_betrouwbaarheid", 0),
        cl.get("evaluatiefase_betrouwbaarheid", 0),
    ]
    scores = [s for s in scores if s is not None]
    return round(sum(scores) / len(scores), 2) if scores else 0.0


def classificeer_alle(records: list[dict], batch_grootte: int) -> list[dict]:
    client = anthropic.Anthropic()
    verwerkt = []
    totaal_batches = (len(records) + batch_grootte - 1) // batch_grootte

    print(f"\n[Classificatie] {len(records)} records in {totaal_batches} batches van {batch_grootte}")

    for batch_nr, start in enumerate(range(0, len(records), batch_grootte), 1):
        batch = records[start: start + batch_grootte]
        print(f"\n  Batch {batch_nr}/{totaal_batches} ({len(batch)} records)...")

        classificaties = classificeer_batch(client, batch, batch_nr)

        for classificatie in classificaties:
            idx = classificatie.get("index", 0)
            if idx < len(batch):
                record = dict(batch[idx])
                cl = {k: v for k, v in classificatie.items() if k != "index"}
                cl["betrouwbaarheid"] = gemiddelde_betrouwbaarheid(cl)
                record["classificatie"] = cl
                verwerkt.append(record)

        if batch_nr < totaal_batches:
            time.sleep(1)

    return verwerkt


# ── Opslaan ───────────────────────────────────────────────────────────────────

def sla_op_json(records: list[dict], bestandsnaam: str):
    pad = os.path.join(OUTPUT_DIR, bestandsnaam)
    with open(pad, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  -> {pad} ({len(records)} records)")


def sla_op_csv(records: list[dict], bestandsnaam: str):
    pad = os.path.join(OUTPUT_DIR, bestandsnaam)
    velden = [
        "id", "tenderned_kenmerk", "publicatie_type", "bron",
        "titel", "aanbestedende_dienst", "publicatiedatum", "sluitingsdatum",
        "gunningswaarde", "winnaar", "procedure_type", "europees", "url",
        "thema", "thema_betrouwbaarheid", "subthema",
        "methodiek", "methodiek_betrouwbaarheid", "methodiek_kwantitatief",
        "skills", "skills_betrouwbaarheid",
        "evaluatiefase", "evaluatiefase_betrouwbaarheid",
        "databronnen", "databronnen_betrouwbaarheid",
        "opdrachtgrootte", "opdrachtgrootte_betrouwbaarheid",
        "causaliteitsvraag", "herhalingskans", "consortiumkans",
        "betrouwbaarheid", "toelichting",
    ]

    with open(pad, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=velden, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            cl = r.get("classificatie") or {}
            rij = {k: r.get(k, "") for k in velden}
            rij.update({
                "thema": cl.get("thema", ""),
                "thema_betrouwbaarheid": cl.get("thema_betrouwbaarheid", ""),
                "subthema": cl.get("subthema", ""),
                "methodiek": cl.get("methodiek", ""),
                "methodiek_betrouwbaarheid": cl.get("methodiek_betrouwbaarheid", ""),
                "methodiek_kwantitatief": cl.get("methodiek_kwantitatief", False),
                "skills": " | ".join(cl.get("skills", [])),
                "skills_betrouwbaarheid": cl.get("skills_betrouwbaarheid", ""),
                "evaluatiefase": cl.get("evaluatiefase", ""),
                "evaluatiefase_betrouwbaarheid": cl.get("evaluatiefase_betrouwbaarheid", ""),
                "databronnen": " | ".join(cl.get("databronnen", [])),
                "databronnen_betrouwbaarheid": cl.get("databronnen_betrouwbaarheid", ""),
                "opdrachtgrootte": cl.get("opdrachtgrootte", ""),
                "opdrachtgrootte_betrouwbaarheid": cl.get("opdrachtgrootte_betrouwbaarheid", ""),
                "causaliteitsvraag": cl.get("causaliteitsvraag", False),
                "herhalingskans": cl.get("herhalingskans", ""),
                "consortiumkans": cl.get("consortiumkans", False),
                "betrouwbaarheid": cl.get("betrouwbaarheid", ""),
                "toelichting": cl.get("toelichting", ""),
            })
            writer.writerow(rij)

    print(f"  -> {pad}")


# ── Laden ─────────────────────────────────────────────────────────────────────

def laad_records(invoerbestanden: list[str]) -> list[dict]:
    alle = []
    for pad in invoerbestanden:
        if not os.path.exists(pad):
            print(f"  [SKIP] {pad} niet gevonden — draai eerst collect.py")
            continue
        with open(pad, encoding="utf-8") as f:
            records = json.load(f)
        print(f"  Geladen: {len(records)} records uit {pad}")
        alle.extend(records)

    voor = len(alle)
    alle = [r for r in alle if (r.get("titel") or r.get("omschrijving"))]
    na = len(alle)
    if voor != na:
        print(f"  Gefilterd: {voor - na} records zonder tekst verwijderd")

    return alle


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Classificeer aanbestedingen via LLM")
    parser.add_argument("--invoer", nargs="+", default=INPUT_BESTANDEN)
    parser.add_argument("--batch-grootte", type=int, default=BATCH_GROOTTE)
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=== Classificatiepijplijn ===")
    print(f"\n[1/3] Laden invoerbestanden...")
    records = laad_records(args.invoer)

    if not records:
        print("Geen records om te classificeren. Draai eerst collect.py.")
        return

    print(f"\n[2/3] Classificeren ({len(records)} records)...")
    geclassificeerd = classificeer_alle(records, args.batch_grootte)

    print(f"\n[3/3] Opslaan...")
    sla_op_json(geclassificeerd, "geclassificeerd.json")
    sla_op_csv(geclassificeerd, "geclassificeerd.csv")

    twijfelgevallen = [
        r for r in geclassificeerd
        if (r.get("classificatie") or {}).get("betrouwbaarheid", 1.0) < TWIJFEL_DREMPEL
    ]
    if twijfelgevallen:
        sla_op_json(twijfelgevallen, "twijfelgevallen.json")
        print(f"  -> {len(twijfelgevallen)} twijfelgevallen voor handmatige review")

    kwantitatief = sum(
        1 for r in geclassificeerd
        if (r.get("classificatie") or {}).get("methodiek_kwantitatief", False)
    )

    print(f"""
=== Samenvatting ===
Totaal geclassificeerd : {len(geclassificeerd)}
Kwantitatief           : {kwantitatief} ({100*kwantitatief//max(len(geclassificeerd),1)}%)
Twijfelgevallen        : {len(twijfelgevallen)}

Volgende stap: python koppel_winnaar.py
""")


if __name__ == "__main__":
    main()
