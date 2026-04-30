"""
collect.py
----------
Haalt aanbestedingsdata op uit TenderNed en geplande evaluaties uit de
Kamerstukken API (officielebekendmakingen.nl). Slaat ruwe output op als JSON.

Gebruik:
    python collect.py --bron tenderned --jaar-vanaf 2019
    python collect.py --bron kamerstukken
    python collect.py --bron beide --jaar-vanaf 2019

Output:
    data/raw_tenderned.json
    data/raw_kamerstukken.json
"""

import requests
import json
import time
import argparse
import os
from datetime import datetime

# ── Configuratie ──────────────────────────────────────────────────────────────

OUTPUT_DIR = "data"
DOCUMENTEN_DIR = "data/documenten"
TENDERNED_DOC_BASE = "https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties"

# CPV-codes voor onderzoek, evaluatie en advies
RELEVANTE_CPV_CODES = [
    # Onderzoek uitvoeren
    "73000000-2",  # Onderzoek en ontwikkeling en aanverwante adviesdiensten
    "73100000-3",  # Onderzoek en experimentele ontwikkeling
    "73110000-6",  # Uitvoeren van onderzoek  ← MDT-type opdrachten
    "73120000-9",  # Experimentele ontwikkeling
    "73200000-4",  # Adviesdiensten voor onderzoek en ontwikkeling
    "73210000-7",  # Adviesdiensten voor onderzoek
    "73220000-0",  # Adviesdiensten voor ontwikkeling
    "73300000-5",  # Ontwerp en uitvoering van onderzoek en ontwikkeling
    # Markt- en beleidsonderzoek
    "79310000-0",  # Marktonderzoek
    "79311000-7",  # Uitvoeren van enquêtes
    "79311400-1",  # Economisch onderzoek
    "79315000-5",  # Sociaal onderzoek
    "79419000-4",  # Adviesdiensten voor evaluatie
]

# Publicatietypen om op te halen
# AGO = Aankondiging Gegunde Opdracht (historisch, met winnaar)
# AOO = Aankondiging van een Opdracht (lopend, zonder winnaar)
PUBLICATIE_TYPEN = {
    "AGO": "gegund",
    "AOO": "lopend",
}

# Zoektermen voor Kamerstukken (geplande evaluaties)
ZOEKTERMEN_KAMERSTUKKEN = [
    "beleidsdoorlichting", "evaluatie", "doeltreffendheid",
    "periodieke rapportage", "syntheseonderzoek",
]

KAMERSTUKKEN_DOCUMENT_TYPES = [
    "Kamerstuk",       # begrotingen, kamerbrieven
    "Staatsblad",
]


# ── TenderNed ────────────────────────────────────────────────────────────────

TENDERNED_TNS_BASE = "https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties"


def fetch_tenderned(jaar_vanaf: int) -> list[dict]:
    """
    Haalt aanbestedingen op uit TenderNed:
    1. Eerst alle AAO's (uitvragen) ophalen — dit zijn de basis
    2. Dan AGO's (gunningen) ophalen — alleen voor uitvragen die als AAO zijn gevonden
    3. Merge: AGO vervangt AAO als gunning beschikbaar is
    """
    datum_vanaf = f"{jaar_vanaf}-01-01"

    # ── Stap 1: AAO's ophalen ────────────────────────────────────────────────
    print(f"\n[TenderNed] Ophalen uitvragen (AAO) vanaf {datum_vanaf}")
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
            batch, totaal = _tenderned_request(params, label=f"AAO CPV {cpv}, p{pagina}")
            if not batch:
                break
            for item in batch:
                pub_id = str(item.get("id", item.get("publicatieId", "")))
                kenmerk = str(item.get("kenmerk", "") or pub_id)
                if pub_id:
                    item["_publicatie_type"] = "lopend"
                    # Bij duplicaat kenmerk: meest recente bewaren
                    if kenmerk not in aao_per_kenmerk:
                        aao_per_kenmerk[kenmerk] = item
            if pagina == 0 and totaal:
                print(f"    Totaal beschikbaar: {totaal}")
            if len(batch) < 50:
                break
            pagina += 1
            time.sleep(0.3)

    print(f"  → {len(aao_per_kenmerk)} unieke uitvragen gevonden")

    # ── Stap 2: AGO's ophalen en koppelen ───────────────────────────────────
    print(f"\n[TenderNed] Ophalen gunningen (AGO) vanaf {datum_vanaf}")
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
            batch, totaal = _tenderned_request(params, label=f"AGO CPV {cpv}, p{pagina}")
            if not batch:
                break
            for item in batch:
                kenmerk = str(item.get("kenmerk", "") or "")
                if kenmerk and kenmerk in aao_per_kenmerk:
                    # Bewaar de originele AAO publicatiedatum
                    item["_aao_publicatiedatum"] = aao_per_kenmerk[kenmerk].get("publicatieDatum", "")
                    item["_publicatie_type"] = "gegund"
                    aao_per_kenmerk[kenmerk] = item
                    gekoppeld += 1
            if pagina == 0 and totaal:
                print(f"    Totaal beschikbaar: {totaal}")
            if len(batch) < 50:
                break
            pagina += 1
            time.sleep(0.3)

    print(f"  → {gekoppeld} gunningen gekoppeld aan uitvragen")

    records = list(aao_per_kenmerk.values())
    gegund = sum(1 for r in records if r.get("_publicatie_type") == "gegund")
    lopend = sum(1 for r in records if r.get("_publicatie_type") == "lopend")
    print(f"\n[TenderNed] Totaal: {len(records)} ({gegund} gegund, {lopend} lopend)")
    return records


def _tenderned_request(params: dict, label: str) -> tuple[list[dict], int]:
    """Voert één paginaverzoek uit op de TenderNed TNS API."""
    try:
        response = requests.get(TENDERNED_TNS_BASE, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        items = data.get("contents", data.get("content", []))
        totaal = data.get("totalElements", 0)
        print(f"  {label}: {len(items)} resultaten")
        return items, totaal
    except requests.exceptions.HTTPError as e:
        print(f"  [FOUT] {label}: HTTP {e.response.status_code} — {e.response.text[:200]}")
        return [], 0
    except Exception as e:
        print(f"  [FOUT] {label}: {e}")
        return [], 0


def normalize_tenderned(record: dict) -> dict:
    """
    Zet een ruwe TNS-record om naar een uniforme structuur
    voor de classificatiestap. Slaat alle beschikbare API-velden op.
    Veldnamen gebaseerd op werkelijke TNS API-response.
    """
    pub_id = str(record.get("publicatieId", ""))
    tn_kenmerk = str(record.get("kenmerk", "") or "")
    return {
        # Identifiers
        "id": pub_id,
        "tenderned_kenmerk": tn_kenmerk,
        "bron": "tenderned",

        # Publicatiemetadata
        "publicatie_type": record.get("_publicatie_type", "gegund"),
        "type_publicatie_code": (record.get("typePublicatie") or {}).get("code", ""),
        "type_publicatie_omschrijving": (record.get("typePublicatie") or {}).get("omschrijving", ""),
        "publicatiecode_code": (record.get("publicatiecode") or {}).get("code", ""),
        "publicatiecode_omschrijving": (record.get("publicatiecode") or {}).get("omschrijving", ""),
        "publicatiestatus": (record.get("publicatiestatus") or {}).get("code", ""),
        "is_vroegtijdige_beeindiging": record.get("isVroegtijdigeBeeindiging", False),
        "is_eforms_wijziging": record.get("isEformsWijziging", False),
        "digitaal": record.get("digitaal", None),
        "europees": record.get("europees", None),

        # Inhoud
        "titel": record.get("aanbestedingNaam", ""),
        "omschrijving": record.get("opdrachtBeschrijving", ""),

        # Opdrachtgever
        "aanbestedende_dienst": record.get("opdrachtgeverNaam", ""),

        # Opdracht kenmerken
        "type_opdracht_code": (record.get("typeOpdracht") or {}).get("code", ""),
        "type_opdracht_omschrijving": (record.get("typeOpdracht") or {}).get("omschrijving", ""),
        "procedure_type": (record.get("procedure") or {}).get("omschrijving", ""),
        "procedure_code": (record.get("procedure") or {}).get("code", ""),

        # Datums
        # publicatiedatum = altijd de datum van de originele uitvraag (AAO)
        # gunningsdatum   = datum van de gunningsaankondiging (AGO), leeg als niet gegund
        "publicatiedatum": record.get("_aao_publicatiedatum", "") or record.get("publicatieDatum", ""),
        "gunningsdatum": record.get("publicatieDatum", "") if record.get("_publicatie_type") == "gegund" else "",
        "sluitingsdatum": record.get("sluitingsDatum", "") or record.get("sluitingsdatumAanbesteding", ""),

        # Financieel
        "gunningswaarde": record.get("gunningswaarde", None),

        # Winnaar (wordt later ingevuld via bulk Excel)
        "winnaar": _extract_winnaar(record),

        # CPV
        "cpv_codes": _extract_cpv(record),

        # Links
        "url": f"https://www.tenderned.nl/aankondigingen/overzicht/{pub_id}",
        "tsender_link": (record.get("link") or {}).get("href", "") if isinstance(record.get("link"), dict) else str(record.get("link", "") or ""),

        # Documenten (worden apart opgeslagen in data/documenten/{id}.txt)
        "documenten_geladen": os.path.exists(os.path.join(DOCUMENTEN_DIR, f"{pub_id}.txt")),

        # Classificatie (wordt ingevuld door classify.py)
        "classificatie": None,
    }


def _extract_winnaar(record: dict) -> str:
    for sleutel in ("gegundAan", "contractors", "inschrijvers", "awards"):
        waarde = record.get(sleutel)
        if waarde and isinstance(waarde, list):
            eerste = waarde[0]
            if isinstance(eerste, dict):
                return eerste.get("naam", "") or eerste.get("name", "") or eerste.get("officialName", "")
    return ""


def _extract_cpv(record: dict) -> list[str]:
    codes = record.get("cpvCodes") or record.get("cpvCode") or []
    if isinstance(codes, list):
        return [c.get("code", c) if isinstance(c, dict) else str(c) for c in codes]
    return [str(codes)] if codes else []


# ── Kamerstukken API ──────────────────────────────────────────────────────────

KAMERSTUKKEN_BASE = "https://zoek.officielebekendmakingen.nl"


def fetch_kamerstukken() -> list[dict]:
    """
    Zoekt in officielebekendmakingen.nl naar documenten die betrekking hebben
    op geplande of uitgevoerde beleidsevaluaties. Gebruikt SRU-zoekprotocol.
    """
    resultaten = {}
    print("\n[Kamerstukken] Ophalen beleidsevaluatiedocumenten")

    for term in ZOEKTERMEN_KAMERSTUKKEN:
        start = 0
        while True:
            params = {
                "operation": "searchRetrieve",
                "version": "1.2",
                "query": f'tekst="{term}" AND (documenttype=Kamerstuk)',
                "startRecord": start,
                "maximumRecords": 100,
                "recordSchema": "oaf",
                "x-connection": "officielepublicaties",
            }
            batch = _kamerstukken_request(params, label=f'term "{term}", start {start}')
            if not batch:
                break
            for item in batch:
                resultaten[item["id"]] = item
            if len(batch) < 100:
                break
            start += 100
            time.sleep(0.5)

    records = list(resultaten.values())
    print(f"[Kamerstukken] Totaal unieke documenten: {len(records)}")
    return records


def _kamerstukken_request(params: dict, label: str) -> list[dict]:
    """Voert één SRU-zoekopdracht uit op officielebekendmakingen.nl."""
    url = f"{KAMERSTUKKEN_BASE}/SRU/Search"
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        # SRU retourneert XML; parse met minimale afhankelijkheden
        from xml.etree import ElementTree as ET
        root = ET.fromstring(response.text)
        ns = {
            "srw": "http://www.loc.gov/zing/srw/",
            "oaf": "http://standaarden.overheid.nl/oaf/1.0/",
        }
        records = []
        for rec in root.findall(".//srw:record", ns):
            item = _parse_kamerstuk_record(rec, ns)
            if item:
                records.append(item)

        print(f"  {label}: {len(records)} resultaten")
        return records

    except Exception as e:
        print(f"  [FOUT] {label}: {e}")
        return []


def _parse_kamerstuk_record(record, ns: dict) -> dict | None:
    """Extraheert relevante velden uit een SRU XML-record."""
    try:
        data = record.find(".//oaf:meta", ns)
        if data is None:
            return None

        def get(tag):
            el = data.find(f"oaf:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        doc_id = get("identifier") or get("dcidentifier")
        if not doc_id:
            return None

        return {
            "id": doc_id,
            "bron": "kamerstukken",
            "titel": get("title") or get("dctitle"),
            "omschrijving": get("description") or get("dcdescription"),
            "aanbestedende_dienst": get("creator") or get("dccreator"),
            "publicatiedatum": get("date") or get("dcdate"),
            "gunningswaarde": None,
            "winnaar": "",
            "cpv_codes": [],
            "procedure_type": get("type") or get("dctype"),
            "url": f"https://zoek.officielebekendmakingen.nl/{doc_id}",
            "classificatie": None,
        }
    except Exception:
        return None


# ── Documenten ophalen ────────────────────────────────────────────────────────

def haal_documentenlijst_op(publicatie_id: str) -> list[dict]:
    """Haalt de lijst van beschikbare documenten op voor een publicatie."""
    url = f"{TENDERNED_DOC_BASE}/{publicatie_id}/documenten"
    try:
        r = requests.get(url, timeout=15, headers={"Accept": "application/json"})
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()
        return data.get("documenten", [])
    except Exception:
        return []


def haal_document_tekst_op(publicatie_id: str, document_id: str) -> str:
    """Downloadt en extraheert tekst uit een document via de download href."""
    url = f"https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties/{publicatie_id}/documenten/{document_id}/content"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return ""
        content_type = r.headers.get("Content-Type", "")
        if "pdf" in content_type.lower():
            return _extraheer_pdf_tekst(r.content)
        if "word" in content_type.lower() or "docx" in content_type.lower():
            return _extraheer_docx_tekst(r.content)
        return r.text[:50000]
    except Exception:
        return ""


def _extraheer_pdf_tekst(pdf_bytes: bytes) -> str:
    """Extraheert tekst uit PDF-bytes via pypdf."""
    try:
        import pypdf
        import io
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        tekst = []
        for pagina in reader.pages[:30]:  # max 30 pagina's
            tekst.append(pagina.extract_text() or "")
        return "\n".join(tekst)[:80000]  # max ~80k tekens
    except ImportError:
        # pypdf niet beschikbaar — sla PDF over
        return ""
    except Exception:
        return ""


def _is_binair(tekst: str) -> bool:
    """Detecteert of geëxtraheerde tekst eigenlijk binaire rommel is."""
    if not tekst:
        return True
    # Als meer dan 10% van de tekens niet-printbaar zijn, is het binair
    niet_printbaar = sum(1 for c in tekst[:500] if ord(c) > 127 or (ord(c) < 32 and c not in '\n\r\t'))
    return (niet_printbaar / min(len(tekst), 500)) > 0.1



def _extraheer_docx_tekst(docx_bytes: bytes) -> str:
    """Extraheert tekst uit Word-document bytes."""
    try:
        import zipfile, io
        from xml.etree import ElementTree as ET
        z = zipfile.ZipFile(io.BytesIO(docx_bytes))
        xml = z.read("word/document.xml")
        tree = ET.fromstring(xml)
        ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        tekst = " ".join(node.text for node in tree.iter(f"{ns}t") if node.text)
        return tekst[:80000]
    except Exception:
        return ""


def haal_alle_documenten_op(publicatie_id: str, max_docs: int = 5) -> str:
    """
    Haalt tekst op uit alle publieke documenten van een publicatie.
    Slaat op als data/documenten/{publicatie_id}.txt
    """
    os.makedirs(DOCUMENTEN_DIR, exist_ok=True)
    pad = os.path.join(DOCUMENTEN_DIR, f"{publicatie_id}.txt")

    if os.path.exists(pad):
        with open(pad, encoding="utf-8") as f:
            return f.read()

    documenten = haal_documentenlijst_op(publicatie_id)
    if not documenten:
        return ""

    HOGE_PRIORITEIT = {"selectieleidraad", "beschrijvend document", "programma van eisen",
                       "offerteaanvraag", "uitvraag", "rfp", "bestek", "nota van inlichtingen"}
    LAGE_PRIORITEIT = {"aanmeldingsformulier", "opgave gegevens", "uea", "uniform europees",
                       "verklaring", "handtekening", "combinant"}

    gesorteerd = []
    for doc in documenten:
        naam = (doc.get("documentNaam", "") or "").lower()
        href = (doc.get("links", {}).get("download", {}).get("href", "") or "")
        parts = href.rstrip("/content").split("/")
        doc_id = parts[-1] if parts else ""
        if not doc_id:
            continue
        bestandstype = (doc.get("typeDocument", {}).get("code", "") or "").lower()
        categorie = (doc.get("publicatieCategorie", {}).get("code", "") or "")

        # Sla zip en excel over
        if bestandstype in ("zip", "xlsx", "xls"):
            continue

        # Prioriteit: laag getal = hogere prioriteit
        if any(k in naam for k in HOGE_PRIORITEIT):
            prioriteit = 0
        elif categorie in {"DOC", "NVI"}:
            prioriteit = 1
        elif any(k in naam for k in LAGE_PRIORITEIT):
            prioriteit = 3
        else:
            prioriteit = 2

        gesorteerd.append((prioriteit, doc_id, naam, bestandstype))

    gesorteerd.sort()
    teksten = []
    for _, doc_id, naam, _ in gesorteerd[:max_docs]:
        tekst = haal_document_tekst_op(publicatie_id, doc_id)
        if tekst.strip() and not _is_binair(tekst):
            teksten.append(f"=== Document: {naam} ===\n{tekst.strip()}")
        time.sleep(0.2)

    gecombineerd = "\n\n".join(teksten)

    if gecombineerd.strip():
        with open(pad, "w", encoding="utf-8") as f:
            f.write(gecombineerd)

    return gecombineerd


# ── Deduplicatie ─────────────────────────────────────────────────────────────

def dedupliceer(records: list[dict]) -> list[dict]:
    """
    Verwijdert records zonder titel of omschrijving.
    AGO/AAO deduplicatie gebeurt al in fetch_tenderned.
    """
    voor = len(records)
    resultaat = [r for r in records if r.get("titel") or r.get("omschrijving")]
    na = len(resultaat)
    if voor != na:
        print(f"[Deduplicatie] {voor - na} records zonder tekst verwijderd")
    print(f"[Deduplicatie] {len(resultaat)} bruikbare aanbestedingen")
    return resultaat


# ── Opslaan ───────────────────────────────────────────────────────────────────

def sla_op(records: list[dict], bestandsnaam: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pad = os.path.join(OUTPUT_DIR, bestandsnaam)
    with open(pad, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\n[Opgeslagen] {len(records)} records → {pad}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Verzamel aanbestedingsdata voor marktonderzoek")
    parser.add_argument("--bron", choices=["tenderned", "kamerstukken", "beide"], default="beide")
    parser.add_argument("--jaar-vanaf", type=int, default=2019, help="Startjaar voor TenderNed (default: 2019)")
    args = parser.parse_args()

    if args.bron in ("tenderned", "beide"):
        records = fetch_tenderned(args.jaar_vanaf)
        genormaliseerd = [normalize_tenderned(r) for r in records]
        genormaliseerd = dedupliceer(genormaliseerd)

        # Documenten ophalen
        print(f"\n[Documenten] Ophalen voor {len(genormaliseerd)} publicaties...")
        os.makedirs(DOCUMENTEN_DIR, exist_ok=True)
        for i, record in enumerate(genormaliseerd):
            pub_id = record.get("id", "")
            if not pub_id:
                continue
            pad = os.path.join(DOCUMENTEN_DIR, f"{pub_id}.txt")
            if os.path.exists(pad):
                record["documenten_geladen"] = True
                continue
            tekst = haal_alle_documenten_op(pub_id)
            record["documenten_geladen"] = bool(tekst.strip())
            if (i + 1) % 10 == 0:
                print(f"  {i+1}/{len(genormaliseerd)} verwerkt...")
            time.sleep(0.3)

        sla_op(genormaliseerd, "raw_tenderned.json")

    if args.bron in ("kamerstukken", "beide"):
        records = fetch_kamerstukken()
        sla_op(records, "raw_kamerstukken.json")

    print("\nKlaar. Volgende stap: python classify.py")


if __name__ == "__main__":
    main()
