"""Leesbare audit van TenderNed-bronrijen; wijzigt de dashboarddataset niet.

Gebruik: python onderzoek_ecorys.py --excel bestand.xlsx --label historisch
Uitvoer: docs/onderzoek_ecorys/{label}.json
"""
import argparse
import csv
import json
import re
import zipfile
from collections import Counter
from pathlib import Path

import openpyxl
from lxml import etree
from openpyxl.utils.datetime import from_excel

from collect import RELEVANTE_CPV_CODES

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
VELDEN = [
    "ID publicatie", "Tenderned kenmerk", "Publicatiedatum",
    "Naam Aanbestedende dienst", "Naam aanbesteding", "URL TenderNed",
    "Publicatie soort", "Publicatie type", "Hoofd cpv code", "Perceel cpv code",
    "Naam gegunde onderneming", "ON kvknummer", "Datum gunning", "Datum besluit gunning",
    "Perceel volgnummer", "Perceel titel", "Publicatie id(s)", "Procedure",
    "Geimporteerd", "Tsender", "Nationaal of Europees", "Definitieve waarde - bedrag",
]


def sleutel(waarde):
    return re.sub(r"\.0$", "", str(waarde or "").strip())


def scan(pad, label):
    dataset = json.loads(Path("data/geclassificeerd_met_winnaar.json").read_text(encoding="utf-8"))
    kenmerken = {sleutel(r.get("tenderned_kenmerk")) for r in dataset}
    ids = {sleutel(r.get("id")) for r in dataset}
    ids.update(sleutel(r.get("gunning_publicatie_id")) for r in dataset)
    ids.discard("")
    wb = openpyxl.load_workbook(pad, read_only=True, data_only=True)
    sheet = wb["OpenData sheet"]
    kop = list(next(sheet.iter_rows(values_only=True)))
    kolommen = {openpyxl.utils.get_column_letter(kop.index(v) + 1): v for v in VELDEN if v in kop}
    strings = sheet._shared_strings
    sheet_path = sheet._worksheet_path.lstrip("/")
    epoch = wb.epoch
    wb.close()
    ecorys = []
    gekoppeld = []
    relevante_publicaties = {}
    soort = Counter()
    namen = Counter()
    cpv = {c[:8] for c in RELEVANTE_CPV_CODES}
    aantal = 0
    with zipfile.ZipFile(pad) as z, z.open(sheet_path) as xml:
        for _, node in etree.iterparse(xml, events=("end",), tag=NS + "row"):
            nummer = int(node.get("r"))
            if nummer == 1:
                continue
            row = {}
            for cell in node:
                col = re.sub(r"\d", "", cell.get("r", ""))
                naam = kolommen.get(col)
                if not naam:
                    continue
                value = cell.find(NS + "v")
                tekst = value.text if value is not None else None
                if cell.get("t") == "s" and tekst is not None:
                    tekst = strings[int(tekst)]
                elif cell.get("t") == "inlineStr":
                    tekst = "".join(cell.itertext())
                elif tekst and ("datum" in naam.casefold() or naam == "Datum gunning"):
                    try:
                        tekst = from_excel(float(tekst), epoch).isoformat()
                    except (ValueError, OverflowError):
                        pass
                row[naam] = tekst
            row["excel_rij"] = nummer
            aantal += 1
            kenmerk = sleutel(row.get("Tenderned kenmerk"))
            pub_id = sleutel(row.get("ID publicatie"))
            winnaar = str(row.get("Naam gegunde onderneming") or "")
            soort[str(row.get("Publicatie type"))] += 1
            kvk = re.sub(r"\D", "", str(row.get("ON kvknummer") or ""))
            if "ecory" in winnaar.casefold() or kvk == "24316726":
                ecorys.append(row)
                namen[winnaar] += 1
            if kenmerk in kenmerken or pub_id in ids:
                gekoppeld.append(row)
            codes = str(row.get("Hoofd cpv code") or "") + " " + str(row.get("Perceel cpv code") or "")
            if any(c in codes for c in cpv):
                relevante_publicaties.setdefault(pub_id, {k: v for k, v in row.items()
                                                          if k not in ("Naam gegunde onderneming", "ON kvknummer")})
            node.clear()
            while node.getprevious() is not None:
                del node.getparent()[0]
            if aantal % 50000 == 0:
                print(f"{label}: {aantal} rijen; {len(ecorys)} Ecorys-rijen", flush=True)
    resultaat = {
        "bronbestand": str(pad), "rijen": aantal, "publicatietypen": dict(soort),
        "ecorys_namen": dict(namen), "ecorys_rijen": ecorys,
        "dashboard_bronrijen": gekoppeld,
        "relevante_publicaties": list(relevante_publicaties.values()),
    }
    doel = Path("docs/onderzoek_ecorys")
    doel.mkdir(parents=True, exist_ok=True)
    (doel / f"{label}.json").write_text(json.dumps(resultaat, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"label": label, "rijen": aantal, "ecorys_namen": dict(namen),
                      "ecorys_kenmerken": len({sleutel(r.get('Tenderned kenmerk')) for r in ecorys}),
                      "dashboard_bronrijen": len(gekoppeld), "relevante_publicaties": len(relevante_publicaties)}, ensure_ascii=False), flush=True)


def vergelijk():
    """Combineer bronnen met expliciet bewezen koppelingen; uitsluitend audituitvoer."""
    doel = Path("docs/onderzoek_ecorys")
    bronnen = {naam: json.loads((doel / f"{naam}.json").read_text(encoding="utf-8"))
               for naam in ("historisch", "actueel")}
    dataset = json.loads(Path("data/geclassificeerd_met_winnaar.json").read_text(encoding="utf-8"))
    per_kenmerk = {sleutel(r.get("tenderned_kenmerk")): r for r in dataset}
    per_id = {sleutel(r.get("id")): r for r in dataset}
    groepen = {}
    relevante = []
    for naam, bron in bronnen.items():
        relevante += bron["relevante_publicaties"]
        for r in bron["ecorys_rijen"]:
            key = sleutel(r.get("Tenderned kenmerk"))
            groepen.setdefault(key, []).append({**r, "bron": naam})
    # Handmatig gevonden, daarna gecontroleerd op gelijke interne referentie,
    # projectlink en eForms-procedure-UUID in de officiële publicatie-PDF's.
    bewezen = {
        "361925": ("322538", "290115c6-f318-4bf4-b23e-f3b6cc461a3c"),
        "365375": ("335914", "a9389d65-d380-45f9-9eae-9d8a1569d209"),
    }
    codes = {c[:8] for c in RELEVANTE_CPV_CODES}
    result = []
    for kenmerk, rijen in groepen.items():
        r = sorted(rijen, key=lambda x: (x["bron"] == "actueel", x.get("Publicatiedatum", "")), reverse=True)[0]
        rid = sleutel(r.get("ID publicatie"))
        api_pad = doel / f"api-{rid}.json"
        api = json.loads(api_pad.read_text(encoding="utf-8")) if api_pad.exists() else {}
        cpvs = sorted({c["code"] for c in api.get("cpvCodes", [])} or
                      {str(x.get(k)) for x in rijen for k in ("Hoofd cpv code", "Perceel cpv code") if x.get(k)})
        geselecteerd = any(c[:8] in codes for c in cpvs)
        huidig = per_kenmerk.get(kenmerk)
        reden = ""
        bewijs = ""
        if str(r.get("Publicatiedatum", "")) < "2019-01-01":
            reden = "Gunning voor 2019"
        elif rid == "284952":
            reden = "Geen zekere winnaar: Ecorys of NIVEL"
        elif huidig:
            if "ecory" in str(huidig.get("winnaar", "")).lower():
                reden = "Aanwezig als eerste winnaar; buiten top 20"
            elif any("ecory" in x.lower() for x in huidig.get("extra_winnaars", [])):
                reden = "Aanwezig als extra winnaar; telt niet mee"
            else:
                reden = "Record aanwezig; Ecorys ontbreekt bij winnaars"
        elif rid in bewezen:
            aao_id, uuid = bewezen[rid]
            huidig = per_id[aao_id]
            uitvraag = json.loads((doel / f"api-{aao_id}.json").read_text(encoding="utf-8"))
            assert uitvraag["referentieNummer"] == api["referentieNummer"]
            assert uitvraag["urlTsenderWebsite"] == api["urlTsenderWebsite"]
            for pub in (rid, aao_id):
                assert uuid in (doel / f"publicatie-{pub}.txt").read_text(encoding="utf-8")
            reden = "Bewezen koppelgat: ander TenderNed-kenmerk"
            bewijs = f"Procedure-UUID {uuid}; referentie {api['referentieNummer']}; identieke projectlink"
        elif not geselecteerd:
            reden = "Buiten huidige CPV-selectie"
        else:
            aao = [x for x in relevante if sleutel(x.get("Tenderned kenmerk")) == kenmerk
                   and x.get("Publicatie type") == "Aankondiging opdracht"]
            if aao and all(str(x.get("Publicatiedatum", "")) < "2019-01-01" for x in aao):
                reden = "Bijbehorende uitvraag voor 2019"
            elif aao:
                reden = "Historisch dekkingsgat; uitvraag en gunning in Excel"
            else:
                reden = "Gunning in bron; geen bijbehorende uitvraag aangetroffen"
        result.append({
            "gunning_publicatie_id": rid, "gunning_kenmerk": kenmerk,
            "publicatiedatum_gunning": r.get("Publicatiedatum"),
            "titel": r.get("Naam aanbesteding"), "opdrachtgever": r.get("Naam Aanbestedende dienst"),
            "ecorys_bronnamen": " | ".join(sorted({x.get("Naam gegunde onderneming", "") for x in rijen})),
            "cpv_codes": " | ".join(cpvs), "past_cpvselectie": geselecteerd,
            "dashboard_id": huidig.get("id") if huidig else "",
            "dashboard_kenmerk": huidig.get("tenderned_kenmerk") if huidig else "",
            "dashboard_eerste_winnaar": huidig.get("winnaar") if huidig else "",
            "bevinding": reden, "bewijs": bewijs,
            "bronrijen": " | ".join(f"{x['bron']}:{x['excel_rij']}" for x in rijen),
            "bron_url": f"https://www.tenderned.nl/aankondigingen/overzicht/{rid}",
        })
    result.sort(key=lambda x: x["publicatiedatum_gunning"])
    (doel / "vergelijking.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with (doel / "vergelijking.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(result[0]), delimiter=";")
        writer.writeheader()
        writer.writerows(result)
    print(json.dumps(dict(Counter(r["bevinding"] for r in result)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--excel")
    parser.add_argument("--label", choices=["historisch", "actueel"])
    parser.add_argument("--vergelijk", action="store_true")
    args = parser.parse_args()
    if args.vergelijk:
        vergelijk()
    elif args.excel and args.label:
        scan(Path(args.excel), args.label)
    else:
        parser.error("Gebruik --excel met --label, of --vergelijk.")
