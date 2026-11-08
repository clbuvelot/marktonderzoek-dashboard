"""TenderNed-bulk per gunningspublicatie, met alle winnaars en percelen."""
import hashlib
import json
import re
import zipfile
from pathlib import Path

import openpyxl
from lxml import etree
from openpyxl.utils.datetime import from_excel
from opslag import schrijf_json

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
KOLOMMEN = {
    "ID publicatie": "id", "Tenderned kenmerk": "kenmerk", "Publicatiedatum": "datum",
    "Naam aanbesteding": "titel", "Naam Aanbestedende dienst": "opdrachtgever",
    "Referentienummer": "referentie", "Publicatie type": "type", "Publicatie soort": "soort",
    "Publicatie id(s)": "publicatie_ids", "Hoofd cpv code": "cpv", "Perceel cpv code": "perceel_cpv",
    "Naam gegunde onderneming": "naam", "ON kvknummer": "kvk", "Perceel volgnummer": "perceel",
    "Perceel titel": "perceel_titel", "Datum gunning": "datum_gunning", "Definitieve waarde - bedrag": "waarde",
}


def lees_bulk(pad):
    pad = Path(pad)
    with pad.open("rb") as f:
        digest = hashlib.file_digest(f, "sha256").hexdigest()
    cache = Path("data/.cache/bulk") / f"v1-{digest}.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    wb = openpyxl.load_workbook(pad, read_only=True, data_only=True)
    ws = wb["OpenData sheet"]
    kop = list(next(ws.iter_rows(values_only=True)))
    for kolom in ("ID publicatie", "Tenderned kenmerk", "Naam gegunde onderneming", "Publicatie type"):
        if kolom not in kop:
            raise ValueError(f"Bulk mist kolom {kolom}")
    pos = {openpyxl.utils.get_column_letter(kop.index(k) + 1): v for k, v in KOLOMMEN.items() if k in kop}
    strings, sheet_path, epoch = ws._shared_strings, ws._worksheet_path.lstrip("/"), wb.epoch
    wb.close()
    publicaties = {}
    with zipfile.ZipFile(pad) as z, z.open(sheet_path) as f:
        for _, node in etree.iterparse(f, events=("end",), tag=NS + "row"):
            nummer = int(node.get("r"))
            row = {}
            if nummer > 1:
                for cell in node:
                    col = re.sub(r"\d", "", cell.get("r", ""))
                    naam = pos.get(col)
                    if not naam:
                        continue
                    v = cell.find(NS + "v")
                    tekst = v.text if v is not None else ""
                    if cell.get("t") == "s" and tekst:
                        tekst = strings[int(tekst)]
                    elif cell.get("t") == "inlineStr":
                        tekst = "".join(cell.itertext())
                    elif naam in ("datum", "datum_gunning") and tekst:
                        try:
                            tekst = from_excel(float(tekst), epoch).isoformat()
                        except (ValueError, OverflowError):
                            pass
                    row[naam] = tekst
                if (row.get("type") == "Aankondiging gegunde opdracht"
                        and row.get("datum", "") >= "2019-01-01"):
                    rid = str(row["id"])
                    if rid not in publicaties:
                        publicaties[rid] = {k: row.get(k, "") for k in
                                            ("id", "kenmerk", "datum", "titel", "opdrachtgever", "referentie", "soort")}
                        publicaties[rid].update({"publicatie_ids": [], "cpv_codes": [], "winnaar_rijen": [],
                                                "bronbestand": pad.name, "bron_sha256": digest})
                    pub = publicaties[rid]
                    pub["publicatie_ids"] = sorted(set(pub["publicatie_ids"]) | set(re.findall(r"\d+", row.get("publicatie_ids", ""))))
                    pub["cpv_codes"] = sorted(set(pub["cpv_codes"]) | {row[k] for k in ("cpv", "perceel_cpv") if row.get(k)})
                    if row.get("naam"):
                        pub["winnaar_rijen"].append({k: row.get(k, "") for k in
                                                     ("naam", "kvk", "perceel", "perceel_titel", "datum_gunning", "waarde")} | {"bronrij": nummer})
            node.clear()
            while node.getprevious() is not None:
                del node.getparent()[0]
            if nummer % 50000 == 0:
                print(f"  Bulk {pad.name}: {nummer} rijen", flush=True)
    resultaat = list(publicaties.values())
    schrijf_json(cache, resultaat)
    return resultaat
