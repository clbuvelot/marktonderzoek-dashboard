"""TenderNed-publicaties koppelen met herleidbare identifiers, nooit op titel alleen."""
import io
import json
import re
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from pypdf import PdfReader
from opslag import schrijf_json

BASE = "https://www.tenderned.nl/papi/tenderned-rs-tns/v2/publicaties"
CACHE = Path("data/.cache/publicaties")


def norm(tekst):
    return re.sub(r"[^\w]+", "", unicodedata.normalize("NFKC", str(tekst or "")).casefold())


def project_sleutel(url):
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    query = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    if (host == "eu-supply.com" or host.endswith(".eu-supply.com")) and query.get("pid"):
        return "eu-supply:pid:" + query["pid"][0]
    if (host == "negometrix.com" or host.endswith(".negometrix.com")) and query.get("tenderid"):
        return "negometrix:tender:" + query["tenderid"][0]
    return ""


def procedure_uuid(tekst):
    tekst = unicodedata.normalize("NFKC", tekst or "")
    patroon = (r"(?:Identificatiecode\s+van\s+de\s+procedure|Procedure\s+identifier)"
               r"\s*:?\s*([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})")
    gevonden = set(re.findall(patroon, tekst, re.I))
    return next(iter(gevonden)).lower() if len(gevonden) == 1 else ""


def _get(url):
    for poging in range(3):
        try:
            response = requests.get(url, timeout=45)
            if response.status_code == 404:
                return response
            response.raise_for_status()
            return response
        except requests.RequestException:
            if poging == 2:
                raise
            time.sleep(2 ** poging)


def haal_identiteit(pub_id):
    pub_id = str(pub_id)
    if not pub_id.isdigit():
        raise ValueError("Ongeldig TenderNed-publicatie-id")
    pad = CACHE / f"{pub_id}.json"
    if pad.exists():
        cached = json.loads(pad.read_text(encoding="utf-8"))
        dagen = 1 if cached.get("http_status") == 404 else 30
        if time.time() - pad.stat().st_mtime < dagen * 86400:
            return cached
    response = _get(f"{BASE}/{pub_id}")
    resultaat = {"publicatie_id": pub_id, "http_status": response.status_code,
                 "geraadpleegd_op": datetime.now(timezone.utc).isoformat()}
    if response.status_code == 200:
        data = response.json()
        if str(data.get("publicatieId")) != pub_id:
            raise ValueError(f"Verkeerd publicatie-id ontvangen voor {pub_id}")
        resultaat.update({
            "kenmerk": str(data.get("kenmerk") or ""),
            "referentie_nummer": str(data.get("referentieNummer") or ""),
            "project_sleutel": project_sleutel(data.get("urlTsenderWebsite")),
            "project_url": data.get("urlTsenderWebsite") or "",
            "titel": data.get("aanbestedingNaam") or "",
            "opdrachtgever": data.get("opdrachtgeverNaam") or "",
            "publicatiedatum": data.get("publicatieDatum") or "",
            "cpv_codes": sorted({c["code"] for c in data.get("cpvCodes", []) if isinstance(c, dict) and c.get("code")}),
            "publicatie_type_bron": data.get("typePublicatie"),
            "vroegtijdig_beeindigd": bool(data.get("isVroegtijdigBeeindigd")),
        })
        # Een procedure-UUID zit niet in de JSON; de officiële publicatie wel.
        if str(data.get("publicatieDatum", "")) >= "2023-01-01":
            pdf = _get(f"{BASE}/{pub_id}/pdf")
            if pdf.status_code == 200:
                if not pdf.content.startswith(b"%PDF"):
                    raise ValueError(f"Geen publicatie-PDF ontvangen voor {pub_id}")
                reader = PdfReader(io.BytesIO(pdf.content))
                tekst = "\n".join(p.extract_text() or "" for p in reader.pages[:3])
                resultaat["procedure_uuid"] = procedure_uuid(tekst)
                # Een voornemen is geen bevestigde gunning.
                resultaat["voornemen"] = "vrijwillige transparantie vooraf" in tekst.casefold()
    schrijf_json(pad, resultaat)
    return resultaat


def haal_identiteiten(ids, workers=4):
    ids = sorted({str(i) for i in ids if i})
    resultaat = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, metadata in enumerate(pool.map(haal_identiteit, ids), 1):
            resultaat[metadata["publicatie_id"]] = metadata
            if i % 50 == 0 or i == len(ids):
                print(f"  Publicatie-identiteit: {i}/{len(ids)}", flush=True)
    return resultaat


def verrijk_records(records):
    def verlopen(r):
        meta = r.get("koppeling_metadata", {})
        try:
            sinds = datetime.fromisoformat(meta["geraadpleegd_op"])
        except (KeyError, ValueError):
            return True
        dagen = 1 if meta.get("http_status") == 404 else 30
        return (datetime.now(timezone.utc) - sinds).total_seconds() >= dagen * 86400
    nodig = [r["id"] for r in records if verlopen(r)]
    meta = haal_identiteiten(nodig)
    for r in records:
        if r["id"] in meta:
            r["koppeling_metadata"] = meta[r["id"]]
        identiteit = r.get("koppeling_metadata", {})
        if identiteit.get("cpv_codes"):
            r["cpv_codes"] = identiteit["cpv_codes"]


def maak_index(records):
    index = {k: defaultdict(set) for k in ("id", "kenmerk", "uuid", "ref_project")}
    for i, r in enumerate(records):
        for pub in [r.get("id"), r.get("gunning_publicatie_id"), *r.get("gunning_publicatie_ids", [])]:
            if pub:
                index["id"][str(pub)].add(i)
        for kenmerk in [r.get("tenderned_kenmerk"), *r.get("gekoppelde_kenmerken", [])]:
            if kenmerk:
                index["kenmerk"][str(kenmerk)].add(i)
        meta = r.get("koppeling_metadata", {})
        if meta.get("procedure_uuid"):
            index["uuid"][meta["procedure_uuid"]].add(i)
        ref, project = norm(meta.get("referentie_nummer")), meta.get("project_sleutel")
        if ref and project:
            index["ref_project"][(ref, project)].add(i)
    return index


def zoek_koppeling(publicatie, records, index):
    """Geeft index en bewijs, of een verklaarde afwijzing. Conflicten gaan naar review."""
    meta = publicatie.get("metadata", {})
    uuid = meta.get("procedure_uuid")
    ref = norm(meta.get("referentie_nummer"))
    project = meta.get("project_sleutel")
    signalen = {}
    opties = [
        ("publicatie_id", index["id"].get(str(publicatie.get("id")), set())),
        ("procedure_uuid", index["uuid"].get(uuid, set()) if uuid else set()),
        ("bronverwijzing", set().union(*(index["id"].get(str(p), set()) for p in publicatie.get("publicatie_ids", [])))),
        ("tenderned_kenmerk", index["kenmerk"].get(str(publicatie.get("kenmerk")), set())),
        ("referentie_en_project", index["ref_project"].get((ref, project), set()) if ref and project else set()),
    ]
    for methode, hits in opties:
        if hits:
            signalen[methode] = hits
    kandidaten = set().union(*signalen.values()) if signalen else set()
    bewijs = {"gunning_publicatie_id": str(publicatie["id"]),
              "gunning_kenmerk": str(publicatie.get("kenmerk") or ""),
              "methoden": list(signalen), "procedure_uuid": uuid or "",
              "referentie_nummer": meta.get("referentie_nummer", ""),
              "project_sleutel": project or ""}
    if len(kandidaten) != 1:
        return None, {**bewijs, "reden": "meerdere_records" if kandidaten else "geen_identiteitsmatch",
                      "kandidaten": [records[i]["id"] for i in sorted(kandidaten)]}
    i = next(iter(kandidaten))
    record_uuid = records[i].get("koppeling_metadata", {}).get("procedure_uuid")
    if uuid and record_uuid and uuid != record_uuid:
        return None, {**bewijs, "reden": "tegenstrijdige_procedure_uuid", "kandidaten": [records[i]["id"]]}
    if publicatie.get("datum", "")[:10] < records[i].get("publicatiedatum", "")[:10]:
        return None, {**bewijs, "reden": "gunning_voor_recorddatum", "kandidaten": [records[i]["id"]]}
    return i, {**bewijs, "record_id": records[i]["id"]}


def werk_gunningen_bij(bestaand, gunningen, metadata_ophalen=False):
    """
    Markeert bestaande records als gegund zodra er een AGO voor is.

    Laat id en publicatiedatum met rust: id verwijst naar het opgeslagen
    document en publicatiedatum is altijd de datum van de oorspronkelijke
    uitvraag. De classificatie blijft ook staan — die verandert niet door een
    gunning, en opnieuw classificeren zou alleen geld kosten.
    """
    if metadata_ophalen:
        verrijk_records(bestaand)
    metadata = haal_identiteiten(str(ago.get("publicatieId") or ago.get("id") or "")
                                 for ago in gunningen.values()) if metadata_ophalen else {}
    index = maak_index(bestaand)
    bijgewerkt = set()
    for kenmerk, ago in gunningen.items():
        pub_id = str(ago.get("publicatieId") or ago.get("id") or "")
        datum = ago.get("publicatieDatum", "")
        meta = metadata.get(pub_id, ago.get("metadata", {}))
        if meta.get("voornemen") or meta.get("vroegtijdig_beeindigd"):
            continue
        publicatie = {"id": pub_id, "kenmerk": str(kenmerk), "datum": datum, "metadata": meta}
        i, bewijs = zoek_koppeling(publicatie, bestaand, index)
        if i is None:
            continue
        r = bestaand[i]
        if (r.get("publicatie_type") == "gegund"
                and pub_id in [r.get("gunning_publicatie_id"), *r.get("gunning_publicatie_ids", [])]):
            continue
        r["gunning_publicatie_ids"] = sorted(set(r.get("gunning_publicatie_ids", [])) | {pub_id})
        r["gekoppelde_kenmerken"] = sorted(set(r.get("gekoppelde_kenmerken", [])) | {str(kenmerk)})
        r.setdefault("gunning_koppelingen", []).append({**bewijs, "publicatiedatum": datum, "bron": "TenderNed API"})
        r["publicatie_type"] = "gegund"
        if datum >= (r.get("gunningsdatum") or ""):
            r["gunningsdatum"] = datum
            r["gunning_publicatie_id"] = pub_id
        bijgewerkt.add(i)
    return len(bijgewerkt)
