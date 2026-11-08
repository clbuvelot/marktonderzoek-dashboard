"""Algemeen herstel en periodieke controle van gunningskoppelingen."""
from collections import defaultdict

from collect import RELEVANTE_CPV_CODES
from publicatie_koppeling import (
    norm, verrijk_records, haal_identiteiten, maak_index, zoek_koppeling,
)
from winnaars import namen, migreer_bestaand, bedrijfsregister, organisaties, zet_organisaties, onzeker


def selecteer_publicaties(records, publicaties):
    ids = {str(p) for r in records for p in
           [r.get("id"), r.get("gunning_publicatie_id"), *r.get("gunning_publicatie_ids", [])] if p}
    kenmerken = {str(r.get("tenderned_kenmerk")) for r in records}
    titels = {norm(r.get("titel")) for r in records}
    refs = {norm(r.get("koppeling_metadata", {}).get("referentie_nummer")) for r in records} - {""}
    codes = {c[:8] for c in RELEVANTE_CPV_CODES}
    return [p for p in publicaties if (
        p["id"] in ids or str(p.get("kenmerk")) in kenmerken
        or bool(set(p.get("publicatie_ids", [])) & ids)
        or norm(p.get("titel")) in titels or norm(p.get("referentie")) in refs
        or any(c[:8] in codes for c in p.get("cpv_codes", []))
    )]


def herstel(records, publicaties, metadata_ophalen=True):
    voor = {r["id"]: {"namen": namen(r), "status": r.get("publicatie_type"),
                       "kenmerk": r.get("tenderned_kenmerk")} for r in records}
    if metadata_ophalen:
        verrijk_records(records)
    geselecteerd = selecteer_publicaties(records, publicaties)
    if metadata_ophalen:
        metadata = haal_identiteiten(p["id"] for p in geselecteerd)
        geselecteerd = [{**p, "metadata": metadata[p["id"]]} for p in geselecteerd]
    index = maak_index(records)
    groepen = defaultdict(list)
    bewijzen = defaultdict(list)
    review = []
    afgewezen = []
    niet_gekoppeld = 0
    for p in geselecteerd:
        i, bewijs = zoek_koppeling(p, records, index)
        if i is None:
            if not bewijs.get("kandidaten"):
                kandidaten = []
                for r in records:
                    if norm(r.get("titel")) != norm(p.get("titel")):
                        continue
                    ruuid = r.get("koppeling_metadata", {}).get("procedure_uuid")
                    puuid = p.get("metadata", {}).get("procedure_uuid")
                    reden = None
                    if p.get("datum", "")[:10] < r.get("publicatiedatum", "")[:10]:
                        reden = "gunning_voor_uitvraag"
                    elif ruuid and puuid and ruuid != puuid:
                        reden = "andere_procedure_uuid"
                    if reden:
                        afgewezen.append({"record_id": r["id"], "gunning_publicatie_id": p["id"], "reden": reden})
                    else:
                        kandidaten.append(r["id"])
                bewijs["kandidaten"] = kandidaten
            if bewijs.get("kandidaten"):
                review.append({**bewijs, "titel": p.get("titel"), "bron": p.get("bronbestand")})
            niet_gekoppeld += 1
            continue
        meta = p.get("metadata", {})
        geldige_rijen = [r for r in p.get("winnaar_rijen", []) if not onzeker(r.get("naam"))]
        if ("vrijwillige transparantie" in p.get("soort", "").lower()
                or meta.get("voornemen") or meta.get("vroegtijdig_beeindigd")
                or not geldige_rijen):
            review.append({**bewijs, "reden": "geen_bevestigde_winnaar", "titel": p.get("titel"),
                           "bronnamen": [r.get("naam") for r in p.get("winnaar_rijen", [])]})
            continue
        groepen[i].append({k: v for k, v in p.items() if k != "metadata"})
        bewijzen[i].append({**bewijs, "publicatiedatum": p.get("datum"),
                            "bronbestand": p.get("bronbestand"), "bron_sha256": p.get("bron_sha256")})

    register = bedrijfsregister(geselecteerd)
    veranderingen = []
    for i, r in enumerate(records):
        migreer_bestaand(r)
        if i in groepen:
            # Ook historische bronnen bewaren die buiten de nieuwste bulkperiode vallen.
            bronnen = {p["id"]: p for p in r.get("gunning_bronnen", [])}
            bronnen.update({p["id"]: p for p in groepen[i]})
            alle = sorted(bronnen.values(), key=lambda p: (p.get("datum", ""), p["id"]))
            zet_organisaties(r, organisaties(alle, register))
            r["gunning_bronnen"] = alle
            bewijs = {p["gunning_publicatie_id"]: p for p in r.get("gunning_koppelingen", [])}
            for p in bewijzen[i]:
                vorige = bewijs.get(p["gunning_publicatie_id"])
                if vorige:
                    p = {**p, "methoden": vorige["methoden"]}
                bewijs[p["gunning_publicatie_id"]] = p
            r["gunning_koppelingen"] = list(bewijs.values())
            # De API kan al recentere gunningen kennen dan de periodieke Excel.
            r["gunning_publicatie_ids"] = sorted(set(bewijs) | {p["id"] for p in alle})
            r["gekoppelde_kenmerken"] = sorted({str(p["gunning_kenmerk"]) for p in bewijs.values()})
            laatste = max(bewijs.values(), key=lambda p: (p.get("publicatiedatum") or "", p["gunning_publicatie_id"]))
            r["gunning_publicatie_id"] = laatste["gunning_publicatie_id"]
            r["gunningsdatum"] = laatste.get("publicatiedatum") or alle[-1]["datum"]
            r["publicatie_type"] = "gegund"
            datums = [rij["datum_gunning"] for p in alle for rij in p["winnaar_rijen"] if rij.get("datum_gunning")]
            if datums:
                r["datum_gunning"] = max(datums)[:10]
        oud = voor[r["id"]]
        toegevoegd = [n for n in namen(r) if norm(n) not in {norm(x) for x in oud["namen"]}]
        verwijderd = [n for n in oud["namen"] if norm(n) not in {norm(x) for x in namen(r)}]
        andere_kenmerken = [p for p in bewijzen[i] if p["gunning_kenmerk"] != str(oud["kenmerk"])]
        if toegevoegd or verwijderd or oud["status"] != r.get("publicatie_type") or andere_kenmerken:
            veranderingen.append({"record_id": r["id"], "titel": r.get("titel"),
                                  "oude_status": oud["status"], "nieuwe_status": r.get("publicatie_type"),
                                  "oude_winnaars": oud["namen"], "nieuwe_winnaars": namen(r),
                                  "toegevoegd": toegevoegd, "verwijderd": verwijderd,
                                  "koppelingen": bewijzen[i]})
    return {
        "opdrachten": len(records), "bronpublicaties_onderzocht": len(geselecteerd),
        "gekoppelde_gunningspublicaties": sum(len(g) for g in groepen.values()),
        "opdrachten_met_bronkoppeling": len(groepen),
        "opdrachten_met_ander_gunningskenmerk": sum(any(p["gunning_kenmerk"] != str(records[i]["tenderned_kenmerk"])
                                                       for p in bewijzen[i]) for i in groepen),
        "voor_met_winnaar": sum(bool(r["namen"]) for r in voor.values()),
        "na_met_winnaar": sum(bool(namen(r)) for r in records),
        "gewijzigde_opdrachten": len(veranderingen), "wijzigingen": veranderingen,
        "controlelijst": review,
        "afgewezen_titelkandidaten": afgewezen,
        "identiteitsconflicten": register[2],
        "zonder_koppeling": niet_gekoppeld,
        "toelichting_zonder_koppeling": "Bronpublicaties kunnen buiten de dataset vallen; titels zijn geen bewijs.",
    }
