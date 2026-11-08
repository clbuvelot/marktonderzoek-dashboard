"""Alle bevestigde winnende organisaties, eenmaal per opdracht tellen."""
import re
from collections import Counter, defaultdict
from publicatie_koppeling import norm
from opschonen import BUREAU_NORMALISATIE


def normaliseer_naam(naam):
    naam = re.sub(r"\s+", " ", str(naam or "")).strip()
    naam = re.sub(r"^contractant\s*:\s*", "", naam, flags=re.I)
    if norm(naam) in {"ecorys", "ecorysnederlandbv", "ecorysnedelrlandbv"}:
        return "Ecorys Nederland B.V."
    return BUREAU_NORMALISATIE.get(naam, naam)


def geldig_kvk(kvk):
    return bool(re.fullmatch(r"\d{8}", kvk)) and len(set(kvk)) > 1 and kvk not in {"12345678", "87654321"}


def naamdelen(naam):
    # Bronvermeldingen als 'X h.o.d.n. Y' vormen een expliciete naambrug.
    naam = naam.split("(")[0]
    delen = re.split(r"h\.?o\.?d\.?n\.?| - ", naam, flags=re.I)
    return {norm(re.sub(r"\bB\.?V\.?\b", "", d, flags=re.I)) for d in delen if len(norm(d)) >= 3}


def samenhangende_namen(namen):
    over = [naamdelen(n) for n in namen]
    verbonden = over.pop() if over else set()
    while over:
        indices = [i for i, delen in enumerate(over)
                   if any(a in b or b in a for a in delen for b in verbonden)]
        if not indices:
            return False
        for i in reversed(indices):
            verbonden.update(over.pop(i))
    return True


def onzeker(naam):
    tekst = str(naam or "").strip()
    return (not tekst or norm(tekst) in {"onbekend", "unknown", "nietgegund", "geen", "nvt", "nan"}
            or bool(re.search(r"\bóf\b|\b(?:of|or)\s*\d+[.)]", tekst, re.I)))


def namen(record):
    if "winnaars" in record:
        return record["winnaars"]
    return list(dict.fromkeys(normaliseer_naam(n) for n in
                             [record.get("winnaar"), *record.get("extra_winnaars", [])]
                             if n and not onzeker(n)))


def bedrijfsregister(publicaties):
    """KvK koppelt naamvarianten; een naam met meerdere KvK's blijft ambigu."""
    per_kvk = defaultdict(Counter)
    naam_kvks = defaultdict(set)
    for pub in publicaties:
        for rij in pub.get("winnaar_rijen", []):
            if onzeker(rij.get("naam")):
                continue
            naam = normaliseer_naam(rij["naam"])
            kvk = str(rij.get("kvk", "")).strip()
            if geldig_kvk(kvk):
                per_kvk[kvk][naam] += 1
                naam_kvks[norm(naam)].add(kvk)
    conflicten = {k: sorted(c) for k, c in per_kvk.items() if not samenhangende_namen(c)}
    canoniek = {k: c.most_common(1)[0][0] for k, c in per_kvk.items() if k not in conflicten}
    return canoniek, {n: next(iter(k)) for n, k in naam_kvks.items()
                     if len(k) == 1 and next(iter(k)) in canoniek}, conflicten


def organisaties(publicaties, register):
    canoniek, naam_kvks, conflicten = register
    gevonden = {}
    for pub in sorted(publicaties, key=lambda p: (p.get("datum", ""), p["id"])):
        for rij in pub.get("winnaar_rijen", []):
            if onzeker(rij.get("naam")):
                continue
            bron = rij["naam"]
            naam = normaliseer_naam(bron)
            kvk = str(rij.get("kvk", "")).strip()
            if not geldig_kvk(kvk):
                kvk = naam_kvks.get(norm(naam), "")
            naam = canoniek.get(kvk, naam)
            key = f"kvk:{kvk}" if kvk and kvk in canoniek else f"naam:{norm(naam)}"
            entry = gevonden.setdefault(key, {"naam": naam, "kvk": kvk, "bronnamen": [], "percelen": [], "bron_publicatie_ids": []})
            if kvk in conflicten:
                entry["kvk_bronconflict"] = True
            for veld, value in (("bronnamen", bron), ("percelen", rij.get("perceel")), ("bron_publicatie_ids", pub["id"])):
                if value and value not in entry[veld]:
                    entry[veld].append(value)
    return list(gevonden.values())


def zet_organisaties(record, orgs):
    record["winnende_organisaties"] = orgs
    record["winnaars"] = list(dict.fromkeys(o["naam"] for o in orgs))
    record["winnaar"] = record["winnaars"][0] if record["winnaars"] else ""
    record["extra_winnaars"] = record["winnaars"][1:]


def migreer_bestaand(record):
    if "winnaars" not in record:
        zet_organisaties(record, [{"naam": n, "kvk": "", "bronnamen": [n],
                                  "percelen": [], "bron_publicatie_ids": [], "bron": "bestaande_dataset"}
                                 for n in namen(record)])
