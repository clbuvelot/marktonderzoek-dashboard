"""
opschonen.py
------------
Corrigeert datakwaliteitsproblemen in geclassificeerd_met_winnaar.json:
1. Typo "Unduidelijk" -> "Onduidelijk"
2. Normalisatie van bureaunamen (deduplicatie varianten)
3. Verwijdert HTML-entities uit omschrijvingen

Gebruik:
    python opschonen.py

Input:  data/geclassificeerd_met_winnaar.json
Output: data/geclassificeerd_met_winnaar.json  (overschreven)
        data/opschonen_rapport.txt             (wijzigingslog)
"""

import json
import re
import os
from collections import defaultdict

INPUT = "data/geclassificeerd_met_winnaar.json"
RAPPORT = "data/opschonen_rapport.txt"

METHODIEK_CORRECTIES = {
    # Typo-correcties
    "Unduidelijk / niet gespecificeerd": "Onduidelijk / niet gespecificeerd",
    "Unduidelijk":                        "Onduidelijk / niet gespecificeerd",
    # Evaluatiefase-waarden die foutief in methodiek terechtkwamen
    "Ex-post":                            "Onduidelijk / niet gespecificeerd",
    "Ex-ante":                            "Onduidelijk / niet gespecificeerd",
    "Mid-term":                           "Onduidelijk / niet gespecificeerd",
    "Monitor / periodieke meting":        "Doeltreffendheidsevaluatie (beschrijvend)",
    # Thema-waarden die foutief in methodiek terechtkwamen
    "Bestuur & uitvoering":               "Onduidelijk / niet gespecificeerd",
    "Arbeidsmarkt & sociale zekerheid":   "Onduidelijk / niet gespecificeerd",
    "Zorg & welzijn":                     "Onduidelijk / niet gespecificeerd",
    "Energie & klimaat":                  "Onduidelijk / niet gespecificeerd",
    "Wonen & ruimte":                     "Onduidelijk / niet gespecificeerd",
    "Economie & innovatie":               "Onduidelijk / niet gespecificeerd",
    "Veiligheid & justitie":              "Onduidelijk / niet gespecificeerd",
    "Onderwijs & talentontwikkeling":     "Onduidelijk / niet gespecificeerd",
    "Internationaal & EU-fondsen":        "Onduidelijk / niet gespecificeerd",
}

BUREAU_NORMALISATIE = {
    # SEO
    "SEO Economisch Onderzoek":                                  "Stichting SEO Economisch Onderzoek",
    "SEO":                                                       "Stichting SEO Economisch Onderzoek",
    # I&O Research
    "I&O Research":                                              "I&O Research B.V.",
    "I&O research BV":                                           "I&O Research B.V.",
    "I&O Research B.V. h.o.d.n. Ipsos I&O":                     "I&O Research B.V.",
    "Ipsos I&O":                                                 "I&O Research B.V.",
    # Arcadis
    "Arcadis Nederland BV":                                      "Arcadis Nederland B.V.",
    "Arcadis Nderland B.V.":                                     "Arcadis Nederland B.V.",
    # Witteveen+Bos
    "Witteveen+Bos":                                             "Witteveen+Bos Raadgevende ingenieurs B.V.",
    "Wittenveen + Bos B.V.":                                     "Witteveen+Bos Raadgevende ingenieurs B.V.",
    "Witteveen + Bos B.V.":                                      "Witteveen+Bos Raadgevende ingenieurs B.V.",
    # Panteia
    "Panteia":                                                   "Panteia B.V.",
    # TNO
    "TNO":                                                       "Nederlandse Organisatie voor toegepast-natuurwetenschappelijk onderzoek TNO",
    "Stichting TNO":                                             "Nederlandse Organisatie voor toegepast-natuurwetenschappelijk onderzoek TNO",
    # Kantar
    "Kantar Public (voorheen TNS NIPO)":                         "Kantar Public - Verian (voorheen Kantar/TNS Nipo)",
    "Kantar Public":                                             "Kantar Public - Verian (voorheen Kantar/TNS Nipo)",
    # De Beleidsonderzoekers
    "De Beleidsonderzoekers B.V.":                               "De Beleidsonderzoekers",
    # Berenschot
    "Berenschot":                                                "Berenschot B.V.",
    # ATKB
    "ATKB BV":                                                   "ATKB B.V.",
    # Motivaction
    "Motivaction International B.V.":                            "Motivaction",
    "Motivaction International BV":                              "Motivaction",
    # Blauw Research
    "Blauw Research":                                            "Blauw Research B.V.",
    # Dialogic
    "Dialogic Innovatie":                                        "Dialogic Innovatie & Interactie",
    # Rho adviseurs
    "Rho adviseurs B.V.":                                        "Rho Adviseurs B.V.",
    # ECA International
    "Employment Conditions Abroad Limited (ECA International)":  "ECA International",
    # RVO — naamswijziging na splitsing EZK
    "Rijksdienst voor Ondernemend Nederland":                    "RVO Nederland",
    "Ministerie van Economische Zaken, Rijksdienst voor Ondernemend Nederland (RVO)": "RVO Nederland",
    "Ministerie van Economische Zaken en Klimaat, Rijksdienst voor Ondernemend Nederland (RVO)": "RVO Nederland",
    "RVO":                                                       "RVO Nederland",
    # Rijkswaterstaat varianten
    "Rijkswaterstaat Water Verkeer en Leefomgeving":             "Rijkswaterstaat Water, Verkeer en Leefomgeving",
    "Rijkswaterstaat WVL":                                       "Rijkswaterstaat Water, Verkeer en Leefomgeving",
    # NWO
    "Nederlandse Organisatie voor Wetenschappelijk Onderzoek":   "NWO",
    "Stichting Nederlandse Organisatie voor Wetenschappelijk Onderzoek": "NWO",
    # BIJ12
    "BIJ12 namens IPO":                                          "BIJ12",
    "BIJ12 namens de provincies":                                "BIJ12",
}

HTML_ENTITIES = {
    "&amp;":  "&",
    "&nbsp;": " ",
    "&lt;":   "<",
    "&gt;":   ">",
    "&quot;": '"',
    "&#39;":  "'",
    "&apos;": "'",
}


def reinig_html(tekst):
    if not tekst:
        return tekst
    for entity, char in HTML_ENTITIES.items():
        tekst = tekst.replace(entity, char)
    tekst = re.sub(r'<[^>]+>', ' ', tekst)
    tekst = re.sub(r'\s+', ' ', tekst).strip()
    return tekst


def verwerk(records):
    tellers = defaultdict(int)

    for r in records:
        cl = r.get("classificatie") or {}
        methodiek = cl.get("methodiek", "")
        if methodiek in METHODIEK_CORRECTIES:
            cl["methodiek"] = METHODIEK_CORRECTIES[methodiek]
            # Als methodiek gecorrigeerd naar Onduidelijk, zet kwantitatief op False
            if cl["methodiek"] == "Onduidelijk / niet gespecificeerd":
                cl["methodiek_kwantitatief"] = False
            r["classificatie"] = cl
            tellers["methodiek_typo"] += 1

        winnaar = r.get("winnaar", "") or ""
        if winnaar in BUREAU_NORMALISATIE:
            r["winnaar"] = BUREAU_NORMALISATIE[winnaar]
            tellers["bureau_genormaliseerd"] += 1

        # Normaliseer ook aanbestedende_dienst waar van toepassing
        dienst = r.get("aanbestedende_dienst", "") or ""
        if dienst in BUREAU_NORMALISATIE:
            r["aanbestedende_dienst"] = BUREAU_NORMALISATIE[dienst]
            tellers["dienst_genormaliseerd"] += 1

        omschrijving = r.get("omschrijving", "") or ""
        if any(e in omschrijving for e in HTML_ENTITIES):
            r["omschrijving"] = reinig_html(omschrijving)
            tellers["html_gereinigd"] += 1

    return records, dict(tellers)


def schrijf_rapport(tellers, pad):
    with open(pad, "w", encoding="utf-8") as f:
        f.write("Opschoningsrapport geclassificeerd_met_winnaar.json\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Methodiek-typos gecorrigeerd:    {tellers.get('methodiek_typo', 0)}\n")
        f.write(f"Bureaunamen genormaliseerd:       {tellers.get('bureau_genormaliseerd', 0)}\n")
        f.write(f"Opdrachtgevers genormaliseerd:    {tellers.get('dienst_genormaliseerd', 0)}\n")
        f.write(f"HTML-entities gereinigd:          {tellers.get('html_gereinigd', 0)}\n")
        f.write("\nNormalisatietabel bureaunamen:\n")
        for oud, nieuw in BUREAU_NORMALISATIE.items():
            f.write(f"  {oud:<60} -> {nieuw}\n")


def controleer_resterende_problemen(records):
    typos = [r for r in records if r.get('classificatie') and
             'unduidelijk' in (r['classificatie'].get('methodiek','') or '').lower()]
    if typos:
        print(f"  [WAARSCHUWING] {len(typos)} records hebben nog een methodiek-typo na opschoning.")
        print(f"  Voer classify.py opnieuw uit en draai daarna opschonen.py nogmaals.")
    else:
        print("  Geen resterende typos gevonden.")


def main():
    if not os.path.exists(INPUT):
        print(f"[FOUT] {INPUT} niet gevonden. Draai eerst koppel_winnaar.py.")
        return

    print(f"[1/3] Laden {INPUT}...")
    with open(INPUT, encoding="utf-8") as f:
        records = json.load(f)
    print(f"  {len(records)} records geladen")

    print("[2/3] Opschonen...")
    records, tellers = verwerk(records)

    print("[3/3] Opslaan...")
    with open(INPUT, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  -> {INPUT} overschreven")

    schrijf_rapport(tellers, RAPPORT)
    print(f"  -> {RAPPORT}")

    print(f"""
=== Samenvatting ===
Methodiek-typos gecorrigeerd:    {tellers.get('methodiek_typo', 0)}
Bureaunamen genormaliseerd:       {tellers.get('bureau_genormaliseerd', 0)}
Opdrachtgevers genormaliseerd:    {tellers.get('dienst_genormaliseerd', 0)}
HTML-entities gereinigd:          {tellers.get('html_gereinigd', 0)}
""")

    print("Validatie na opschoning...")
    controleer_resterende_problemen(records)
    print("\nKlaar. Draai nu: python analyse.py")


if __name__ == "__main__":
    main()
