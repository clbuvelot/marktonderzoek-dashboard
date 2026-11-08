"""Haal inhoudelijke context uit samengestelde PDF-teksten, ook voorbij voorbladen."""
import math
import re
from collections import Counter

STOP = set('de het een van voor met door deze dit dat als zijn wordt worden heeft hebben opdracht aanbesteding onderzoek onderzoeken services contract agreement tender procedure openbare europese the and for with from this that shall will including ministry'.split())


def woorden(tekst):
    return [w for w in re.findall(r'[^\W\d_]{4,}', tekst.casefold()) if w not in STOP]


def selecteer_document(tekst, vraag='', limiet=16000):
    if len(tekst) <= limiet:
        return tekst
    delen = re.split(r'(?m)^=== Document: (.*?) ===\s*$', tekst)
    documenten = [(delen[i], delen[i + 1]) for i in range(1, len(delen) - 1, 2)]
    if not documenten:
        documenten = [('Aanbestedingsdocument', tekst)]
    chunks = []
    inhoudelijk = re.compile(r'beschrijvend|leidraad|offerteaanvraag|terms of reference|programma van eisen|specification|scope|aanbestedingsdocument', re.I)
    juridisch = re.compile(r'algemene voorwaarden|verwerkers|privacy|overeenkomst|contract|uea|verklaring|nota van inlichtingen|inschrijf', re.I)
    sectie = re.compile(r'opdrachtomschrijving|beschrijving van de opdracht|onderzoeksvragen|doelstelling|doelgroep|achtergrond|scope of|background|objectives|purpose of|description of', re.I)
    for nr, (kop, inhoud) in enumerate(documenten):
        for start in range(0, len(inhoud), 2200):
            fragment = inhoud[start:start + 3000]
            chunks.append({'doc': nr, 'kop': kop, 'start': start, 'tekst': fragment,
                           'woorden': Counter(woorden(fragment))})
    df = Counter(w for c in chunks for w in c['woorden'])
    query = set(woorden(vraag))
    for c in chunks:
        score = sum((1 + math.log(c['woorden'][w])) * math.log(1 + len(chunks) / df[w])
                    for w in query if w in c['woorden'])
        score += min(8, len(sectie.findall(c['tekst']))) * 2
        score += 5 if inhoudelijk.search(c['kop']) else 0
        score -= 8 if juridisch.search(c['kop']) else 0
        c['score'] = score
    gekozen, lengte = [], 0
    # Trefwoorden selecteren context; de classifier beoordeelt daarna dienst én onderwerp.
    for c in sorted(chunks, key=lambda c: (-c['score'], c['doc'], c['start'])):
        if any(c['doc'] == d['doc'] and abs(c['start'] - d['start']) < 3000 for d in gekozen):
            continue
        fragment = f"\n=== Document: {c['kop']} (fragment vanaf teken {c['start']}) ===\n{c['tekst']}"
        if lengte + len(fragment) > limiet:
            continue
        gekozen.append(c)
        lengte += len(fragment)
    gekozen.sort(key=lambda c: (c['doc'], c['start']))
    # Borg de daadwerkelijke opdrachtbeschrijving wanneer de trefwoordscore die
    # verdringt. Alleen losse sectiekoppen, dus geen inhoudsopgave met puntleiders.
    kopregel = re.compile(
        r'(?im)^[ \t]*(?:\d+(?:\.\d+)*\.?[ \t]+)?'
        r'(beschrijving en doel van de opdracht|beschrijving van de opdracht|'
        r'opdrachtomschrijving|onderzoeksvragen|scope of work|objectives of the assignment)'
        r'[ \t]*\r?$', re.I)
    ankers = []
    for nr, (kop, inhoud) in enumerate(documenten):
        if juridisch.search(kop) and not inhoudelijk.search(kop):
            continue
        for match in kopregel.finditer(inhoud):
            # Een kop zonder inhoud aan het documenteinde helpt niet.
            if len(inhoud) - match.end() < 600:
                continue
            ankers.append({'doc': nr, 'kop': kop, 'start': match.start(),
                           'tekst': inhoud[match.start():match.start() + min(5000, limiet // 2)],
                           'prioriteit': 2 if match.group(1).lower() in ('beschrijving en doel van de opdracht', 'onderzoeksvragen') else 1})
    if ankers:
        anker = max(ankers, key=lambda a: (a['prioriteit'], bool(inhoudelijk.search(a['kop'])), -a['doc'], -a['start']))
        gedekt = any(c['doc'] == anker['doc'] and c['start'] <= anker['start']
                     and c['start'] + len(c['tekst']) >= anker['start'] + min(1800, len(anker['tekst'])) for c in gekozen)
        if not gedekt:
            gekozen, lengte = [], 0
            for c in [anker, *sorted(chunks, key=lambda c: (-c['score'], c['doc'], c['start']))]:
                if any(c['doc'] == d['doc'] and c['start'] < d['start'] + len(d['tekst'])
                       and d['start'] < c['start'] + len(c['tekst']) for d in gekozen):
                    continue
                fragment = f"\n=== Document: {c['kop']} (fragment vanaf teken {c['start']}) ===\n{c['tekst']}"
                if lengte + len(fragment) <= limiet:
                    gekozen.append(c)
                    lengte += len(fragment)
            gekozen.sort(key=lambda c: (c['doc'], c['start']))
    return '\n'.join(f"=== Document: {c['kop']} (fragment vanaf teken {c['start']}) ===\n{c['tekst']}" for c in gekozen)
