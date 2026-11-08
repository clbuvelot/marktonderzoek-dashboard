# Waarom Ecorys nauwelijks als winnaar zichtbaar is

Dit verslag beschrijft de situatie vóór herstel. Daarna zijn de algemene
koppellogica, dataset en telling aangepast; zie het
[herstelverslag voor alle bureaus](../herstel_koppelingen.md).

Onderzocht op 8 november 2026. Uitgangspunt: de lokale dashboarddataset met
929 opdrachten. De dashboarddataset en applicatiecode zijn voor dit onderzoek
niet aangepast.

## Conclusie

Ecorys is deels uit beeld geraakt door de presentatie, deels door aantoonbaar
mislukte koppelingen, en deels door de afbakening en historische dekking van de
dataset. De huidige zichtbaarheid betekent dus niet dat Ecorys nauwelijks werk
heeft gewonnen.

In de bestaande dataset staat Ecorys bij vier opdrachten: twee als eerste
winnaar en twee als extra winnaar. Alleen de eerste twee tellen mee in de
ranglijst; daarmee valt Ecorys buiten de getoonde top 20. Bij twee andere,
al aanwezige uitvragen bewijzen de officiële gunningspublicaties dat Ecorys
eveneens winnaar is. Die koppelingen ontbreken. Binnen de bestaande 929
opdrachten zijn dus **minimaal zes opdrachten** met Ecorys als (mede)winnaar
aanwijsbaar, terwijl de huidige ranglijst er twee telt.

Dit is geen schatting van de totale omzet of het aantal afzonderlijke
deelopdrachten. Raamovereenkomsten, percelen en medecontractanten vragen om
een expliciete telregel.

## 1. Wat staat er al in het dashboard?

| Opdracht | TenderNed-publicatie in dashboard | Ecorys in de huidige data | Telt mee voor Ecorys? |
| --- | --- | --- | --- |
| BZ-ET Prospects consultancy | [235829](https://www.tenderned.nl/aankondigingen/overzicht/235829) | Eerste winnaar | Ja |
| Verkeerstellingen en verkeersonderzoeken, Rotterdam | [253789](https://www.tenderned.nl/aankondigingen/overzicht/253789) | Eerste winnaar | Ja |
| BZ EA ROVK Evaluations, eerdere raamovereenkomst | 220097 | Extra winnaar; eerste winnaar is Particip GmbH | Nee |
| Baseline, Monitoring and Mid-Term Evaluation Strengthening Civil Society | [278183](https://www.tenderned.nl/aankondigingen/overzicht/278183) | Extra winnaar; eerste winnaar is Itad Ltd | Nee |

De eerste twee worden bevestigd door de bulkdata en de officiële
gunningspublicaties. De derde staat in beide bulkbestanden; het oude
detail-endpoint voor publicatie 220097 retourneerde bij deze controle HTTP 404.
De vierde is ook gecontroleerd in de officiële gunningspublicatie.

De code in `dashboard.html` telt uitsluitend `winnaar`, niet `extra_winnaars`.
De grafiek toont tien bureaus en de concurrentiekaart twintig. Er zijn 26
bureaus met meer dan twee eerste-winnaarregistraties en 35 bureaus met precies
twee. Ecorys komt daardoor zonder verdere filters niet in die overzichten.
De kaart bevat wel code om een zichtbare Ecorys-rij te markeren, maar die wordt
pas uitgevoerd nadat de selectie tot twintig bureaus is beperkt.

## 2. Twee bewezen koppelproblemen

### BZK – Uitvoeren lastendrukmetingen

| Eigenschap | Uitvraag in het dashboard | Officiële gunning |
| --- | --- | --- |
| Publicatie-id | [322538](https://www.tenderned.nl/aankondigingen/overzicht/322538) | [361925](https://www.tenderned.nl/aankondigingen/overzicht/361925) |
| TenderNed-kenmerk | 447347 | 503410 |
| Publicatiedatum | 13 januari 2024 | 20 december 2024 |
| Interne opdrachtnummer | 201865002.001.095 | 201865002.001.095 |
| eForms-procedure-UUID | 290115c6-f318-4bf4-b23e-f3b6cc461a3c | 290115c6-f318-4bf4-b23e-f3b6cc461a3c |
| Mercell-project | PID=391171 | PID=391171 |

De officiële gunning noemt **ECORYS Nederland B.V.**, KvK **24316726**, als
winnaar. De bron noemt een overeenkomst met Ecorys voor het uitvoeren van
lastendrukmetingen. De hoofd-CPV `79311400-1` valt binnen onze bestaande selectie.

In het dashboard staat deze uitvraag nog op `lopend`, zonder winnaar. Dit is
geen kwestie van een uitgesloten marktsegment: de uitvraag staat er al in,
maar het TenderNed-kenmerk is bij de gunning anders geworden.

Bewijs: [uitvraagtekst](publicatie-322538.txt),
[gunningstekst](publicatie-361925.txt),
[uitvraagmetadata](api-322538.json), [gunningsmetadata](api-361925.json).

### BZ ROVK Evaluations, nieuwe raamovereenkomst

| Eigenschap | Uitvraag in het dashboard | Officiële gunning |
| --- | --- | --- |
| Publicatie-id | [335914](https://www.tenderned.nl/aankondigingen/overzicht/335914) | [365375](https://www.tenderned.nl/aankondigingen/overzicht/365375) |
| TenderNed-kenmerk | 466907 | 508395 |
| Publicatiedatum | 10 mei 2024 | 29 januari 2025 |
| Interne opdrachtnummer | 201865003.001.106 | 201865003.001.106 |
| eForms-procedure-UUID | a9389d65-d380-45f9-9eae-9d8a1569d209 | a9389d65-d380-45f9-9eae-9d8a1569d209 |
| Mercell-project | PID=403116 | PID=403116 |

De officiële gunning vermeldt **Ecorys Nedelrland B.V.** voor lot 1,
Evaluation services. De tikfout staat in de bron. Het KvK-nummer is
**24316726**, waarmee de identiteit wordt bevestigd. De hoofd-CPV
`79419000-4` is eveneens onderdeel van onze huidige selectie.

Ook deze uitvraag staat nog op `lopend`, zonder winnaar. De procedure-UUID,
interne referentie en projectlink komen exact overeen; dit is een bewezen
koppeling, geen overeenkomst op alleen titel.

Bewijs: [uitvraagtekst](publicatie-335914.txt),
[gunningstekst](publicatie-365375.txt),
[uitvraagmetadata](api-335914.json), [gunningsmetadata](api-365375.json).

### Waar gaat dit technisch mis?

- `collect.py` neemt een gunning alleen mee als het kenmerk voorkomt bij de
  opgehaalde uitvragen. Bij de twee gevallen hierboven is dat niet zo.
- `update.py`, functie `werk_gunningen_bij`, zoekt de gunning eveneens op
  `tenderned_kenmerk`. Een langere terugkijkperiode herstelt een afwijkende
  sleutel niet.
- `vul_winnaars_aan.py` probeert eerst het kenmerk, daarna een al bekende
  gunningspublicatie-id en het eigen publicatie-id. De uitvraag-id is niet de
  gunnings-id, en de gunnings-id is bij een mislukte eerdere koppeling juist
  nog niet opgeslagen. Alle routes missen deze twee gevallen.

De veronderstelling dat het TenderNed-kenmerk altijd gelijk blijft tussen
uitvraag en gunning klopt aantoonbaar niet voor deze geïmporteerde
eForms-publicaties. Dat bewijst niet dat het bij alle geïmporteerde
publicaties misgaat: verscheidene oudere geïmporteerde Ecorys-opdrachten
zijn wel gekoppeld.

De eerdere controle op de actuele publicatie en interne consistentie kon
dit niet aantonen. Ook een dagelijkse controle die dezelfde koppelsleutel
gebruikt, kan ten onrechte geen wijzigingen melden voor deze uitvragen.

## 3. Wat staat er buiten deze vier plus twee opdrachten in de bron?

Beide bulkbestanden samen bevatten 35 unieke TenderNed-kenmerken met een
Ecorys-naam of het KvK-nummer 24316726 in de gegunde-ondernemingvelden.
Hiervan hebben tien een gunningspublicatie vóór 2019. Eén andere vermelding
is expliciet onzeker en mag niet als gewonnen worden geteld.

Voor de **24 overige bronregistraties met gunningspublicaties vanaf 2019**
is de verdeling:

| Bevinding | Aantal |
| --- | ---: |
| Aanwezig als eerste winnaar; buiten de top 20 | 2 |
| Aanwezig als extra winnaar; telt niet mee | 2 |
| Bewezen koppelgat bij een bestaande uitvraag | 2 |
| Buiten de huidige CPV-selectie | 12 |
| Gunning aanwezig, maar geen bijbehorende uitvraag aangetroffen in de onderzochte bronselectie | 4 |
| Historisch dekkingsgat: uitvraag én gunning in de Excel, niet in het dashboard | 1 |
| Gunning in 2019, maar bijbehorende uitvraag vóór 2019 | 1 |
| **Totaal** | **24** |

De volledige uitsplitsing, inclusief bronrijen, ids, namen, CPV-codes en
bevinding per opdracht, staat in [vergelijking.csv](vergelijking.csv) en
[vergelijking.json](vergelijking.json). Meerdere percelen en dubbele
vermeldingen in de twee bronbestanden zijn hierbij per TenderNed-kenmerk
samengenomen; de 24 zijn geen aantal afzonderlijke contracten of omzetclaims.

**Andere CPV-codes.** Voorbeelden zijn parkeeronderzoek, ingenieursdiensten,
inhuur en infrastructuuradvies. Ook inhoudelijk relevante evaluaties kunnen
onder een andere code staan, bijvoorbeeld de evaluatie van twaalf bruggen
in Laos onder `79900000-3`. Deze uitsluitingen volgen uit de huidige technische
afbakening; de afbakening dekt dus niet al het werk van Ecorys.
Waar het actuele detail-endpoint beschikbaar was, zijn ook de aanvullende
CPV-codes gecontroleerd, niet alleen de hoofdcode uit Excel. Voor oudere
publicaties die HTTP 404 gaven, berust deze indeling op de hoofd- en
perceelcodes in de bulkbestanden.

**Vier gunningen zonder aangetroffen uitvraag.** Dit betreft onderzoeken voor
Zorginstituut Nederland uit 2019 en 2020, waaronder regionale IC-netwerken,
ontwikkeltools, voorlichting bij blaasontsteking en zelfmanagement bij eczeem
of psoriasis. In de bulk staan gunningen en Ecorys als winnaar, maar de
onderzochte bronselectie en de opgenomen publicatieverwijzingen leveren geen
bijbehorende uitvraag. Een pipeline die uitsluitend vanaf een gevonden
uitvraag werkt, neemt zulke gunningen niet zelfstandig op. Dat is een
dekkingsbeperking; het is geen bewijs dat de gunning niet heeft plaatsgevonden.

**Eén historisch gat.** Voor *Beoordeling van financieel-economische en
technologische expertise bij internationale regelingen* bevat de historische
Excel de uitvraag 161736 van 26 maart 2019 en gunning 170304 van 9 juli 2019,
beide onder kenmerk 217407 en CPV `73000000-2`. Ecorys staat bij de gunning.
De opdracht ontbreekt volledig in het dashboard. Het huidige detail-endpoint
voor de gunning retourneerde 404. De precieze historische uitvalstap is met
de nu beschikbare gegevens niet bewezen.

**Uitvraag vóór de startdatum.** De OCW-raamovereenkomst Beleidsgerichte
Onderzoeksopdrachten 2018 is in maart 2018 uitgevraagd en in januari 2019
als gunning gepubliceerd. Die valt buiten een selectie op uitvragen vanaf
2019, ondanks een gunningspublicatie in 2019.

## 4. Een bronvermelding die géén zekere Ecorys-winst is

[VWS EA Onderzoek medicatieveiligheid en medicatieoverdracht, 284952](https://www.tenderned.nl/aankondigingen/overzicht/284952)
heeft in het veld gegunde onderneming letterlijk:

> 1) Ecorys Nederland B.V. óf 2) Stichting NIVEL

In de [officiële aankondiging](publicatie-284952.txt) staat dat het ministerie
voornemens is te gunnen aan één van deze twee partijen. Het gaat om vrijwillige
transparantie vooraf, niet om bewijs dat Ecorys daadwerkelijk is gekozen.
Deze vermelding is daarom apart gehouden en niet opgenomen in de 24.
Een zoekactie op alleen de bedrijfsnaam zou dit onterecht als winst tellen.

## 5. Overige relevante bevindingen

- Er bestaan meerdere naamvarianten, waaronder de bronfout “Nedelrland”.
  Zoeken is daarom zowel op naamdeel `ecory` als op KvK 24316726 uitgevoerd.
  De aanvullende KvK-controle leverde geen extra registraties op. Eén vaste
  bedrijfsnaam als zoekwaarde is onvoldoende.
- Alle 929 huidige records hebben een leeg `cpv_codes`-veld. De bij de
  collectie gebruikte filterinformatie is dus niet in de dataset bewaard,
  terwijl de gecontroleerde detail-endpoints wel CPV-codes leveren. Dat
  bemoeilijkt het achteraf controleren van de selectie.
- `vul_winnaars_aan.py` slaat records met een al ingevulde eerste winnaar
  helemaal over. Daardoor worden nieuw beschikbare extra winnaars of
  broncorrecties bij zulke records niet automatisch bijgewerkt.
- De brondata gebruikt raamovereenkomsten en percelen met meerdere winnaars.
  “Eerste winnaar” betekent hier de eerste opgeslagen bronrij, niet bewezen
  marktleider, grootste aandeel of penvoerder van een consortium.

Deze bevindingen zijn een audit van Ecorys en van de gevolgde codepaden,
geen volledig geverifieerde koppelaudit van alle 929 opdrachten.

## 6. Benodigde herstelrichting

1. Koppel de twee bewezen gunningen aan de bestaande uitvragen, met behoud
   van beide publicatie-id's, de oorspronkelijke uitvraagdatum en het
   controleerbare bewijs. Voeg alle winnaars van de betreffende gunningen
   toe, niet alleen Ecorys.
2. Gebruik waar beschikbaar de eForms-procedure-UUID. Een gecontroleerde
   combinatie van interne referentie, opdrachtgever en project-id van het
   externe platform kan als aanvullende route dienen. Koppel niet
   automatisch op een gelijkluidende titel.
3. Sla de volledige lijst van winnende organisaties op, inclusief
   bronnaam, KvK, perceel en herkomst. Tel elke organisatie maximaal eenmaal
   per gekozen eenheid en normaliseer naamvarianten. Vernieuw ook al bekende
   winnaarslijsten.
4. Maak de volledige concurrentenlijst toegankelijk via zoeken of een
   winnaarfilter. Een top 10/top 20 moet geen bureau onvindbaar maken.
5. Vul historische data vanuit de bulk aan en behandel gunningen zonder
   gevonden uitvraag expliciet. Leg vast of de tijdselectie gebaseerd is
   op de uitvraag- of gunningsdatum. Heroverweeg CPV-dekking voor de beoogde markt.
6. Houd voornemens en onzekere bronvermeldingen apart van bevestigde gunningen.

## Bronnen en reproduceerbaarheid

- Historische lokale TenderNed-bulk 2016-01-01 t/m 2025-12-31:
  300.635 datarijen volledig doorzocht.
- [Actuele bulk 2021-01-01 t/m 2026-10-01](https://www.tenderned.nl/cms/sites/default/files/2026-10/Dataset_Tenderned-compleet-2021-01-01-2026-10-01.xlsx):
  198.334 datarijen volledig doorzocht. Deze bron dekt oktober/november 2026
  niet volledig; afwezigheid daarin zegt niets definitiefs over latere gunningen.
- [Datasetpagina TenderNed](https://www.tenderned.nl/cms/aanbesteden-in-cijfers/datasets-aanbestedingen).
- Detail-endpoints van de 25 gevonden publicaties vanaf 2019 opgevraagd:
  13 beschikbaar, 12 met HTTP 404. Daarnaast zijn beide ontbrekend gekoppelde
  uitvragen opgevraagd. Acht officiële publicatie-PDF's zijn als tekst
  opgeslagen en gebruikt om winnaars en koppelingen te toetsen.
- [Bronbestanden, SHA-256-controlesommen en raadpleegmoment](bronnen.json).
- [Historische bronextractie](historisch.json), [actuele bronextractie](actueel.json).

Het auditscript wijzigt uitsluitend onderzoeksbestanden:

```powershell
python -m pip install -r requirements-onderzoek.txt
python onderzoek_ecorys.py --excel "Dataset_Tenderned-2016-01-01 tm 2025-12-31_Leeswijzer.xlsx" --label historisch
python onderzoek_ecorys.py --excel data/.cache/tenderned-dcf16f1e52e35736.xlsx --label actueel
python onderzoek_ecorys.py --vergelijk
```

`--vergelijk` gebruikt tevens de opgeslagen officiële metadata en
publicatieteksten. De twee bewezen koppelingen worden daarin gecontroleerd
op procedure-UUID, interne referentie en identieke projectlink. De bulkbestanden
zijn lokale bronnen; het actuele bestand wordt in de genegeerde downloadcache
bewaard.
