# Herstel van gunningskoppelingen — 8 november 2026

Het probleem beperkte zich niet tot Ecorys. Bij 104 van de 929 bestaande
opdrachten stond de gunning onder een ander TenderNed-kenmerk. De oude
koppeling op alleen dat kenmerk miste de winnaar. Daarnaast telde het
dashboard alleen de eerste winnaar van een opdracht.

Beide problemen zijn lokaal hersteld. De dagelijkse en periodieke pipeline
gebruiken nu dezelfde identificatiecontrole. De wijzigingen zijn nog niet
gepubliceerd op de online dashboardomgeving.

## Resultaat

| Controle | Voor herstel | Na herstel |
| --- | ---: | ---: |
| Opdrachten in dataset | 929 | 929 |
| Opdrachten met een bekende winnaar | 440 | 544 |
| Opdrachten met Ecorys in de onderliggende data | 4 | 6 |
| Opdrachten die voor Ecorys meetellen in het dashboard | 2 | 6 |
| Opdrachten met Panteia B.V. in de onderliggende data | 13 | 17 |
| Opdrachten met ATKB B.V. in de onderliggende data | 7 | 9 |

115 opdrachten hebben meerdere winnaars. Elk bureau telt per opdracht één
keer mee, ook bij meerdere percelen. Daardoor kan de som van gewonnen
opdrachten per bureau hoger zijn dan het aantal opdrachten. Het dashboard
biedt een winnaarfilter en een knop om alle bureaus te tonen; Ecorys is dus
ook buiten de standaard topselectie terug te vinden. Een filter op Ecorys
toont de zes opdrachten én de andere bureaus die daarop mede-winnaar zijn.

De oorspronkelijke ids, uitvraagdatums, classificaties en documentkoppelingen
van alle 929 opdrachten zijn behouden. De 19 overige wijzigingen in
winnaarslabels betreffen naamnormalisatie en samenvoeging van aantoonbare
naamvarianten; de oorspronkelijke bronnamen blijven opgeslagen.

## Bewijs en broncontrole

De historische TenderNed-Excel tot en met 2025 en de nieuwste beschikbare
Excel van 1 januari 2021 tot 1 oktober 2026 zijn samen onderzocht. De nieuwste
versie gaat voor bij dezelfde publicatie-id. Uit 74.306 gunningspublicaties
vanaf 2019 zijn 1.157 kandidaten geselecteerd voor vergelijking met deze
dashboardselectie. Er zijn 545 bevestigde gunningspublicaties gekoppeld aan
544 opdrachten.

De 104 opdrachten met een afwijkend gunningskenmerk hebben samen 105
gekoppelde publicaties:

- 100 hebben dezelfde procedure-UUID in de officiële publicaties.
- De overige vijf hebben hetzelfde referentienummer én hetzelfde project-id
  op het aanbestedingsplatform. Deze vijf zijn afzonderlijk gecontroleerd.

Een titelovereenkomst is uitsluitend aanleiding voor onderzoek. Een koppeling
vereist een identifier, mag niet meerdere opdrachten aanwijzen en wordt
geweigerd bij tegenstrijdige procedure-UUID's of een gunning vóór de uitvraag.
Voor iedere geaccepteerde koppeling zijn publicatie-id, bewijsmethode en
bronbestand vastgelegd. Winnaars, KvK-nummers en percelen zijn terug te voeren
op de oorspronkelijke Excel-rijen, inclusief hash van het bronbestand.

Voorbeelden van herstelde opdrachten:

| Uitvraag | Gunningspublicatie | Bevestigde winnaar |
| --- | --- | --- |
| [BZK-EA Uitvoeren lastendrukmetingen (322538)](https://www.tenderned.nl/aankondigingen/overzicht/322538) | [361925](https://www.tenderned.nl/aankondigingen/overzicht/361925) | Ecorys Nederland B.V. |
| [BZ ROVK Evaluations (335914)](https://www.tenderned.nl/aankondigingen/overzicht/335914) | [365375](https://www.tenderned.nl/aankondigingen/overzicht/365375) | Onder andere Ecorys Nederland B.V. |
| [Herijkingsonderzoek studentenreisproduct (391229)](https://www.tenderned.nl/aankondigingen/overzicht/391229) | [430159](https://www.tenderned.nl/aankondigingen/overzicht/430159) | Panteia B.V. |
| [Rivierkreeftenmonitoring (408855)](https://www.tenderned.nl/aankondigingen/overzicht/408855) | [426765](https://www.tenderned.nl/aankondigingen/overzicht/426765) | ATKB B.V. |

## Wat onzeker blijft

De controlelijst bevat 46 publicaties: 44 hebben wel een mogelijke
titelovereenkomst maar onvoldoende identificatiebewijs; twee bevatten geen
bevestigde winnaar. Dit zijn geen 46 bewezen gemiste opdrachten. Daarnaast
zijn 268 titelcombinaties uitgesloten vanwege een onmogelijke datumvolgorde
of een andere procedure-UUID.

Bij 22 KvK-clusters zijn de bronnamen onvoldoende eenduidig om automatisch
samen te voegen. Sommige kunnen handelsnamen zijn, maar er zitten ook
duidelijke tegenstrijdigheden in de bron: 09159322 staat bij ResearchNed én
Maastricht University, en 41197444 bij SEO én Boston Consulting Group.
Deze clusters worden niet blind samengevoegd; de oorspronkelijke namen en
het bronconflict blijven zichtbaar in de data. Het aantal unieke bureaus is
daarom nog gevoelig voor niet opgeloste naamvarianten.

Voor 63 oudere datasetpublicaties levert het TenderNed-detailendpoint een 404;
de beschikbare bulkgegevens blijven behouden. De CPV-afbakening, ontbrekende
historische uitvragen en niet openbaar gepubliceerde opdrachten zijn met dit
koppelherstel niet aangevuld. Het dashboard blijft een marktselectie, geen
volledig overzicht van alle door Ecorys of andere bureaus gewonnen opdrachten.

Winnaarsnamen volgen de periodieke Excel, momenteel tot 1 oktober 2026.
De dagelijkse API kan recentere gunningen herkennen; die blijven bewaard
wanneer een oudere Excel wordt ingelezen, ook als de winnaar nog ontbreekt.
De aanvullende API-controle op 8 november 2026 om 16:24 UTC heeft vier
recentere gunningen gekoppeld: uitvragen 423645, 395293, 380855 en 359614.
Deze staan nu als gegund, maar nog zonder winnaar omdat de API die namen
niet levert. Het aantal opdrachten met een bekende winnaar blijft 544.

## Bestanden en verificatie

- [Wijzigingen per opdracht, te openen in Excel](herstel_koppelingen.csv).
- [Bewijs en controlelijst bij dit herstel](onderzoek_ecorys/koppelrapport_herstel_2026-11-08.json).
- [Actueel periodiek koppelrapport](../data/koppelrapport.json).
- [Eerste onderzoek naar Ecorys](onderzoek_ecorys/rapport.md), als historische uitgangssituatie.

23 regressietests slagen, inclusief behoud van documentidentiteit,
tegenstrijdige identifiers, herhaalbaarheid, mede-winnaars, KvK-conflicten en
recentere API-gunningen. De datasetcontrole bevestigt overeenstemming tussen
JSON, CSV, concurrentiekaart, trendanalyse en de 725 beschikbare documenten.
Een browsertest in Edge bevestigt zes Ecorys-opdrachten, zeventien
Panteia-opdrachten, alle bureaus in de tabel en werkende links naar de
gunningspublicaties, zonder JavaScript-fouten.
