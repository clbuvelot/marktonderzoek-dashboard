# Welke vragen horen bij de teams binnen Regions & cities?

De classificatie moet herkennen **welke vraag de opdrachtgever beantwoord wil
hebben, waarop die vraag betrekking heeft en welk product wordt gevraagd**.
Daaruit volgt inhoudelijke aansluiting bij één of meer teams. Een onderwerp,
onderzoeksmethode of de naam van een opdrachtgever alleen is onvoldoende.

## Basis en betekenis van de bronnen

Onderzocht zijn de 188 aangeleverde projectprofielen met unitcode NL5300
(Regions & cities). Daarvan hebben 185 bruikbare inhoud; drie waren al uitgesloten
vanwege ontbrekende of onvolledige samenvattingen. De velden `team` en `team_naam`
zijn in de bronapp gevuld vanuit de projectmap/unitcode, zoals blijkt uit
`backend/scripts/backfill_report_teams.py`. Zij noemen **units**, niet de teams
binnen Regions & cities. Ze vormen dus geen gelabelde trainingsset voor de
werkelijke historische teamverdeling.

De analyse gebruikt onderzoeksvragen, thematiek en gevraagde producten uit die
profielen, naast Ecorys' openbare dienstenbeschrijvingen. De hieronder gebruikte
interne teamnamen en hun plaats binnen Regions & cities volgen de informatie
van de gebruiker. TIM staat op hetzelfde organisatieniveau als Regions & cities.
De openbare pagina Energie & Klimaat onderbouwt inhoudelijk aanbod, maar bewijst
geen exacte organisatorische gelijkstelling met Sustainable Transitions.

De concrete profielen, bronverwijzingen en beslisregels zijn ook gestructureerd
vastgelegd in [rc_teamprofielen_voorstel.json](rc_teamprofielen_voorstel.json).
De implementatie op basis van dit onderzoek is beschreven in [teamindeling.md](teamindeling.md). Het JSON-voorstel bewaart de oorspronkelijke onderzoeksbevindingen.

## Vier inhoudelijke profielen

| Team | De kenmerkende vraag | Typen opdrachten en producten |
| --- | --- | --- |
| **Evaluaties** | Werkt beleid of een organisatie zoals bedoeld, waarom wel/niet en hoe kan het beter? | Doeltreffendheid, doelmatigheid, uitvoering en governance; evaluaties van regelingen, fondsen, programma's en publieke organisaties; beleidstheorie, monitoring- en evaluatieplannen. |
| **Brede Welvaart** | Welke keuze levert maatschappelijke waarde op, voor wie, waar en wanneer? | MKBA/BWA, maatschappelijke afwegingskaders, economische en maatschappelijke impact, verdelingseffecten; woon- en werklocaties, behoefteonderzoek, regionale visies, strategieën en bredewelvaartsmonitors. |
| **Sustainable Transitions** | Hoe verduurzamen we, welke maatregelen en systemen zijn mogelijk en wat zijn de kosten, effecten en voorwaarden? | Warmte, energie, netcongestie, circulaire economie; scenario's, energiemodellen, techno-economische analyses, beleidsinstrumenten, evaluaties en transitieadvies. |
| **Visitaties** | Hoe presteert een woningcorporatie maatschappelijk, en wat kan zij leren over haar keuzes en samenwerking? | Maatschappelijke visitaties volgens SVWN; beoordeling van prestaties, verankering, besturing en capaciteit; gesprekken met huurders, gemeenten en partners; visitatierapport en terugkoppeling. |

### Evaluaties: werking, uitvoering en leren

De profielen bevatten onder meer evaluaties van MKB-fondsen, een staatsdeelneming
in de energiesector en een regeling rond erfgoed en gebiedsontwikkeling. De
vragen gaan verder dan alleen het meten van effecten: ook fondsbeheer,
governance, samenwerking, uitvoering en verbeteropties komen terug.
Een profiel voor een monitoring- en evaluatiehandboek onderbouwt dat ook het
ontwerpen van een evaluatieaanpak relevant is.

Daarom is “alleen ex-post effectevaluatie” te smal. Maar “iedere enquête of ieder
onderzoek” is te ruim: er moet een concrete evaluatieve, uitvoeringsgerichte of
monitoringsvraag voor beleid of publieke organisaties zijn.

### Brede Welvaart: keuzes, waarde, ontwikkeling en verdeling

Ecorys beschrijft zelf MKBA/BWA, het maatschappelijk afwegingskader, visies en
strategieën, monitoring/evaluatie, governance en impactstudies. Genoemde visies
betreffen onder meer economie, wonen, bedrijventerreinen en steden/regio's.
De lokale profielen bevatten onder meer een MKBA voor digitale identiteit, een
raming van de behoefte aan werklocaties en een MKBA van OV en wonen.

Daarmee is het team breder dan alleen onderzoek waarin letterlijk “brede
welvaart” staat. De rode draad is onderbouwde maatschappelijke, economische en
ruimtelijke besluitvorming. Zuivere vastgoedtaxatie, grondprijsberekening of
financiële controle mag echter niet alleen vanwege NL5300 automatisch onder
dit team vallen; die opdrachten vragen een nadere expertiseafbakening.

### Sustainable Transitions: inhoudelijke transitie-expertise

De profielen bevatten onder andere warmteprogramma's met Vesta-MAIS,
maatschappelijke kosten van netcongestie, varianten van een plasticbelasting
en minimumnormen voor gerecycled/biobased plastic. Het aanbod omvat dus
beleidsontwerp, regelgeving, uitvoerbaarheid, markten, modellen en advisering,
naast evaluatie en effectanalyse.

De openbare pagina Energie & Klimaat bevestigt strategieontwikkeling,
beleidsevaluaties, energie- en simulatiemodellen, markt-/investeringsanalyses,
data-analyse en dashboards. Openbare circulaire-economieprojecten onderbouwen
ook keten-, ruimte- en beleidsvragen buiten energie alleen.

Technische aanleg en ecologische veldinventarisatie zijn iets anders dan dit
aanbod. **Techno-economische modellen uitsluiten omdat ze technisch zijn, zou
juist aantoonbare expertise wegfilteren.**

### Visitaties: een specifieke opdrachtvorm en sector

Eén lokaal profiel is expliciet een maatschappelijke corporatievisitatie
(project 1004877). Het beschrijft SVWN-methodiek 7 met maatschappelijke waarde,
verankering, besturing en capaciteit. De openbare Ecorys-pagina bevestigt deze
specialisatie en noemt de perspectieven van huurders, gemeenten en lokale
partners, verantwoording én leren.

Het dashboard bevat ook een uitvraag “Evaluatiebureau ten behoeve van
visitatiediensten” van een hogeschool (TenderNed-id 420200). Dat is geen bewijs
voor aansluiting bij het corporatievisitatieteam. Alleen het woord “visitatie”
zou hier tot een foutieve koppeling leiden. Evenmin maakt een woningcorporatie
als opdrachtgever elke woningmarktstudie of enquête tot een visitatie.

## Bewust meerdere teams

De teams vertegenwoordigen deels verschillende perspectieven op dezelfde
opdracht. Beoordeel ze onafhankelijk; kies geen hoofdteam en vereis geen aparte
werkpakketten voor een tweede match.

| Vraag | Onderbouwde aansluiting |
| --- | --- |
| Evaluatie van de werking van energiebeleid | Evaluaties + Sustainable Transitions |
| MKBA van warmtevarianten | Brede Welvaart + Sustainable Transitions |
| Evaluatie van een regionale regeling met maatschappelijke en verdelingseffecten | Evaluaties + Brede Welvaart |
| Evaluatie van klimaatbeleid met MKBA en verdelingseffecten | Evaluaties + Brede Welvaart + Sustainable Transitions |
| Reguliere maatschappelijke corporatievisitatie | Visitaties; standaardpassages over duurzaamheid of betaalbaarheid voegen niet automatisch andere teams toe |
| MKBA van OV en wonen | Brede Welvaart; daarnaast kan de **unit TIM** relevant zijn |

Een energieonderwerp alleen is geen bewijs voor Brede Welvaart; een MKBA alleen
is geen bewijs voor een gevraagde beleidsevaluatie. De concrete vraag en het
eindproduct moeten de combinatie dragen. Overlap mag hetzelfde onderdeel betreffen.

## Gevolgen voor de classifier

1. Bewaar **unit en team als verschillende niveaus**. NL5300/Regions & cities en
   TIM zijn geen alternatieven voor Evaluaties of Brede Welvaart in één teamlijst.
2. Extraheer eerst de hoofdvragen, het object/de sector en de producten. Gebruik
   de echte opdrachtomschrijving en relevante documentpassages.
3. Beoordeel elk team afzonderlijk: passend, niet passend of onvoldoende informatie.
   Geef bij een positieve match een reden en een citaat uit de uitvraag.
4. Gebruik inhoudelijk vergelijkbare projecten als referentie, maar behandel de
   unitcode niet als bewijs van een historisch uitvoerend team.
5. Herzie te grove uitsluitingen. Ecorys biedt expliciet workshops/training over
   brede welvaart, procesbegeleiding, modellen en dashboards aan. De inhoudelijke
   opdracht bepaalt de aansluiting; generieke personeelsinhuur, IT-levering of
   opleiding is daarmee nog niet automatisch passend.
6. Laat twijfel zichtbaar. Geen aansluiting op deze vier teams betekent niet dat
   een opdracht voor heel Ecorys ongeschikt is. Bijvoorbeeld sectorale expertise
   van andere units kan wel relevant zijn.

## Bronnen en controle

Geraadpleegd op 8 november 2026:

- [Ecorys — Brede welvaart](https://www.ecorys.com/brede-welvaart/)
- [Ecorys — Maatschappelijke visitaties](https://www.ecorys.com/maatschappelijke-visitaties/)
- [Ecorys — Energie en Klimaat](https://www.ecorys.com/energie-en-klimaat/)
- [Ecorys — Ruimtelijke impact van circulaire economie in het NZKG](https://www.ecorys.com/case-studies/spatial-impact-of-the-circular-economy-in-the-north-sea-canal-area/)
- [Ecorys — Circulaire economie en de klimaatopgave](https://www.ecorys.com/case-studies/circular-economy-and-the-climate-challenge/)
- [Ecorys — Beleid en versnelling van de circulaire transitie](https://www.ecorys.com/case-studies/how-is-the-netherlands-taking-the-lead-in-the-circular-economy/)

Concrete lokale onderbouwing: profielen 1006438, 1002957, 1006605, 1003725,
1001051, 1001219, 1004855, 1006930, 1006599, 1007206, 1008138 en 1004877.
De voorbeeldvragen komen uit deze profielen; er is geen uitspraak gedaan over
welk intern team deze projecten feitelijk heeft uitgevoerd. De controle op
profiel-id's, unitcodes en aanwezigheid van vraag-/inhoudsvelden is lokaal
vastgelegd in de genegeerde onderzoekscache. De toepassing op tenders wordt afzonderlijk
verantwoord in [teamindeling.md](teamindeling.md).
