# Team- en unitaansluiting

Het dashboard onderscheidt **units** en **teams binnen Regions & cities**.
De vier teamprofielen zijn Evaluaties, Brede Welvaart, Sustainable Transitions en
Visitaties. TIM staat naast Regions & cities als unit. Ook de overige historisch
onderbouwde units blijven apart beschikbaar: Resilient Economies, Social and
Health, Natural Resources en Security & Justice.

## Onderbouwing

De aangeleverde rapportprofielen bevatten 393 projecten, waarvan 377 bruikbare
inhoud hebben. De 188 profielen met unitcode NL5300 leveren 185 bruikbare
R&C-referenties. De bronapp vulde de velden team en team_naam vanuit projectmappen
en unitcodes; die velden bewijzen geen uitvoerend intern team.

De R&C-teamprofielen zijn daarom inhoudelijk opgebouwd uit onderzoeksvragen,
onderwerpen en producten, aangevuld met openbare Ecorys-diensten.
Zie [het onderzoek met bronverwijzingen](onderzoek_rc_teams.md).
De interne teamnamen en hun organisatorische plaats volgen de informatie van
de gebruiker. De referenties zijn geen volledige expertise- of capaciteitsinventaris.
Auteursnamen worden niet gebruikt om teams te voorspellen.

## Classificatie

De actieve catalogus staat in [teamprofielen.json](../teamprofielen.json).
Versie 4 bepaalt per opdracht:

1. De vraag, het object/onderwerp en de gevraagde producten.
2. Voor elk van de zes units: passend, niet passend of onvoldoende informatie.
3. Voor elk van de vier R&C-teams: dezelfde afzonderlijke beoordeling.
4. Voor elke positieve match een inhoudelijke reden en een letterlijk broncitaat.

Alle passende teams en units worden geselecteerd. Er is geen hoofdteam en geen
maximum van bijvoorbeeld twee of drie matches. De volgorde geeft geen voorkeur.
Een passend R&C-team impliceert aansluiting op Regions & cities. Omgekeerd kan
een opdracht bij een unit passen zonder dat een van de beschreven teams
onderbouwd kan worden toegewezen. Onzekere beoordelingen blijven bewaard.

| Team | Kern van de aansluiting |
| --- | --- |
| Evaluaties | Werking, effecten, doelmatigheid en uitvoering van beleid of publieke organisaties; ook evaluatieontwerp. |
| Brede Welvaart | Maatschappelijke, economische en ruimtelijke keuzes; MKBA, impact, verdeling, woon-/werklocaties, visies en monitors. |
| Sustainable Transitions | Energie, warmte, klimaat en circulaire transities; beleid, scenario's, modellen, haalbaarheid en advies. |
| Visitaties | Maatschappelijke visitaties van woningcorporaties volgens SVWN. |

Een evaluatie van energiebeleid kan Evaluaties én Sustainable Transitions krijgen.
Een MKBA van warmtealternatieven kan Brede Welvaart én Sustainable Transitions
krijgen. Een reguliere corporatievisitatie krijgt niet automatisch extra teams
vanwege standaardpassages over duurzaamheid en betaalbaarheid.
Onderwijsvisitaties worden niet op het woord visitatie aan corporatievisitaties gekoppeld.

Inhoudelijke workshops, energiemodellen, onderzoeksdashboards en transitiebegeleiding
kunnen passen. Generieke training, personeelsinhuur, technische realisatie,
vergunningbehandeling en ecologische inventarisatie worden niet op een onderwerp
alleen aan een team gekoppeld. Een raamovereenkomst voor onderzoek is geen
personeelsinhuur; ondersteunende ICT-eisen maken een onderzoek geen IT-levering.

## Broncontext en gegevens

De classifier gebruikt titel, omschrijving en maximaal 16.000 tekens geselecteerde
passages uit alle lokaal beschikbare documentdelen. Voorbladen en lange technische
bijlagen verdringen zo niet automatisch de inhoudelijke opdracht. De selectie
blijft beperkt; niet-lokaal beschikbare stukken kunnen niet worden meegewogen.

Een daadwerkelijke sectiekop voor opdrachtbeschrijving of onderzoeksvragen krijgt
ruimte als de gewone trefwoordselectie die inhoud mist. Inhoudsopgaven tellen niet
als inhoudelijke sectie. Dit herstelde onder meer de selectie bij een training over
energiebeleid: de logistieke passages verdrongen eerder de inhoudelijke leervraag.
Bij 59 uitvragen veranderden hierdoor de gelezen passages; die zijn opnieuw beoordeeld.

De classificatievelden zijn:

- opdrachtprofiel: vraag, object, producten.
- teams en units: alle onderbouwde matches, op afzonderlijke organisatieniveaus.
- teams_afwegingen en units_afwegingen: alle individuele oordelen, redenen en citaten.
- teams_matches en units_matches: de positieve beoordelingen.
- teams_status en units_status: passend, buiten_scope of onduidelijk voor dat niveau.
- teams_toelichting: uitleg van aansluiting op de vier R&C-teams.
- teams_versie: hash van catalogus, instructies en schema.
- teams_review: alleen aanwezig bij een actuele, bron- en versiegebonden bronreview.

Een lege teamlijst zegt niets over de overige units of over inschrijfgeschiktheid.

## Dashboard

De filter **Team of unit** bevat één groep voor Regions & cities, met de keuze
voor de hele unit en daaronder de vier teams. Een tweede groep bevat de andere units.
Meerdere keuzes werken als OF; een opdracht telt eenmaal. Andere filters worden
daarmee gecombineerd met EN. Er komen geen extra filters bovenaan bij.

De detailpagina toont vraag en producten, teammatches met broncitaten, onzekere
teambeoordelingen en uitklapbare unitmatches. Herkenbare opgeslagen keuzes blijven
behouden; onbekende labels vervallen met een melding. Winnaar-, datum- en zoekfilters
blijven per werkwijze bewaard.

## Herbeoordelen en controleren

### Resultaat op 8 november 2026

Alle 929 uitvragen zijn opnieuw beoordeeld. Voor 353 is een unit onderbouwd;
194 hebben onvoldoende informatie en 382 vallen buiten het beschreven aanbod.
150 uitvragen passen bij een of meer R&C-teams, waarvan 13 bij twee teams.
109 uitvragen passen bij meerdere units. Een uitvraag telt in de lijst steeds eenmaal.

| R&C-team | Uitvragen |
| --- | ---: |
| Evaluaties | 71 |
| Brede Welvaart | 46 |
| Sustainable Transitions | 46 |
| Visitaties | 0 |

Er is in deze dataset geen onderbouwde maatschappelijke corporatievisitatie.
De aangetroffen onderwijsvisitatie krijgt daarom geen Visitaties-label. De keuze
blijft beschikbaar voor toekomstige passende uitvragen.

Ten opzichte van de voorgaande unitindeling kregen 63 uitvragen zonder aansluiting
nu een unit; 23 eerdere toewijzingen zijn niet meer onderbouwd. Die vergelijking
betreft units, niet de aantallen van de nieuw ingevoerde R&C-teams.

De 59 gewijzigde documentselecties zijn afzonderlijk opnieuw beoordeeld. De nieuwe
context maakte onder meer duidelijk dat het Trainingsprogramma Energie Transitie
inhoudelijke kennisoverdracht vraagt en bij Sustainable Transitions past.
Bij drie uitvragen (225195, 245084 en 238711) zijn de positieve beoordelingen aan
de bron getoetst en de modelcitaten vervangen door letterlijke bronfragmenten.
Deze controles zijn bron- en versiegebonden vastgelegd in teamreviews.json.

De controles omvatten 46 Python-tests, 19 JavaScript-tests, acht synthetische
grensgevallen via de dagelijkse classificatie-API en de Edge-browsertest.
Alle oorspronkelijke broncitaten en het behoud van overige gegevens zijn
gecontroleerd. De 544 opdrachten met bekende winnaar, waaronder zes met Ecorys
en zeventien met Panteia, blijven behouden.

### Werkwijze voor volgende updates

Nieuwe uitvragen en herbeoordelingen gebruiken dezelfde catalogus en regels.
Een volledige herbeoordeling is nodig bij de overgang van units-in-het-teamveld
naar deze structuur. Ook eerder uitgesloten opdrachten moeten opnieuw worden
gelezen omdat uitsluitingsregels zijn aangescherpt.

```powershell
python herclassificeer_teams.py
python controleer_teamherindeling.py data/geclassificeerd_met_winnaar.json data/.cache/teams/dataset-voorstel.json
python herclassificeer_teams.py --toepassen
python controleer.py
```

De herbeoordeling is hervatbaar. Antwoorden worden op bron, model en catalogusversie
gecachet. Voor toepassen worden volledigheid, letterlijke citaten en behoud van
alle overige gegevens gecontroleerd. Een backup gaat vooraf aan het vervangen
van de dataset en vernieuwen van de exports. Winnaars, koppelbewijzen, documenten,
datums, onderwerpen en methodieken worden niet door de herindeling gewijzigd.

De vergelijking staat in [teamherindeling.csv](teamherindeling.csv) en
[data/teamherindeling.json](../data/teamherindeling.json). Broncontrole van
TenderNed en inhoudelijke herclassificatie zijn afzonderlijke gebeurtenissen.

Automatische controles bewijzen schema-integriteit, herleidbare citaten en
gegevensbehoud. Synthetische grensgevallen en een inhoudelijke steekproef toetsen
de aansluiting. Dit is geen gemeten nauwkeurigheid op een onafhankelijk, door
de teams gelabeld testbestand.
