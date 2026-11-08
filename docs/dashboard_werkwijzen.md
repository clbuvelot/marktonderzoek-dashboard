# Tenders vinden en uitvragen onderzoeken

De twee ingangen ondersteunen een ander gebruiksdoel met dezelfde TenderNed-data.
Er is geen datamigratie of nieuwe backend nodig. Alle dashboardbestanden moeten
bij publicatie samen worden overgenomen; de HTML verwijst naar lokale JS- en
CSS-bestanden. De bestaande login blijft van toepassing.

## Tenders vinden

Het eerste bezoek opent de tenders met een toekomstige, geldige deadline,
zonder bekende gunning of beëindiging. De lijst sorteert op eerstvolgende
deadline. Zoeken, team, onderwerp en opdrachtgever helpen de selectie te
verkleinen; de deadline kan worden beperkt tot zeven of dertig dagen.
Onder Meer filters staat alleen procedure. Vijftig resultaten worden per
stap getoond. Team verwijst naar inhoudelijke aansluiting bij één of meer van
de zes afdelingen uit de historische rapportprofielen. De onderbouwing staat
bij de opdracht; onvoldoende informatie en werk buiten het beschreven aanbod
krijgen verschillende labels. Zie [teamindeling](teamindeling.md).

Eén uitvraag kan bij meerdere teams passen, ook voor hetzelfde onderdeel. Alle
passende teams zijn gelijkwaardige ingangen naar dezelfde uitvraag. Een combinatie
van teamfilters selecteert de gezamenlijke verzameling zonder dubbele uitvragen;
de volgorde van teams geeft geen voorkeur of hoofdteam aan.

De knop Inschrijfstatus onbekend bevat alleen niet-gegunde, niet-beëindigde
opdrachten zonder bruikbare deadline. Deze lijst sorteert op nieuwste
publicatie. Een nulresultaat wordt nooit opgevuld met gesloten opdrachten.

De deadline is Nederlandse tijd. Tijdstempels zonder offset worden in
Europe/Amsterdam geïnterpreteerd, inclusief zomer-/wintertijd. Een expliciete
offset blijft geldig. Exact op de deadline is de inschrijving gesloten.
De selectie wordt elke minuut en bij terugkeer naar het tabblad bijgewerkt.

## Uitvragen onderzoeken

Deze ingang bevat standaard alle opdrachten. Onderwerp, opdrachtgever,
periode en winnaar zijn de primaire filters. Periode verwijst naar de
oorspronkelijke uitvraagdatum, niet naar de gunningsdatum.

Onder Meer filters staan team, inschrijf- en gunningsstatus, methodiek,
evaluatiefase en causaal effect. De inhoudelijke specialistische filters
zijn niet beschikbaar in Tenders vinden. Procedure is daarentegen alleen
een filter bij tenders; de procedure blijft in alle opdrachtdetails staan.

De filters Kwantitatief, Nationaal/Europees, Aard opdracht, Herhalingskans,
Consortiumkans en Opdrachtgrootte zijn verwijderd. Ook de keuze voor een
AI-betrouwbaarheidsscore vervalt. Het menu Verdeel op gebruikt dezelfde
beperkte onderzoeksdimensies. De onderliggende bron- en classificatievelden
zijn niet verwijderd of opnieuw geclassificeerd.

Uitvragen en Marktbeeld delen één selectie. Elke grafiek en elk bureau
heeft een bedienbare link naar de onderliggende opdrachten. Ook Overig
verwijst naar de exacte verzameling publicatie-ids. De concurrentiekaart
telt iedere bekende mede-winnaar eenmaal per opdracht.

Bij meer dan drie winnaars toont de lijst een compacte samenvatting met een
uitklapbare lijst van de overige namen. Een bureau uit het actieve
winnaarfilter staat vooraan. Alle namen blijven meetellen en doorzoekbaar.

De bekende-winnaardekking wordt zichtbaar gemaakt: geen bekende winnaar is
niet hetzelfde als niet gegund. Beëindigd, gesloten, onbekende inschrijfstatus
en gegund worden afzonderlijk benoemd. Een gekoppelde gunning gaat vóór de
oorspronkelijke publicatiestatus en deadline.

## Zoeken en terugkeren

Zoeken is ongevoelig voor hoofdletters en accenten. Alle ingevoerde woorden
moeten ergens in titel, omschrijving, opdrachtgever, winnaars, onderwerp,
subthema, methode of teams voorkomen. Het is geen semantische zoekfunctie en
doorzoekt niet de documentbestanden. Binnen een filter geldt of; tussen
verschillende filters en zoekwoorden geldt en.

Actieve selecties blijven zichtbaar, ook als hun filter onder Meer filters
staat. Wis filters wist de zoekopdracht, filterwaarden en context- of
grafiekselectie van de actieve ingang. De gekozen ingang en inschrijfselectie
blijven staan. Bij nul resultaten kan de gebruiker dus veilig opnieuw zoeken.

De browser bewaart de laatste ingang en de instellingen per ingang onder
`ecorys-dashboard-ux-v1`. De terug-/vooruitgeschiedenis bewaart tevens
momentopnamen, zodat doorklikken en teruggaan filters en lijstpositie
herstellen. Bij geblokkeerde of beschadigde browseropslag blijft het
dashboard werken zonder blijvende voorkeuren. Sleutels en documentgesprekken
worden niet in deze voorkeuren opgeslagen.

Filterinstellingen hebben revisie 3. Bij het inlezen van oudere instellingen
vervallen verwijderde of niet-toepasselijke filters en AI-scoregrenzen.
Oude grafiekselecties worden gewist, omdat hun lijst met opdracht-ids nog op
verwijderde filters kan berusten. Oude teamlabels Evaluaties, Brede Welvaart en
Sustainable Transitions vervallen met een zichtbare melding. Ze worden niet
willekeurig omgezet naar afdelingen; TIM blijft geldig. Zoektekst, overige geldige filters
en voorkeuren blijven behouden. Nieuwe grafiekselecties blijven bij teruggaan
en herladen wel intact.

De volgende URL-hashes zijn beschikbaar:

- `#tenders`
- `#onderzoek/uitvragen`
- `#onderzoek/markt`
- `#tenders/opdracht/322538` en `#onderzoek/uitvragen/opdracht/322538`

Een opdrachtlink opent het record ongeacht actieve filters. De link deelt
de opdracht en weergave; persoonlijke filters worden niet in de URL gezet.
Een ontbrekende opdracht krijgt een melding met een terugknop.

## Van actuele kans naar eerdere uitvragen

Eerdere uitvragen van deze opdrachtgever maakt een expliciete nieuwe
onderzoeksselectie: dezelfde opdrachtgever, met een uitvraagdatum vóór de
geopende opdracht. Vergelijkbare opdrachten selecteert hetzelfde
AI-geclassificeerde onderwerp en sluit de huidige opdracht uit. De knop
ontbreekt bij een onbekend of onduidelijk onderwerp. Er wordt geen nieuw
AI-oordeel over overeenkomst of geschiktheid gegenereerd.

De detailweergave bevat de bron, documenten, onderzoekskenmerken en alle
bekende gunningspublicaties. De nadruk verschilt per ingang: deadline bij
tenders, uitkomst bij onderzoek. Dezelfde bronidentiteit blijft gebruikt.

## Controle en grenzen

De pure JavaScript-tests controleren status, Nederlandse tijd, zoekwoorden,
filters, contextselecties en exacte grafiekkoppelingen. De Edge-browsertest
controleert navigatie, bewaarde selecties, beide schermformaten, de bestaande
Ecorys-/Panteia-tellingen, datumfilters, directe links, gesimuleerde
documentvragen en fouten bij laden of browseropslag. Bestaande Python-tests
en datasetcontroles blijven onderdeel van de verificatie.

Marktgrafieken gebruiken Chart.js; bij een laadfout blijven de aantallen en
links naar opdrachten beschikbaar. Externe bronnen en AI-classificaties
behouden hun bestaande beperkingen. Bewaarlijsten, meldingen, nieuwe
AI-aanbevelingen en vergelijken naast elkaar zijn geen onderdeel van deze
versie. De wijzigingen zijn lokaal; online publicatie volgt afzonderlijk.
