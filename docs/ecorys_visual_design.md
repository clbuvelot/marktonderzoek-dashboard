# Visueel ontwerp — Ecorys Marktinzicht

Ontworpen en doorgevoerd op 8 november 2026, op basis van onderzoek naar de
publieke Ecorys-website. De twee bestaande werkwijzen, zoekfunctie, filters
en gegevens blijven de basis; het ontwerp maakt hun hiërarchie duidelijker.

## Onderzochte bronnen

- [Ecorys Nederland](https://www.ecorys.com/nl/): logo, lettertype, blauw,
  witruimte en de schuine begrenzing van kleurvlakken.
- [Ons werk](https://www.ecorys.com/nl/our-work/): lichte header, subtiel
  vierkantpatroon, duidelijke koppen, rechthoekige knoppen en filterpresentatie.
- [Ons bedrijf](https://www.ecorys.com/nl/our-company/): samenhang tussen
  typografie, inhoud en visuele identiteit.
- [Publieke stylesheet](https://www.ecorys.com/app/themes/pulsar/dist/theme/app-styles.css):
  Montserrat 400/600. De kleurvariabelen zijn uit de publieke paginabron gelezen.

Screenshots en onderzochte bronbestanden staan lokaal onder
`data/.cache/ecorys-design/`. Dit is een toepassing van de zichtbare
Ecorys-identiteit op een werkomgeving, geen kopie van een marketingpagina.

## Ontwerpbeslissingen

| Onderdeel | Uitwerking |
| --- | --- |
| Logo | Originele SVG-paden en kleuren uit de officiële header, in de juiste verhouding |
| Lettertype | Montserrat, normaal 400 en semibold 600; lokaal geladen |
| Primair blauw | `#006EB8`, voor navigatie, acties en datavisualisatie |
| Donkerblauw | `#002137`, voor koppen, cijfers en duidelijk contrast |
| Neutraal | Ecorys-tekst `#2E2E2E`, logogrijs `#505354`, afgeleide lichte werkvlakken |
| Accent | Ecorys-teal voor tweede grafiekreeks; oranje `#FAA638` markeert naderende deadlines |
| Geometrie | Subtiel vierkantpatroon, dunne lijnen, vrijwel rechte hoeken; schuine afsluiting in login |
| Schaal | Grote paginakop, middelgrote opdrachttitel, compacte metadata; cijfers met vaste cijferbreedte |

Het hoofdkleurgebruik is terughoudend: wit, neutrale tinten en blauw.
Statuskleuren zijn donkerdere, leesbare varianten met een tekstlabel.
De officiële teal is voor tekst donkerder gemaakt; kleur is niet de enige
drager van betekenis. Focus krijgt een donkere rand met de gele Ecorys-focuskleur.

Tenders krijgen twee visuele ankerpunten: de titel en de deadline. Een
deadline binnen zeven dagen krijgt een oranje rand en een expliciet label.
Op mobiel staat de deadline direct bij de titel. De broncontroledatum blijft
ook op een klein scherm beschikbaar.

Op brede schermen staan inschrijfselectie en primaire filters naast elkaar.
Dit beperkt de hoogte boven de eerste tender. Op kleinere schermen verdelen
de bediening en de kaarten zich over meerdere regels. De navigatie blijft
bereikbaar bij scrollen.

Het marktbeeld groepeert kengetallen in één rustig vlak, met dezelfde
typografie en kleuren als de grafieken. De details, login en documentvragen
gebruiken dezelfde knoppen, randen, kleuren en afstanden. Alle originele
winnaars- en bronkoppelingen blijven intact.

## Bestanden en herkomst

Het originele logo, de twee officiële webfonts, fontlicentie en bronverwijzingen
staan in `assets/ecorys/`. `sources.json` bevat de download-URL's en hashes van
de lettertypebestanden. Montserrat wordt geleverd onder de SIL Open Font License;
de bijbehorende licentie is opgenomen. Er is geen Google Fonts-aanroep meer.

De vormgeving staat in `dashboard.css` en `dashboard-chat.css`. De HTML gebruikt
het echte beeldmerk; `dashboard.js` voegt consistente SVG-iconen, deadlinepresentatie
en grafiektypografie toe. De dataset en koppelpipeline zijn niet gewijzigd.

## Verificatie

De bestaande browserroute controleert de echte dataset en gecontroleerde
testgevallen. Aanvullend worden lokale font- en logolading, mobiele deadlines,
zichtbare broncontroledatum en schermbreedten van 320, 390, 768 en 1440 pixels
gecontroleerd. De screenshots staan onder `data/.cache/ux/`.

De JavaScript-regressietests en de browsertest voeren geen betaalde
AI-aanroepen uit. Documentvragen worden in de browsertest gesimuleerd.
Het ontwerp staat lokaal klaar; online publicatie is een afzonderlijke stap.
