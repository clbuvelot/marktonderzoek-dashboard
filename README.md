# Ecorys – Marktonderzoek beleidsevaluatie

Dit dashboard ondersteunt marktpositionering en acquisitie voor Ecorys: welke
opdrachtgevers besteden onderzoek aan, welke methoden worden gevraagd, wie wint
de opdrachten en welke uitvragen passen bij de Ecorys-teams en units?

De toepassing bestaat uit één HTML-pagina en een Python-datapipeline. De actuele
dataset staat in `data/geclassificeerd_met_winnaar.json`. Grafieken, rapporten en
exports gebruiken deze dataset als bron. De actuele tellingen staan in
[het marktrapport](data/marktrapport.md); de laatste geslaagde broncontrole staat
in `data/update_staat.json` en bovenaan het dashboard.

## Lokaal gebruiken

Gebruik Python 3.12 of nieuwer. Voer de commando's uit vanuit deze projectmap:

```powershell
python -m pip install -r requirements.txt
python -m http.server 8000 --bind 127.0.0.1
```

Open http://localhost:8000/dashboard.html en gebruik het bestaande teamwachtwoord.
Bij een eerste bezoek opent **Tenders vinden**: actuele inschrijvingen met
zichtbare deadlines, zoekfunctie en filters voor team, onderwerp, opdrachtgever en deadline.
**Inschrijfstatus onbekend** toont afzonderlijk opdrachten waarvan de deadline
ontbreekt. Een ontbrekende deadline betekent niet dat inschrijving gesloten is.

**Uitvragen onderzoeken** doorzoekt alle opdrachten, inclusief actuele
uitvragen. Wissel tussen **Uitvragen** en **Marktbeeld** met dezelfde selectie.
Klik op grafieken of bureaus om de bijbehorende opdrachten te openen. Alle
mede-winnaars tellen mee. De periodefilters gebruiken de uitvraagdatum.

Zoeken combineert woorden uit titel, omschrijving, opdrachtgever, alle winnaars
en onderzoekskenmerken. Bij tenders bevat **Meer filters** alleen procedure;
bij onderzoek staan daar team, status, methodiek, evaluatiefase en causaal effect.
De speculatieve filters en de AI-scorekeuze zijn verwijderd. De selectie **Team of unit** onderscheidt de vier teams binnen
Regions & cities — Evaluaties, Brede Welvaart, Sustainable Transitions en Visitaties —
van de zes units, waaronder TIM. De profielen zijn gebaseerd op eerder Ecorys-werk
en openbare dienstenbeschrijvingen. Unitcodes bewijzen geen historisch uitvoerend team.
De classifier bepaalt eerst vraag, onderwerp en gevraagd product en beoordeelt
ieder team en iedere unit afzonderlijk. Alle onderbouwde matches blijven behouden.
Meerdere keuzes werken als OF en tellen een gedeelde uitvraag eenmaal.
De detailpagina toont de gevraagde prestatie, redenen, broncitaten en onzekere
teambeoordelingen. Een passende unit hoeft niet tot een van de vier teams te leiden;
geen R&C-team betekent dus niet dat een opdracht buiten het hele Ecorys-aanbod valt.
Zie [de verantwoording van de teamindeling](docs/teamindeling.md).
Instellingen worden per ingang onthouden in deze browser. De winnaarselectie
bij onderzoek beperkt dus niet de actuele tenders.

De opdrachttitel opent de gedeelde detailweergave met een aparte officiële
bronlink. **Vraag documenten** biedt een samenvatting en vervolgvragen wanneer
documenten beschikbaar zijn. Vanuit een opdracht zijn eerdere uitvragen van
dezelfde opdrachtgever en opdrachten over hetzelfde onderwerp bereikbaar.
De terugknop herstelt de voorgaande selectie en positie. Zie de
[beschrijving van de gebruikerservaring](docs/dashboard_werkwijzen.md).

De visuele vormgeving is gebaseerd op de officiële Ecorys-website: origineel
SVG-logo, lokaal geladen Montserrat en de Ecorys-kleuren. Zie de
[ontwerpverantwoording en bronnen](docs/ecorys_visual_design.md).
Publiceer de map `assets/ecorys/` samen met de dashboardbestanden.

## Dataset en interpretatie

- TenderNed is de bron. Er wordt gezocht op 13 CPV-codes voor onderzoek,
  enquêtes, economisch/sociaal onderzoek en evaluatieadvies; de lijst staat in
  `collect.py`. De selectie bevat ook onderzoeks- en advieswerk dat geen
  beleidsevaluatie is. Gebruik de inhoudelijke en teamfilters voor verdere selectie.
- Nationale én Europese uitvragen (AAO) worden meegenomen. Gunningen (AGO)
  worden met controleerbare publicatie- en procedure-identifiers gekoppeld. `lopend` betekent dat nog geen gunning
  is gekoppeld; alleen de sluitingsdatum bepaalt mede of inschrijving nog open is.
- Onderhandse opdrachten en nadere uitvragen onder raamovereenkomsten zijn niet
  volledig zichtbaar. Documenten van externe aanbestedingsplatforms zijn niet
  altijd via TenderNed beschikbaar. Het dashboard meet geen volledige markt.
- `publicatiedatum` is de uitvraagdatum, `gunningsdatum` de publicatiedatum van
  de gunning, en `datum_gunning` de gunningsdatum uit de bulk-Excel. Deze datums
  zijn niet onderling uitwisselbaar. Historische brongegevens kunnen ontbreken.
- Winnaars en definitieve waarden komen uit de periodieke TenderNed-bulk-Excel.
  De koppeling gebruikt publicatie-id's, eForms-procedure-UUID's, expliciete
  bronverwijzingen, TenderNed-kenmerken en de combinatie van opdrachtnummer met
  extern project-id. Een gelijkluidende titel is nooit voldoende bewijs.
  Tegenstrijdige of meervoudige matches gaan naar de controlelijst.
  Ook winnaars van eerder gekoppelde opdrachten worden opnieuw gecontroleerd.
- De concurrentiekaart telt iedere bekende (mede)winnaar eenmaal per opdracht.
  `winnaars` bevat de namen; `winnende_organisaties` bewaart KvK, bronnamen,
  percelen en gunningspublicaties. `winnaar` en `extra_winnaars` blijven als
  compatibiliteitsvelden aanwezig. Met het winnaarfilter en **Toon alle bureaus**
  zijn ook bureaus buiten de top twintig bereikbaar.
  Dit is geen verdeling van omzet, percelen of volledige marktaandelen.
- Claude via Azure AI Foundry schat onder andere thema, methodiek, teams, skills,
  evaluatiefase, databronnen en opdrachtgrootte. De betrouwbaarheidsscores zijn
  modelinschattingen, geen gekalibreerde kansen. Records met een gemiddelde score
  onder 0,6 staan in `data/twijfelgevallen.json`.
- De classificatie selecteert maximaal 16.000 tekens aan relevante passages uit
  de opgeslagen documentdelen, ook voorbij voorbladen en technische bijlagen.
  De eerste chatsamenvatting gebruikt maximaal 25.000 tekens. De context blijft
  een selectie; controleer belangrijke conclusies in het originele document.

Het lopende jaar is onvolledig. Een recente broncontrole garandeert niet dat de
bulk-Excel en alle documenten even recent of volledig zijn.

## Bijwerken

```powershell
# Broncontrole zonder opslaan of AI-aanroepen
python update.py --dry-run

# Nieuwe uitvragen classificeren en bestaande gunningen bijwerken
python update.py

# Winnaars aanvullen uit de nieuwste beschikbare TenderNed-bulk
python vul_winnaars_aan.py

# Hetzelfde onderzoek zonder de dashboarddataset te wijzigen
python vul_winnaars_aan.py --dry-run

# Of de bewaarde historische bron-Excel gebruiken
python vul_winnaars_aan.py --excel "Dataset_Tenderned-2016-01-01 tm 2025-12-31_Leeswijzer.xlsx"

# Rapporten en CSV's opnieuw genereren zonder externe API-aanroepen
python analyse.py
python controleer.py
```

Voor nieuwe classificaties moet `AZURE_FOUNDRY_API_KEY` in de procesomgeving
staan. `AZURE_FOUNDRY_RESOURCE` is optioneel; standaard is dit `nl-ai-fndry`.
Het model is `claude-sonnet-5-5`, zoals ingesteld in `classify.py`. De sleutel
wordt niet automatisch uit een tekstbestand gelezen. Analyse en broncontrole
hebben geen AI-sleutel nodig.

`update.py` haalt uitvragen op vanaf de laatste geslaagde update. Zonder
updatestatus begint het vanaf 2019-01-01. Gunningen worden minimaal 30 dagen
terug bekeken, en verder terug bij een langere onderbreking. Voor een expliciete
inhaalslag zijn `--vanaf JJJJ-MM-DD` en `--gunningen-vanaf JJJJ-MM-DD` beschikbaar.
Een mislukte bronaanroep of onvolledige AI-classificatie breekt de run af; de
updatestatus schuift dan niet op. Dataset en status worden elk atomair vervangen.

De update vernieuwt ook de rapporten, CSV-export en twijfelgevallen.
`vul_winnaars_aan.py` en `opschonen.py` doen dat na hun wijzigingen eveneens.
`analyse.py` genereert alles opnieuw uit de huidige dataset en overschrijft
het marktrapport; bewaar eigen interpretaties in een afzonderlijk bestand.

`data/koppelrapport.json` bevat de resultaten van de laatste bulkcontrole,
de gewijzigde opdrachten en de controlelijst. `gunning_koppelingen` en
`gunning_bronnen` bewaren per opdracht het bewijs en de oorspronkelijke
winnaarsrijen. Publicatiemetadata wordt tijdelijk gecachet (maximaal 30 dagen,
één dag bij een 404). Bewaarde historische gunningsbronnen blijven beschikbaar
als de nieuwste bulk niet meer de hele historische periode dekt.
Voor een historische inhaalslag accepteert `--excel` meerdere bestanden,
in volgorde van oud naar nieuw. Een actuele bron vervangt dezelfde publicatie
uit een ouder bestand. Voornemens en alternatieve winnaars worden niet als
bevestigde winst geteld.

## Automatische update en publiceren

`.github/workflows/update.yml` is de enige updateworkflow: dagelijks om 06:00 UTC
en handmatig via GitHub Actions. Winnaars worden op maandag en bij een handmatige
run aangevuld. Tests en een consistentiecontrole gaan vooraf aan publicatie van
de gewijzigde data. Runs worden achter elkaar uitgevoerd om botsende writes
te voorkomen. De workflow gebruikt de repository-secrets `GH_PAT` en
`AZURE_FOUNDRY_API_KEY`.

Repository: https://github.com/clbuvelot/marktonderzoek-dashboard

GitHub Pages publiceert de hoofdmap van de branch `main` op
https://clbuvelot.github.io/marktonderzoek-dashboard/. De startpagina opent het
dashboard; `.nojekyll` zorgt dat de statische bestanden rechtstreeks worden gepubliceerd.
Een push naar `main` start de Pages-publicatie. Controleer na afloop de deployment
en de online login, dataset en filters.

Gebruik voor publicatie een actuele Git-checkout. Neem vanuit een losse werkmap
ook bedoelde verwijderingen over en synchroniseer vlak voor de push opnieuw met
de dagelijkse data-updates. Sleutelbestanden, caches, lokale onderzoeksprofielen
en uitvoerbare bestanden horen niet in de publicatie. De oude eenmalige
uploadscripts zijn verwijderd.

## Bestanden

| Bestand | Functie |
| --- | --- |
| `dashboard.html` | Statische ingang, navigatie en toegankelijke login-/chatdialogen |
| `dashboard.js` / `dashboard.css` | Twee werkwijzen, lijsten, grafieken, detailweergave en browserinstellingen |
| `dashboard-core.js` | Toetsbare status-, zoek-, filter- en groeperingslogica |
| `dashboard-chat.js` / `dashboard-chat.css` | Bestaande login en documentvragen; versleutelde sleutel |
| `update.py` | Dagelijkse incrementele update; kan ook een dataset opbouwen |
| `collect.py` | TenderNed ophalen, normaliseren en documenten uitlezen |
| `classify.py` | AI-classificatie en platte CSV-export |
| `vul_winnaars_aan.py` | Gunningskoppelingen en alle bevestigde winnaars controleren |
| `publicatie_koppeling.py` | Officiële identifiers ophalen, matches en conflicten toetsen |
| `tenderned_bulk.py` | Gunningspublicaties, winnaars en percelen uit bulkbestanden lezen |
| `herstel_gunningen.py` | De hele dataset vergelijken met bronpublicaties; wijzigingen en review vastleggen |
| `winnaars.py` | Bedrijfsnamen en KvK koppelen; mede-winnaars eenmaal tellen |
| `opschonen.py` | Naamnormalisatie, tekstopschoning en correcties in classificaties |
| `analyse.py` | Alle afgeleide analyses, marktrapport en exports |
| `opslag.py` | Atomair JSON opslaan |
| `controleer.py` | Consistentie van dataset, documenten, updatestatus en exports |
| `versleutel_key.py` | De bestaande wachtwoordconstructie voor documentchat instellen |
| `tests/test_pipeline.py` | Regressietests voor updates en gegevensbehoud |
| `data/documenten/` | Opgeslagen tekst van documenten bij de actuele opdrachten |

`collect.py` en `classify.py` kunnen apart worden gebruikt voor onderzoek aan
ruwe selecties. Hun tussenbestanden zijn geen alternatieve dashboarddataset.
De oude, ongebruikte Kamerstukken-route is verwijderd.

De historische bron-Excel is lokaal behouden en staat niet in versiebeheer.
Downloadcaches, tijdelijke bestanden en lokale sleutels zijn uitgesloten via
`.gitignore`. De dashboardlogin ontsleutelt de AI-sleutel in de browser; het is
geen serverbeveiliging voor de statisch gepubliceerde dataset. `api.txt` en
`token.txt` zijn lokale bestanden en worden door de pipeline niet gebruikt.

## Controle

```powershell
python -m unittest discover -s tests -v
node --test tests/dashboard_core.test.cjs
python controleer.py
```

De tests voeren geen betaalde AI-aanroepen uit. De actuele datasetcontrole
controleert onder andere unieke ids, publicatiedatums, documentstatus en
overeenstemming tussen JSON, CSV, trendanalyse en updatetelling.

De JavaScript-tests vereisen Node 24 en geen npm-installatie. De browsertest
`python tests/browser_smoke.py` vereist daarnaast Playwright en Edge. Die test
controleert beide gebruikersroutes op desktop en mobiel, simuleert alle
AI-verzoeken en schrijft screenshots onder `data/.cache/ux/`.

## Openstaande uitbreiding

Het [brononderzoek naar Ecorys als winnaar](docs/onderzoek_ecorys/rapport.md)
beschrijft de situatie vóór het herstel. De algemene koppellogica en telling
van mede-winnaars zijn inmiddels aangepast; de resultaten voor alle bureaus
staan in [het herstelverslag](docs/herstel_koppelingen.md).

Ondersteuning van nadere uitvragen binnen raamovereenkomsten is nog een
inhoudelijke en technische uitbreiding. Documentchat, TenderNed-filters,
groepering van grafieken en de wachtwoorddialoog uit de oude takenlijst zijn
al aanwezig.
