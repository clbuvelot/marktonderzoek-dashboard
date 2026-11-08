# Onderhoud op 8 november 2026

De lokale dataset is gesynchroniseerd met de geslaagde GitHub-publicatie
`ed9ec04a4755148aec20ed57adeb8aa319e16f0f`. Er zijn nu 929 opdrachten,
440 opdrachten met een bekende eerste winnaar en 725 opdrachten met
documentteksten. De broncontrole in deze publicatie is van 8 november 2026,
12:42:43 UTC. Een aanvullende rechtstreekse TenderNed-controle vond geen
nieuwe uitvragen of gunningen.

Ten opzichte van de oude lokale dataset zijn tien opdrachten toegevoegd,
61 winnaars aangevuld en 50 bestaande gunningen bijgewerkt. Er zijn 61
ontbrekende documentteksten opgehaald. Eén bureaunaam is genormaliseerd.
Rapporten, CSV's, twijfelgevallen en updatetelling zijn opnieuw op elkaar afgestemd.

De dashboardkop toont nu de werkelijke periode en broncontrole. Scope en
gunningsdatums zijn verduidelijkt. De README vervangt de verouderde
Word-documentatie en takenlijst. De pipeline stopt bij mislukte bronaanroepen
of onvolledige classificaties, houdt rekening met langere onderbrekingen,
schrijft JSON atomair en vernieuwt afgeleide bestanden na wijzigingen.

Er zijn 24 legacybestanden, de map `oud`, Python-caches en 745 documentteksten
zonder verwijzing vanuit een actuele opdracht verwijderd. De exacte lijst en
de locatie van de herstelkopie staan in [opruiming.json](opruiming.json).
Die herstelkopie bevat de oorspronkelijke scripts, data en documentatie.
De bron-Excel, lokale sleutelbestanden en `Project Assistent.exe` zijn behouden;
deze zijn niet opgenomen in de herstelkopie. Het losse hulpprogramma wordt
niet aangeroepen door de dashboardcode; de verdere functie is niet vastgesteld.

Validatie: negen regressietests geslaagd; consistentiecontrole van dataset,
documenten, status en exports geslaagd; Python-syntax en imports gecontroleerd.
In Edge zijn loginweergave, dataladen, jaarfilter, reset, grafiekgroepering,
opdrachtdetails en documentchat gecontroleerd zonder JavaScript-fouten.
De documentchat gebruikte daarbij een gesimuleerde AI-reactie; er is geen
betaalde AI-aanroep uitgevoerd.

Alle wijzigingen zijn lokaal aangebracht. De nieuwe code, workflow en
verwijderingen zijn nog niet naar GitHub gepubliceerd.
