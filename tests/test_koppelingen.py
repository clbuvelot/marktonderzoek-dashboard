import copy
import unittest

from analyse import analyse_concurrenten
from herstel_gunningen import herstel
from publicatie_koppeling import maak_index, zoek_koppeling, procedure_uuid, project_sleutel
from winnaars import bedrijfsregister, organisaties, onzeker
from update import werk_gunningen_bij

UUID = "290115c6-f318-4bf4-b23e-f3b6cc461a3c"


class KoppelTests(unittest.TestCase):
    def setUp(self):
        self.record = {"id": "322538", "tenderned_kenmerk": "447347", "publicatiedatum": "2024-01-13",
                       "titel": "Uitvoeren lastendrukmetingen", "classificatie": {"thema": "Wonen"},
                       "koppeling_metadata": {"procedure_uuid": UUID, "referentie_nummer": "201865002.001.095",
                                              "project_sleutel": "eu-supply:pid:391171"}}
        self.gunning = {"id": "361925", "kenmerk": "503410", "datum": "2024-12-20",
                        "titel": self.record["titel"], "metadata": copy.deepcopy(self.record["koppeling_metadata"]),
                        "winnaar_rijen": [{"naam": "ECORYS Nederland B.V.", "kvk": "24316726"}]}

    def match(self, gunning=None, records=None):
        records = records if records is not None else [self.record]
        return zoek_koppeling(gunning or self.gunning, records, maak_index(records))

    def test_andere_kenmerken_koppelen_met_procedure_uuid(self):
        i, bewijs = self.match()
        self.assertEqual(i, 0)
        self.assertIn("procedure_uuid", bewijs["methoden"])

    def test_dagelijkse_update_gebruikt_ook_procedure_uuid(self):
        records = [copy.deepcopy(self.record)]
        ao = {"503410": {"publicatieId": "361925", "publicatieDatum": "2024-12-20",
                          "metadata": self.gunning["metadata"]}}
        self.assertEqual(werk_gunningen_bij(records, ao), 1)
        self.assertEqual(records[0]["gunning_publicatie_id"], "361925")
        self.assertEqual(records[0]["id"], "322538")

    def test_titel_alleen_is_nooit_bewijs(self):
        self.gunning["metadata"] = {}
        i, bewijs = self.match()
        self.assertIsNone(i)
        self.assertEqual(bewijs["reden"], "geen_identiteitsmatch")

    def test_ref_en_project_samen_zijn_alternatieve_koppeling(self):
        self.gunning["metadata"].pop("procedure_uuid")
        self.assertEqual(self.match()[0], 0)
        self.gunning["metadata"].pop("project_sleutel")
        self.assertIsNone(self.match()[0])

    def test_conflict_uuid_blokkeert_ook_gelijk_kenmerk(self):
        self.gunning["kenmerk"] = self.record["tenderned_kenmerk"]
        self.gunning["metadata"]["procedure_uuid"] = "een-andere-procedure"
        self.assertEqual(self.match()[1]["reden"], "tegenstrijdige_procedure_uuid")

    def test_meerdere_records_met_zelfde_identiteit_vereisen_review(self):
        tweede = copy.deepcopy(self.record)
        tweede["id"] = "999"
        self.assertEqual(self.match(records=[self.record, tweede])[1]["reden"], "meerdere_records")

    def test_oude_gunning_niet_aan_nieuwe_uitvraag_hangen(self):
        self.gunning["datum"] = "2023-01-01"
        self.assertEqual(self.match()[1]["reden"], "gunning_voor_recorddatum")

    def test_expliciete_bronverwijzing_is_bruikbaar(self):
        self.gunning["metadata"] = {}
        self.gunning["publicatie_ids"] = ["322538", "361925"]
        self.assertEqual(self.match()[0], 0)

    def test_herstel_behoudt_opdracht_en_is_herhaalbaar(self):
        records = [copy.deepcopy(self.record)]
        origineel = copy.deepcopy(records[0])
        herstel(records, [self.gunning], metadata_ophalen=False)
        self.assertEqual(records[0]["winnaars"], ["Ecorys Nederland B.V."])
        self.assertEqual(records[0]["publicatie_type"], "gegund")
        for veld in ("id", "tenderned_kenmerk", "publicatiedatum", "classificatie"):
            self.assertEqual(records[0][veld], origineel[veld])
        eenmaal = copy.deepcopy(records)
        herstel(records, [self.gunning], metadata_ophalen=False)
        self.assertEqual(records, eenmaal)

    def test_bulk_behoudt_recentere_api_gunning(self):
        records = [copy.deepcopy(self.record)]
        werk_gunningen_bij(records, {"600001": {"publicatieId": "400001", "publicatieDatum": "2025-06-01",
                                               "metadata": self.gunning["metadata"]}})
        herstel(records, [self.gunning], metadata_ophalen=False)
        self.assertEqual(set(records[0]["gunning_publicatie_ids"]), {"361925", "400001"})
        self.assertEqual(records[0]["gunning_publicatie_id"], "400001")
        self.assertEqual(records[0]["gunningsdatum"], "2025-06-01")
        self.assertIn("600001", records[0]["gekoppelde_kenmerken"])

    def test_voornemen_en_alternatieve_winnaar_tellen_niet(self):
        for soort, naam in [("Vrijwillige transparantie vooraf", "Ecorys"), ("", "1) Ecorys of 2) NIVEL")]:
            with self.subTest(soort=soort):
                pub = {**self.gunning, "soort": soort, "winnaar_rijen": [{"naam": naam}]}
                records = [copy.deepcopy(self.record)]
                result = herstel(records, [pub], metadata_ophalen=False)
                self.assertEqual(records[0]["winnaars"], [])
                self.assertEqual(result["controlelijst"][0]["reden"], "geen_bevestigde_winnaar")
        self.assertFalse(onzeker("University of London"))

    def test_kvk_voegt_varianten_en_percelen_samen(self):
        p = {"id": "1", "winnaar_rijen": [{"naam": "Bureau B.V.", "kvk": "12348765", "perceel": "1"},
                                             {"naam": "Bureau BV", "kvk": "12348765", "perceel": "2"}]}
        orgs = organisaties([p], bedrijfsregister([p]))
        self.assertEqual(len(orgs), 1)
        self.assertEqual(orgs[0]["percelen"], ["1", "2"])
        kaart = analyse_concurrenten([{"winnaars": ["Bureau B.V.", "Partner"], "gunningswaarde": 100,
                                       "classificatie": {"methodiek_kwantitatief": True}}])
        self.assertEqual([r["aantal_gewonnen"] for r in kaart], [1, 1])
        self.assertEqual(sum(r["bekende_waarde_als_enige_winnaar_euro"] for r in kaart), 0)

    def test_kvk_met_tegenstrijdige_bedrijfsnamen_voegt_niet_samen(self):
        p = {"id": "1", "winnaar_rijen": [{"naam": "ResearchNed", "kvk": "09159322"},
                                             {"naam": "Maastricht University", "kvk": "09159322"}]}
        register = bedrijfsregister([p])
        self.assertIn("09159322", register[2])
        self.assertEqual(len(organisaties([p], register)), 2)

    def test_uuid_en_project_uit_officiele_velden(self):
        self.assertEqual(procedure_uuid("Identiﬁcatiecode van de\nprocedure: " + UUID), UUID)
        self.assertEqual(procedure_uuid("Procedure\nidentifier: " + UUID), UUID)
        self.assertEqual(procedure_uuid("Notice identifier: " + UUID), "")
        self.assertEqual(project_sleutel("https://eu.eu-supply.com/app?PID=391171&B="), "eu-supply:pid:391171")
        self.assertEqual(project_sleutel("https://example.com/?PID=391171"), "")


if __name__ == "__main__":
    unittest.main()
