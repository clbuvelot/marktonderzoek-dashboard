import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests
import analyse
import classify
import collect
import opslag
import update
import vul_winnaars_aan
from herstel_gunningen import herstel


class PipelineTests(unittest.TestCase):
    def test_bronfout_mag_geen_lege_succesvolle_update_worden(self):
        with patch.object(collect.requests, "get", side_effect=requests.Timeout):
            with self.assertRaises(RuntimeError):
                update._haal_publicaties("2026-01-01", "AAO", "test")

    def test_mislukte_update_schuift_watermerk_niet_op(self):
        with (patch("sys.argv", ["update.py"]),
              patch.object(update, "laad_staat", return_value={"laatste_update": "2026-04-23T12:00:00Z"}),
              patch.object(update, "laad_dataset", return_value=[]),
              patch.object(update, "fetch_nieuwe_uitvragen", side_effect=RuntimeError("bron offline")),
              patch.object(update, "sla_staat_op") as sla_staat,
              patch.object(update, "sla_dataset_op") as sla_dataset):
            with self.assertRaises(RuntimeError):
                update.main()
            sla_staat.assert_not_called()
            sla_dataset.assert_not_called()

    def test_lange_onderbreking_haalt_ook_oude_gunningen_in(self):
        with (patch("sys.argv", ["update.py", "--dry-run"]),
              patch.object(update, "laad_staat", return_value={"laatste_update": "2020-04-23T12:00:00Z"}),
              patch.object(update, "laad_dataset", return_value=[]),
              patch.object(update, "fetch_nieuwe_uitvragen", return_value={}),
              patch.object(update, "fetch_gunningen", return_value={}) as ophalen):
            update.main()
            ophalen.assert_called_once_with("2020-04-23")

    def test_gunning_behoudt_document_id_en_uitvraag(self):
        records = [{"id": "10", "tenderned_kenmerk": "123", "publicatiedatum": "2026-01-01",
                    "publicatie_type": "lopend", "classificatie": {"thema": "Zorg"}}]
        origineel = copy.deepcopy(records[0])
        gunningen = {"123": {"publicatieId": 20, "publicatieDatum": "2026-06-01"}}
        self.assertEqual(update.werk_gunningen_bij(records, gunningen), 1)
        for veld in ("id", "publicatiedatum", "classificatie"):
            self.assertEqual(records[0][veld], origineel[veld])
        self.assertEqual(records[0]["gunning_publicatie_id"], "20")
        self.assertEqual(update.werk_gunningen_bij(records, gunningen), 0)

    def test_ai_indices_moeten_bijectief_zijn(self):
        basis = {"teams": [], "teams_status": "onduidelijk", "teams_matches": [],
                 "teams_toelichting": "De opdracht bevat te weinig inhoud.",
                 "thema": "Zorg", "methodiek": "MKBA", "methodiek_kwantitatief": True}
        for indices in ([0, 0], [-1, 1], [0], [0, 2], [False, 1]):
            with self.subTest(indices=indices), self.assertRaises(ValueError):
                classify.valideer_classificaties([{**basis, "index": i} for i in indices], 2)
        geldig = [{**basis, "index": i} for i in (1, 0)]
        self.assertEqual(classify.valideer_classificaties(geldig, 2), geldig)

    def test_ongeldige_ai_json_maakt_geen_nepclassificaties(self):
        client = SimpleNamespace(messages=SimpleNamespace(
            create=lambda **kwargs: SimpleNamespace(content=[SimpleNamespace(type="text", text="geen JSON")])
        ))
        with patch.object(classify, "bouw_classificatieprompt", return_value="test"):
            with self.assertRaises(RuntimeError):
                classify.classificeer_batch(client, [{"id": "10"}], 1)

    def test_winnaar_koppelen_vernieuwt_ook_bekende_winnaars(self):
        records = [{"id": "10", "tenderned_kenmerk": "123", "publicatie_type": "lopend"},
                   {"id": "11", "tenderned_kenmerk": "456", "winnaar": "Bestaand"}]
        pubs = [{"id": "100", "kenmerk": "123", "datum": "2026-05-01", "winnaar_rijen": [{"naam": "Nieuw"}, {"naam": "Partner"}]},
                {"id": "101", "kenmerk": "456", "datum": "2026-05-01", "winnaar_rijen": [{"naam": "Bestaand"}, {"naam": "Extra bureau"}]}]
        herstel(records, pubs, metadata_ophalen=False)
        self.assertEqual(records[0]["id"], "10")
        self.assertEqual(records[0]["winnaars"], ["Nieuw", "Partner"])
        self.assertEqual(records[1]["winnaar"], "Bestaand")
        self.assertEqual(records[1]["extra_winnaars"], ["Extra bureau"])

    def test_mislukt_opslaan_behoudt_oude_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            pad = Path(tmp) / "data.json"
            opslag.schrijf_json(pad, [{"id": "1"}])
            with self.assertRaises(ValueError):
                opslag.schrijf_json(pad, [{"waarde": float("nan")}])
            self.assertEqual(json.loads(pad.read_text()), [{"id": "1"}])
            self.assertEqual(list(Path(tmp).iterdir()), [pad])

    def test_export_en_rapport_volgen_dezelfde_dataset(self):
        records = [{"id": "10", "publicatiedatum": "2026-01-01", "winnaar": "Bureau",
                    "classificatie": {"methodiek": "Mixed methods", "methodiek_kwantitatief": True,
                                      "betrouwbaarheid": 0.4}}]
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(analyse, "OUTPUT_DIR", tmp), patch.object(classify, "OUTPUT_DIR", tmp):
                analyse.vernieuw_exports(records)
                self.assertEqual(len(json.loads(Path(tmp, "twijfelgevallen.json").read_text())), 1)
                self.assertIn("waarvan 1 als kwantitatief", Path(tmp, "marktrapport.md").read_text(encoding="utf-8"))
                analyse.vernieuw_exports([])
                self.assertEqual(json.loads(Path(tmp, "twijfelgevallen.json").read_text()), [])
                self.assertEqual(Path(tmp, "concurrentiekaart.csv").read_text(), "")


if __name__ == "__main__":
    unittest.main()
