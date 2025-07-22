import json
import os
import unittest
from pathlib import Path

from pydantic import ValidationError

from data_processing.models.unified_model import UnifiedModel


def read_data(data_folder):
    return [
        json.load(open(f"{data_folder}/{file}"))
        for file in os.listdir(data_folder)
        if os.path.isfile(f"{data_folder}/{file}")
    ]


class TestUnifiedModel(unittest.TestCase):
    def setUp(self) -> None:
        self.data_parent_dir = Path(__file__).parent

    def test_unified_model_phase3(self):
        "Test all the allowed phase 3 messages in the Unified Model"
        p3_dir = self.data_parent_dir.joinpath("data").joinpath("p3").absolute()
        p3_metrics = read_data(p3_dir)

        try:
            for msg in p3_metrics:
                UnifiedModel.parse_obj(msg)
        except ValidationError:
            self.fail("should not raise error")

    def test_unified_model_phase4(self):
        "Test all the allowed phase 4 messages in the Unified Model"
        p4_dir = self.data_parent_dir.joinpath("data").joinpath("p4").absolute()
        self.p4 = read_data(p4_dir)

        try:
            for msg in self.p4:
                UnifiedModel.parse_obj(msg)
        except ValidationError:
            self.fail("should not raise error")

        "Test discarding forbidden IscSchedGrp in the Unified Model"
        p4_dir_forbidden = self.data_parent_dir.joinpath("data").joinpath("p4").joinpath("forbidden").absolute()
        self.p4_forbidden = read_data(p4_dir_forbidden)

        try:
            for msg in self.p4_forbidden:
                with self.assertRaises(ValidationError):
                    UnifiedModel.parse_obj(msg)
        except AssertionError:
            self.fail(f"should not raise error #{msg['snapshot']['obj']}")

    def test_p3_sla_mtr_unified_model(self):
        """
        Test that phase 3 SlaMtr messages are transformed to phase 4 format.
        """
        sla_mtr_file_path = self.data_parent_dir.joinpath("data").joinpath("p3").joinpath("SlaMtr.json").absolute()
        sla_mtr_message = json.load(open(sla_mtr_file_path))

        expected_model = {
            "Hd": {
                "Ct": "lw.comp.ISC.context.snapshot",
                "Ti": "20240613T072920.527407",
                "Cl": {"ownr": "<IscCtx!304115>", "oid": "<SlaMtr!305164>[fvkau3-sla]", "info": "", "sinf": "",
                       "goid": ""}
            },
            "field_0": "",
            "snapshot": {
                "sid": 17493,
                "ctx": {"ctxId": "5808c266-26b8-4699-8c4d-11fc2e995721", "oid": "<IscCtx!304115>"},
                "obj": "<SlaMtr!305164>[fvkau3-sla]",
                "val": {
                    "Sta": {"g": {"id": "fvkau3-sla"}},
                    "Sts": {"tx": {"tot": {"t": 750542, "u": 3517, "ur": 3631, "uro": 5254}}}
                }
            }
        }

        self.assertEqual(UnifiedModel.parse_obj(sla_mtr_message).dict(), expected_model)
