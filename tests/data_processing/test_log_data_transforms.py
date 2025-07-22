import unittest
from unittest.mock import MagicMock, patch
from data_processing import log_data_transforms
from output_unit.metric_storage import MetricStore
from data_processing.exit_transform_loop_exception import ExitTransformLoopException


class TestLogDataProcessing(unittest.TestCase):
    def setUp(self) -> None:
        self.k1 = None
        self.v1 = None
        self.message = ({
            "Hd": {
                "Ty": "Log",
                "P": {
                    "Fi": "lw/shared/cf/src/comps/isc/ISC_CtxRemoteInterface.cpp",
                    "Li": 198
                },
                "Ct": "lw.comp.ISC.context.interface",
                "Ti": "20240606T073816.688372",
                "Cl": {
                    "ownr": "<IscCtx!398489>",
                    "oid": "<IscCtxRemIf!398500>",
                    "info": "",
                    "sinf": "",
                    "goid": ""
                },
                "Fu": "UpdateFromMsgFromRemote"
            },
            "txt": "remIf=\":{\"Sta\":{\"g\":{\"S\":{\"cSt\":203,\"bfCSC\":0,\"svcCost\":\"0\"},\"Ept\":\"{\\\"Candidates\\\":[{\\\"Dom\\\":\\\"private\\\",\\\"Ep\\\":\\\"10.251.191.6:4000\\\",\\\"EpFmt\\\":\\\"IP\\\"}]}\",\"brrs\":{\"nBrrs\":3}},\"tx\":{\"e\":1,\"tuBu\":6710887,\"Bu\":{\"B\":2683196}}},\"Sts\":{\"tx\":{\"nETrs\":0,\"nBuTrs\":1,\"Bu\":{\"tTu\":2684355,\"nTu\":3,\"nooB\":0,\"tTkn\":1159,\"tSto\":0}}}}, msg=Ctx.Connected\r\nbrrCtxID=3\r\nbrrDIrID=2\r\nbrrSIrID=7\r\nchanID=1\r\niscE2EV=\"2.4.0 LER\"\r\nremIfUID=Ethernet_eth2_3917\r\nx-sarq-nr=1\r\nx-sarq-vers=1.0.0\r\nEND\r\n\n"
        })

    def test_process_log_messages(self):

        result = log_data_transforms.process_log_messages(message=self.message)
        self.assertEqual(self.message, result)

    def test_process_log_messages_values_are_extracted(self):
        def mock_update_attribute_map(name, map_name, key, value):
            self.k1 = key
            self.v1 = value

        with patch.object(MetricStore, "update_attribute_map") as mock_ms:
            mock_ms.side_effect = mock_update_attribute_map

            result = log_data_transforms.process_log_messages(message=self.message)
        self.assertEqual(self.message, result)
        self.assertIn(self.k1, self.message["Hd"]["Cl"]["oid"])
        self.assertIn(self.v1, self.message["txt"])

    def test_process_log_messages_wrong_data_type1(self):
        self.message["Hd"]["Cl"]["oid"] = None
        def mock_update_attribute_map(name, map_name, key, value):
            self.k1 = key
            self.v1 = value

        with patch.object(MetricStore, "update_attribute_map") as mock_ms:
            mock_ms.side_effect = mock_update_attribute_map

        with self.assertRaises(ExitTransformLoopException):
            log_data_transforms.process_log_messages(message=self.message)


    def test_process_log_messages_wrong_log(self):
        self.message["Hd"]["Ct"] = None
        def mock_update_attribute_map(name, map_name, key, value):
            self.k1 = key
            self.v1 = value

        with patch.object(MetricStore, "update_attribute_map") as mock_ms:
            mock_ms.side_effect = mock_update_attribute_map

            result = log_data_transforms.process_log_messages(message=self.message)
        self.assertEqual(self.message, result)
        self.assertEqual(self.k1, None)
        self.assertEqual(self.v1, None)

    def test_process_log_messages_missing_key1(self):
        self.message["Hd"].pop("Fu")
        def mock_update_attribute_map(name, map_name, key, value):
            self.k1 = key
            self.v1 = value

        with patch.object(MetricStore, "update_attribute_map") as mock_ms:
            mock_ms.side_effect = mock_update_attribute_map
        with self.assertRaises(ExitTransformLoopException):
            log_data_transforms.process_log_messages(message=self.message)
