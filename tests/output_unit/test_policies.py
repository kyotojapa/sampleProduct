import unittest

import product_common_logging as logging

from data_processing.models.unified_model import UnifiedModel
from output_unit.adapters import (
    FluentdAdapter, LogsFileAdapter, MetricsFileAdapter,
    MQTTLogsAdapter, MQTTMetricsAdapter, OTELMetricsAdapter)
from output_unit.policies import (
    PolicyLogsDebug, PolicyLogsDefault, PolicyMetricsDebug,
    PolicyMetricsDefault, PolicyShoreLogsDefault, PolicyShoreMetricsOTEL,
    SingletonMeta)
from output_unit.utils import BufferNames


class TestPolicies(unittest.TestCase):

    def setUp(self) -> None:
        self._policy_logs_debug = PolicyLogsDebug()
        self._policy_metrics_debug = PolicyMetricsDebug()
        self._policy_logs_default = PolicyLogsDefault()
        self._policy_metrics_default = PolicyMetricsDefault()
        self._policy_shore_logs_default = PolicyShoreLogsDefault()
        self._policy_shore_metrics_otel = PolicyShoreMetricsOTEL()

    def tearDown(self) -> None:
        del self._policy_logs_debug
        del self._policy_metrics_debug
        del self._policy_logs_default
        del self._policy_metrics_default
        del self._policy_shore_logs_default
        del self._policy_shore_metrics_otel

    def test_singleton_meta(self):
        class TestSingleton(metaclass=SingletonMeta):
            pass

        instance1 = TestSingleton()
        instance2 = TestSingleton()
        self.assertIs(instance1, instance2)

    def test_policy_logs_debug_should_process_successful(self):
        self.assertTrue(self._policy_logs_debug.should_process(
                        BufferNames.LOGS_BUFFER))

    def test_policy_logs_debug_should_process_failure(self):
        self.assertFalse(self._policy_logs_debug.should_process(
                         BufferNames.METRICS_BUFFER))

    def test_policy_logs_debug_condition_successful(self):
        logging.setLevel(logging.DEBUG)
        self.assertTrue(self._policy_logs_debug.condition())
        logging.setLevel(logging.CRITICAL)

    def test_policy_logs_debug_condition_failure(self):
        self.assertFalse(self._policy_logs_debug.condition())

    def test_policy_logs_debug_chosen_adapter(self):
        self.assertEqual(self._policy_logs_debug.chosen_adapter(),
                         LogsFileAdapter.__name__)

    def test_policy_metrics_debug_should_process_successful(self):
        self.assertTrue(self._policy_metrics_debug.should_process(
                        BufferNames.METRICS_BUFFER))

    def test_policy_metrics_debug_should_process_failure(self):
        self.assertFalse(self._policy_metrics_debug.should_process(
                         BufferNames.LOGS_BUFFER))

    def test_policy_metrics_debug_condition_successful(self):
        logging.setLevel(logging.DEBUG)
        self.assertTrue(self._policy_metrics_debug.condition())
        logging.setLevel(logging.CRITICAL)

    def test_policy_metrics_debug_condition_failure(self):
        self.assertFalse(self._policy_metrics_debug.condition())

    def test_policy_metrics_debug_chosen_adapter(self):
        self.assertEqual(self._policy_metrics_debug.chosen_adapter(),
                         MetricsFileAdapter.__name__)

    def test_policy_logs_default_should_process_successful(self):
        self.assertTrue(self._policy_logs_default.should_process(
                        BufferNames.LOGS_BUFFER))

    def test_policy_logs_default_should_process_failure(self):
        self.assertFalse(self._policy_logs_default.should_process(
                         BufferNames.METRICS_BUFFER))

    def test_policy_logs_default_condition(self):
        self.assertTrue(self._policy_logs_default.condition())

    def test_policy_logs_default_chosen_adapter(self):
        self.assertTrue(self._policy_logs_default.chosen_adapter(),
                        MQTTLogsAdapter.__name__)

    def test_policy_metrics_default_should_process_successful(self):
        self.assertTrue(self._policy_metrics_default.should_process(
                        BufferNames.METRICS_BUFFER))

    def test_policy_metrics_default_should_process_failure(self):
        self.assertFalse(self._policy_metrics_default.should_process(
                         BufferNames.LOGS_BUFFER))

    def test_policy_metrics_default_condition(self):
        self.assertFalse(self._policy_metrics_default.condition())

    def test_policy_metrics_data_specific_condition(self):
        data = {"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/LoggingObject.cpp","Li":862},"Ct":"lw.comp.IR.snapshot","Ti":"20240217T083551.181535","Cl":{"ownr":"<IrMgr!5851>","oid":"<IrIf!5886>","info":"bfCSC=0,AddrDoms=private,public,sock=<PubDgrmComms!5897>,Svr=1","sinf":"0203-Active","goid":"0efbd20e-9853-4582-ba95-b84cfcca8e42"},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":3302,"ctx":"<IrMgr!323222>","obj":"<IrIf!5886>","val":{"Sta":{"g":{"IfId":"Ethernet_eth2_4031","S":{"cSt":203,"bfCSC":0,"svcCost":"1"}},"tx":{"qued":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"tuB":6710887,"Bu":{"B":10066330}}},"Sts":{"tx":{"Bu":{"tTu":10210652,"nTu":15219,"nooB":0,"tTkn":144322,"tSto":0},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"t":{"nP":"100000","nB":"20000"}},"rx":{"r":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"}}}}}}
        data = UnifiedModel.parse_obj(data)
        self.assertTrue(self._policy_metrics_default.condition(data))

    def test_policy_metrics_default_chosen_adapter(self):
        self.assertTrue(self._policy_metrics_default.chosen_adapter(),
                        MQTTMetricsAdapter.__name__)

    def test_policy_shore_logs_default_should_process_successful(self):
        self.assertTrue(self._policy_shore_logs_default.should_process(
                        BufferNames.LOGS_BUFFER))

    def test_policy_shore_logs_default_should_process_failure(self):
        self.assertFalse(self._policy_shore_logs_default.should_process(
                         BufferNames.METRICS_BUFFER))

    def test_policy_shore_logs_default_condition(self):
        self.assertTrue(self._policy_shore_logs_default.condition())

    def test_policy_shore_logs_default_chosen_adapter(self):
        self.assertTrue(self._policy_shore_logs_default.chosen_adapter(),
                        FluentdAdapter.__name__)

    def test_policy_shore_metrics_otel_should_process_successful(self):
        self.assertTrue(self._policy_shore_metrics_otel.should_process(
                        BufferNames.METRICS_BUFFER))

    def test_policy_shore_metrics_otel_should_process_failure(self):
        self.assertFalse(self._policy_shore_metrics_otel.should_process(
                         BufferNames.LOGS_BUFFER))

    def test_policy_shore_metrics_otel_condition(self):
        self.assertTrue(self._policy_shore_metrics_otel.condition())

    def test_policy_shore_metrics_otel_chosen_adapter(self):
        self.assertTrue(self._policy_shore_metrics_otel.chosen_adapter(),
                        OTELMetricsAdapter.__name__)
