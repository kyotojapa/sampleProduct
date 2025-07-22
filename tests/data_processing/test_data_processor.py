""" Module to run tests on data processing unit"""

import time
import unittest
from json import loads
from queue import Queue
from threading import Event, Thread
from typing import Dict, List, Tuple
from unittest import mock
from unittest.mock import call, patch

from config_manager.models import (
    CorrelationConfig, FileAdapterConfig, FluentdAdapterConfig, GeneralConfig,
    MetricTransformConfig, MQTTAdapterConfig)
from data_processing import log_data_transforms, metric_data_transforms
from data_processing.data_processor import (
    TRANSFORM_FUNCTIONS_MAP, DataProcessor, MessageType,
    get_transform_functions)
from data_processing.models.unified_model import UnifiedModel
from input_unit.input import InputUnit
from input_unit.shared_data import DecoderErrorScheme, PurgeScheme
from output_unit.output_unit import OutputUnit
from output_unit.utils import BufferFullError, BufferNames

# pylint: disable=no-member


class TestDataProcessorUnit(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    @classmethod
    def setUpClass(cls) -> None:
        "Test set up method"

        cls.startup_cfg_dict = {
            "General": GeneralConfig(
                listening_port=5676,
                listening_address="0.0.0.0",
                input_buffer_chunk_size=4096,
                input_buffer_size=32768,
                input_buffer_purge_scheme=PurgeScheme.DISABLED,
                input_buffer_error_scheme=DecoderErrorScheme.REPLACE,
                queue_max_items_metrics=1000,
                queue_max_items_logs=1000,
                cycle_queue_metrics=True,
                cycle_queue_logs=True,
            )
        }

        cls.input_unit = InputUnit(cls.startup_cfg_dict)
        cls.adapters_config = {
            "MQTTAdapter": MQTTAdapterConfig(),
            "FileAdapter": FileAdapterConfig(),
            "FluentAdapter": FluentdAdapterConfig(),
        }
        connection = mock.MagicMock()
        location = mock.MagicMock()
        cls.correlation_config = CorrelationConfig(
            location=location, connection=connection,
            ctx_release_exp_back_off_enabled=False,
            ctx_release_hold_off_period=0,
            ctx_release_start_at=60,
            ctx_release_max_setting=120,
            ctx_release_exp_factor=1.3,
            sub_sampler_enable=False,
            sub_sampling_period=10,
            sub_sampler_cleanup_period=60,
            disabled_queries=[])
        cls.output_unit = OutputUnit(cls.adapters_config, 10, 10)
        metric_transform_config: MetricTransformConfig = MetricTransformConfig()
        cls.data_proc = DataProcessor(
            cls.input_unit, cls.output_unit,
            metric_transform_config,
            cls.correlation_config
        )
        cls.trans_map = TRANSFORM_FUNCTIONS_MAP
        cls.log_data = loads(
            '{"Hd":{"Ty":"Audit","P":'
            '{"Fi":"lw/shared/prod1/src/tenants/TenantManager.cpp",'
            '"Li":3525},"Ct":"RazorLink.Internal.Razor.Tenants.Verbose",'
            '"Ti":"20230117T141244.696419","Cl":{"ownr":"","oid":"",'
            '"info":"","sinf":"","goid":""},"Fu":"OnConntrackTimer"}, '
            '"txt":""}'
        )
        cls.met_data = loads(
            '{"Hd":{"Ty":"Log","P":'
            '{"Fi":"lw/shared/util/src/public/log2/LoggingObject.cpp","Li":823'
            '},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20230612T142144.'
            '962732","Cl":{"ownr":"<IscCtx!2548>","oid":"<MbcpDefCC!3040>",'
            '"info":"Ethernet_eth2_3917<-><IscCtxRemIf!2556>","sinf":"",'
            '"goid":""},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":3495,'
            '"ctx":{"ctxId": "1500aabb-3484-4c8f-baea-7131d7c76138",'
            '"oid": "<IscCtx!2548>"},'
            '"obj":"<MbcpDefCC!3040>","val":{"Sta":{'
            '"e":1,"cW":4380,"pBr":35040000,"Bu":{"B":109500},"ss":1,'
            '"inc":6144,"lgW":4380,"nSP":0,"owB":0,"owP":0,"oBr":21286,'
            '"aBr":21286,"rtt":{"Sta":{"rto":200,"rrto":46,"b":0,"s":0,"v":9}'
            ',"Sts":{"min":1,"max":79,"ns":1929}},"lR":0,"cc":{'
            '"sst":18446744073709551615}},"Sts":{"p":0,"t":0,"f":0,"c":0,'
            '"afE":0,"afP":0,"afB":0,"ifE":0,"ifP":0,"ifB":0,"idE":1787,'
            '"wlE":1929,"amE":0,"asE":2008,"Bu":{"tTu":919184,"nTu":18858,'
            '"nooB":1},"nETrs":3,"nBuTrs":3,"nFCwndTrs":0,"aI":{"sP":2740,'
            '"sB":809684,"fP":0,"fB":0,"tP":0,"tB":0}}}}}'
        )

        cls.valid_msg = loads(
            '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/Loggi'
            'ngObject.cpp","Li":862},"Ct":"lw.comp.IR.snapshot","Ti":"20240217'
            'T083551.181535","Cl":{"ownr":"<IrMgr!5851>","oid":"<IrIf!5886>","'
            'info":"bfCSC=0,AddrDoms=private,public,sock=<PubDgrmComms!5897>,S'
            'vr=1","sinf":"0203 - Active","goid":"0efbd20e-9853-4582-ba95-b84c'
            'fcca8e42"},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":3302,'
            '"ctx":{"ctxId": "1500aabb-3484-4c8f-baea-7131d7c76138",'
            '"oid": "<IrMgr!323222>"},'
            '"obj":"<IrIf!5886>","val":{"Sta":{"g":{"IfI'
            'd":"Ethernet_eth2_4031","S":{"cSt":203,"bfCSC":0,"svcCost'
            '":"1"}},"tx":{"qued":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"tuB":'
            '6710887,"Bu":{"B":10066330}}},"Sts":{"tx":{"Bu":{"tTu":10210652,"'
            'nTu":15219,"nooB":0,"tTkn":144322,"tSto":0},"d":{"nP":"TCV-NotSet'
            '","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"t'
            '":{"nP":"100000","nB":"20000"}},"rx":{"r":{"nP":"TCV-NotSet","nB"'
            ':"TCV-NotSet"},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP'
            '":"TCV-NotSet","nB":"TCV-NotSet"}}}}}}'
        )

        cls.met_data_with_no_metrics = loads(
            '{"Hd":{"Ty":"Log",'
            '"P":{"Fi":"lw/shared/util/src/public/log2/LoggingObject.cpp",'
            '"Li":800},"Ct":"lw.comp.ISC.context.snapshot",'
            '"Ti":"20221201T090914.439486",'
            '"Cl":{"ownr":"<IscCtx!2058>","oid":"<IscBrrGrp!2063>",'
            '"info":"","sinf":"","goid":""},"Fu":"DoSnapshotLog"},'
            '"0":"","snapshot":{"sid":2498,'
            '"ctx":"{\\"ctxId\\": \\"1500aabb-3484-4c8f-baea-7131d7c76138\\",'
            '\\"oid\\": \\"<IscCtx!2058>\\"}","obj":"<IscBrrGrp!2063>",'
            '"val":{"Sta":{"SPO":"1","nBr":0,"Wl":0}}}}'
        )

    @classmethod
    def tearDownClass(cls):
        """Tear Down method"""

        TestDataProcessorUnit.data_proc.stop()
        del TestDataProcessorUnit.input_unit
        del TestDataProcessorUnit.data_proc
        del TestDataProcessorUnit.output_unit

    def test_data_proc_init(self):
        """Method that tests the initialise function of the
        data processor"""

        self.assertIsInstance(TestDataProcessorUnit.data_proc.shutdown_event, Event)
        self.assertIsInstance(TestDataProcessorUnit.data_proc.input_unit, InputUnit)
        self.assertIsInstance(TestDataProcessorUnit.data_proc.output_unit, OutputUnit)

    def test_stop(self):
        """Method that tests the stop function of the data processor"""

        TestDataProcessorUnit.data_proc.stop()
        self.assertTrue(TestDataProcessorUnit.data_proc.shutdown_event.is_set())

    def test_transform_map(self):
        """Method that tests the transforms map"""

        self.assertIsNotNone(TestDataProcessorUnit.trans_map)
        self.assertIsNotNone(TestDataProcessorUnit.trans_map[MessageType.LOG])
        self.assertIsNotNone(TestDataProcessorUnit.trans_map[MessageType.METRIC])
        self.assertIsInstance(TestDataProcessorUnit.trans_map[MessageType.METRIC], List)
        self.assertIsInstance(TestDataProcessorUnit.trans_map[MessageType.LOG], List)
        self.assertGreaterEqual(
            len(TestDataProcessorUnit.trans_map[MessageType.LOG]), 1
        )
        self.assertGreaterEqual(
            len(TestDataProcessorUnit.trans_map[MessageType.METRIC]), 1
        )

    def test_get_transform_functions(self):
        """Method that tests the get transforms function"""

        log_trans_funcs = get_transform_functions(log_data_transforms)
        metric_trans_funcs = get_transform_functions(metric_data_transforms)

        self.assertIsInstance(log_trans_funcs, List)
        self.assertIsInstance(metric_trans_funcs, List)
        self.assertGreaterEqual(len(log_trans_funcs), 1)
        self.assertGreaterEqual(len(metric_trans_funcs), 1)
        for trn in log_trans_funcs:
            self.assertIsInstance(trn, Tuple)
        for trn in metric_trans_funcs:
            self.assertIsInstance(trn, Tuple)

    def test_process_data_metric(self):
        """Method that tests the process_data function"""

        TestDataProcessorUnit.input_unit.data_store.put_message(
            MessageType.METRIC, TestDataProcessorUnit.valid_msg
        )
        TestDataProcessorUnit.data_proc.shutdown_event.clear()

        process_data = Thread(
            target=TestDataProcessorUnit.data_proc.process_data,
            args=(
                MessageType.METRIC,
                BufferNames.METRICS_BUFFER,
                TestDataProcessorUnit.trans_map[MessageType.METRIC],
            ),
        )
        process_data.daemon = True
        process_data.start()
        time.sleep(1)

        queues = TestDataProcessorUnit.input_unit.data_store.queues
        self.assertGreaterEqual(queues[MessageType.METRIC].messages_in, 1)
        self.assertGreaterEqual(queues[MessageType.METRIC].messages_out, 1)
        TestDataProcessorUnit.data_proc.shutdown_event.set()

        out_buffs = TestDataProcessorUnit.output_unit._output_buffers
        self.assertGreaterEqual(out_buffs[BufferNames.METRICS_BUFFER].qsize(), 1)

    def test_process_data_log(self):
        """Method that tests the process_data function"""

        TestDataProcessorUnit.input_unit.data_store.put_message(
            MessageType.LOG, TestDataProcessorUnit.met_data
        )
        TestDataProcessorUnit.data_proc.shutdown_event.clear()

        process_data = Thread(
            target=TestDataProcessorUnit.data_proc.process_data,
            args=(
                MessageType.LOG,
                BufferNames.LOGS_BUFFER,
                TestDataProcessorUnit.trans_map[MessageType.LOG],
            ),
        )

        process_data.daemon = True
        process_data.start()
        time.sleep(1)

        queues = TestDataProcessorUnit.input_unit.data_store.queues
        self.assertGreaterEqual(queues[MessageType.LOG].messages_in, 1)
        self.assertGreaterEqual(queues[MessageType.LOG].messages_out, 1)
        TestDataProcessorUnit.data_proc.shutdown_event.set()

        out_buffs = TestDataProcessorUnit.output_unit._output_buffers
        self.assertGreaterEqual(out_buffs[BufferNames.LOGS_BUFFER].qsize(), 1)

    @patch("data_processing.data_processor.logging")
    def test_process_data_invalid_data_format(self, mock_logs):
        """Method that tests the process_data function exceptions"""

        TestDataProcessorUnit.input_unit.data_store.put_message(
            MessageType.METRIC, TestDataProcessorUnit.met_data
        )

        with patch.object(
            TestDataProcessorUnit.input_unit.data_store, "get_message"
        ) as get_msg_mock:
            with patch.object(
                TestDataProcessorUnit.data_proc, "shutdown_event"
            ) as shut_event_mock:

                shut_event_mock.is_set.side_effect = [False, False, True]
                exception_message = "Caught get_message" " exception processing data!"
                get_msg_mock.side_effect = TypeError(exception_message)
                with self.assertRaises(Exception):
                    TestDataProcessorUnit.data_proc.process_data(
                        MessageType.METRIC,
                        BufferNames.METRICS_BUFFER,
                        TestDataProcessorUnit.trans_map[MessageType.METRIC],
                    )
                    get_msg_mock.assert_called_once()
                    mock_logs.warning.assert_called_with(TypeError(exception_message))

    @patch("data_processing.data_processor.logging")
    def test_process_data_handle_enqueue_exception(self, mock_logs):
        """Method that tests the process_data function exceptions"""

        TestDataProcessorUnit.data_proc.output_unit._output_buffers: Dict = {
            BufferNames.LOGS_BUFFER: Queue(maxsize=1),
            BufferNames.METRICS_BUFFER: Queue(maxsize=1),
        }
        for _ in range(5):
            TestDataProcessorUnit.input_unit.data_store.put_message(
                MessageType.METRIC, UnifiedModel.parse_obj(TestDataProcessorUnit.valid_msg)
            )

        with patch.object(
            TestDataProcessorUnit.output_unit, "enque_data_in_buffer"
        ) as enq_dat_buff_mock:
            with patch.object(
                TestDataProcessorUnit.data_proc, "shutdown_event"
            ) as shut_event_mock:
                shut_event_mock.is_set.side_effect = [False, False, True]
                exception_message = "The buffer METRICS_BUFFER is full!"
                buff_full_error_exception = BufferFullError(exception_message)
                enq_dat_buff_mock.side_effect = buff_full_error_exception

                TestDataProcessorUnit.data_proc.process_data(
                    MessageType.METRIC,
                    BufferNames.METRICS_BUFFER,
                    TestDataProcessorUnit.trans_map[MessageType.METRIC],
                )
                enq_dat_buff_mock.assert_called()
                mock_logs.debug.assert_called_with(buff_full_error_exception)

    @patch("data_processing.data_processor.logging")
    def test_process_data_msg_not_enqueued_if_no_metrics(self, mock_logs):
        """Method that tests that if a metrics message does
        not contain any metrics if is not forwarded to the output
        buffer"""

        TestDataProcessorUnit.data_proc.output_unit._output_buffers: Dict = {
            BufferNames.LOGS_BUFFER: Queue(maxsize=1),
            BufferNames.METRICS_BUFFER: Queue(maxsize=1),
        }
        for _ in range(5):
            TestDataProcessorUnit.input_unit.data_store.put_message(
                MessageType.METRIC, TestDataProcessorUnit.met_data_with_no_metrics
            )

        with patch.object(
            TestDataProcessorUnit.output_unit, "enque_data_in_buffer"
        ) as enq_dat_buff_mock:
            with patch.object(
                TestDataProcessorUnit.data_proc, "shutdown_event"
            ) as shut_event_mock:
                shut_event_mock.is_set.side_effect = [False, False, True]

                TestDataProcessorUnit.data_proc.process_data(
                    MessageType.METRIC,
                    BufferNames.METRICS_BUFFER,
                    TestDataProcessorUnit.trans_map[MessageType.METRIC],
                )
                enq_dat_buff_mock.assert_not_called()

    @patch("data_processing.data_processor.logging")
    def test_process_data_func_exc(self, mock_logs):
        """Method that tests the process_data function"""

        TestDataProcessorUnit.input_unit.data_store.put_message(
            MessageType.METRIC, TestDataProcessorUnit.met_data
        )

        with patch.object(TestDataProcessorUnit, "trans_map") as trn_map_mock:
            with patch.object(
                TestDataProcessorUnit.data_proc, "shutdown_event"
            ) as shut_event_mock:
                shut_event_mock.is_set.side_effect = [False, False, True]
                exception_message = (
                    "Caught trn map function" " exception processing data!"
                )
                trn_map_mock.side_effect = Exception(exception_message)
                with self.assertRaises(Exception):
                    TestDataProcessorUnit.data_proc.process_data(
                        MessageType.METRIC,
                        BufferNames.METRICS_BUFFER,
                        TestDataProcessorUnit.trans_map[MessageType.METRIC],
                    )
                    trn_map_mock.assert_called_once()
                    mock_logs.warning.assert_called_with(Exception(exception_message))

                    shut_event_mock.is_set.assert_has_calls([call(), call(), call()])


if __name__ == "__main__":
    unittest.main(exit=False)
