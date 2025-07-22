# pylint: disable=all

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, call

from data_processing.models.unified_model import UnifiedModel
from data_processing.sub_sampler import SubSampler


class TestSubSampler(unittest.TestCase):
    def setUp(self):
        self.sampler = SubSampler(sub_sampling_period=10, timeout_period=30)
        self.message1=UnifiedModel.parse_obj({"Hd":{"Ty":"Log","P":{"Fi":"a.cpp","Li":862},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20230831T124141.673414","Cl":{"ownr":"<IscCtx!1448>","oid":"<MbcpDefCC!3464>","info":"ppp0<-><IscCtxRemIf!1549>","sinf":"","goid":""},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":33406,"ctx":{"ctxId":"481bc2a1-8a96-432b-862b-61b6e1a953cf","oid":"<IscCtx!1448>"},"obj":"<MbcpDefCC!3464>","val":{"Sta":{"e":1,"cW":4480,"pBr":35840000,"Bu":{"B":112000},"ss":0,"inc":6144,"lgW":4380,"nSP":0,"owB":0,"owP":0,"oBr":12,"aBr":12,"rtt":{"Sta":{"rto":200,"rrto":26,"b":0,"s":0,"v":4},"Sts":{"min":1,"max":109,"ns":8940}},"lR":0,"cc":{"sst":4380}},"Sts":{"p":0,"t":0,"f":0,"c":0,"afE":0,"afP":0,"afB":0,"ifE":0,"ifP":0,"ifB":0,"idE":13915,"wlE":8940,"amE":1,"asE":9911,"Bu":{"tTu":759869,"nTu":145308,"nooB":0,"tTkn":647869,"tSto":0},"nETrs":1,"nBuTrs":1,"nFCwndTrs":0,"aI":{"sP":10232,"sB":647869,"fP":0,"fB":0,"tP":1,"tB":99}}}}})
        self.message2=UnifiedModel.parse_obj({"Hd":{"Ty":"Log","P":{"Fi":"a.cpp","Li":862},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20230831T124152.674514","Cl":{"ownr":"<IscCtx!1448>","oid":"<MbcpDefCC!3464>","info":"ppp0<-><IscCtxRemIf!1549>","sinf":"","goid":""},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":33406,"ctx":{"ctxId":"481bc2a1-8a96-432b-862b-61b6e1a953cf","oid":"<IscCtx!1448>"},"obj":"<MbcpDefCC!3464>","val":{"Sta":{"e":1,"cW":4480,"pBr":35840000,"Bu":{"B":112000},"ss":0,"inc":6144,"lgW":4380,"nSP":0,"owB":0,"owP":0,"oBr":12,"aBr":12,"rtt":{"Sta":{"rto":200,"rrto":26,"b":0,"s":0,"v":4},"Sts":{"min":1,"max":109,"ns":8940}},"lR":0,"cc":{"sst":4380}},"Sts":{"p":0,"t":0,"f":0,"c":0,"afE":0,"afP":0,"afB":0,"ifE":0,"ifP":0,"ifB":0,"idE":13915,"wlE":8940,"amE":1,"asE":9911,"Bu":{"tTu":759869,"nTu":145308,"nooB":0,"tTkn":647869,"tSto":0},"nETrs":1,"nBuTrs":1,"nFCwndTrs":0,"aI":{"sP":10232,"sB":647869,"fP":0,"fB":0,"tP":1,"tB":99}}}}})
        self.message3=UnifiedModel.parse_obj({"Hd":{"Ty":"Log","P":{"Fi":"a.cpp","Li":862},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20230831T124141.672414","Cl":{"ownr":"<IscCtx!1448>","oid":"<MbcpDefCC!3464>","info":"ppp0<-><IscCtxRemIf!1549>","sinf":"","goid":""},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":33406,"ctx":{"ctxId":"481bc2a1-8a96-432b-862b-61b6e1a953cf","oid":"<IscCtx!1448>"},"obj":"<MbcpDefCC!3464>","val":{"Sta":{"e":1,"cW":4480,"pBr":35840000,"Bu":{"B":112000},"ss":0,"inc":6144,"lgW":4380,"nSP":0,"owB":0,"owP":0,"oBr":12,"aBr":12,"rtt":{"Sta":{"rto":200,"rrto":26,"b":0,"s":0,"v":4},"Sts":{"min":1,"max":109,"ns":8940}},"lR":0,"cc":{"sst":4380}},"Sts":{"p":0,"t":0,"f":0,"c":0,"afE":0,"afP":0,"afB":0,"ifE":0,"ifP":0,"ifB":0,"idE":13915,"wlE":8940,"amE":1,"asE":9911,"Bu":{"tTu":759869,"nTu":145308,"nooB":0,"tTkn":647869,"tSto":0},"nETrs":1,"nBuTrs":1,"nFCwndTrs":0,"aI":{"sP":10232,"sB":647869,"fP":0,"fB":0,"tP":1,"tB":99}}}}})
        self.message4=UnifiedModel.parse_obj({"Hd":{"Ty":"Log","P":{"Fi":"a.cpp","Li":862},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20230831T124141.673414","Cl":{"ownr":"<IscCtx!1448>","oid":"<MbcpDefCC!3464>","info":"ppp0<-><IscCtxRemIf!1549>","sinf":"","goid":""},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":33406,"ctx":{"ctxId":"481bc2a1-8a96-432b-862b-61b6e1a953cf","oid":"<IscCtx!1448>"},"obj":"<MbcpDefCC!3465>","val":{"Sta":{"e":1,"cW":4480,"pBr":35840000,"Bu":{"B":112000},"ss":0,"inc":6144,"lgW":4380,"nSP":0,"owB":0,"owP":0,"oBr":12,"aBr":12,"rtt":{"Sta":{"rto":200,"rrto":26,"b":0,"s":0,"v":4},"Sts":{"min":1,"max":109,"ns":8940}},"lR":0,"cc":{"sst":4380}},"Sts":{"p":0,"t":0,"f":0,"c":0,"afE":0,"afP":0,"afB":0,"ifE":0,"ifP":0,"ifB":0,"idE":13915,"wlE":8940,"amE":1,"asE":9911,"Bu":{"tTu":759869,"nTu":145308,"nooB":0,"tTkn":647869,"tSto":0},"nETrs":1,"nBuTrs":1,"nFCwndTrs":0,"aI":{"sP":10232,"sB":647869,"fP":0,"fB":0,"tP":1,"tB":99}}}}})

    def test_should_process(self):
        self.sampler.messages = {}
        self.assertEqual(self.sampler.should_process(
            self.message1), True)
        self.assertEqual(self.sampler.should_process(
            self.message1), False)
        self.sampler.messages[self.message1.snapshot.obj] = (
            datetime.utcnow() - timedelta(seconds=12))
        self.assertEqual(self.sampler.should_process(
            self.message1), True)

    def test_should_process_by_tstamp(self):
        self.sampler.messages = {}
        self.assertEqual(self.sampler.should_process_by_tstamp(
            self.message1), True)
        self.assertEqual(self.sampler.should_process_by_tstamp(
            self.message3), False)
        self.assertEqual(self.sampler.should_process_by_tstamp(
            self.message2), True)

    def test_cleanup_unused_entries(self):
        self.sampler.messages = {}
        self.sampler.last_time_cleanup_was_performed = (datetime.utcnow() - timedelta(seconds=65))
        self.sampler.should_process(
            self.message1)

        self.assertEqual(len(self.sampler.messages.keys()), 1)
        self.sampler.clean_up_unused_entries()
        self.assertEqual(len(self.sampler.messages.keys()), 1)
        self.sampler.last_time_cleanup_was_performed = (datetime.utcnow() - timedelta(seconds=65))
        self.sampler.messages[self.message1.snapshot.obj] = (datetime.utcnow() - timedelta(seconds=60))
        self.sampler.clean_up_unused_entries()
        self.assertEqual(len(self.sampler.messages.keys()), 0)

        self.sampler.messages = {}
        self.sampler.last_time_cleanup_was_performed = (datetime.utcnow() - timedelta(seconds=65))
        self.sampler.should_process_by_tstamp(self.message1)
        self.sampler.should_process_by_tstamp(self.message4)
        self.assertEqual(len(self.sampler.messages.keys()), 2)
        self.sampler.clean_up_unused_entries()
        self.assertEqual(len(self.sampler.messages.keys()), 0)

    @patch("data_processing.sub_sampler.datetime")
    def test_should_process_new_obj_id(self, mock_datetime):
        mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
        obj_id = "test_id"
        snapshot_mock = MagicMock()
        message_mock = MagicMock()
        snapshot_mock.obj = obj_id
        message_mock.snapshot = snapshot_mock

        result = self.sampler.should_process(message_mock)

        self.assertTrue(result)
        self.assertIn(obj_id, self.sampler.messages)
        self.assertEqual(
            self.sampler.messages[obj_id], mock_datetime.utcnow())

    @patch("data_processing.sub_sampler.datetime")
    def test_should_process_existing_obj_id_after_period(self, mock_datetime):
        initial_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.return_value = initial_time
        obj_id = "test_id"

        self.sampler.messages[obj_id] = initial_time
        mock_datetime.utcnow.return_value = initial_time + timedelta(
            seconds=11)

        snapshot_mock = MagicMock()
        message_mock = MagicMock()
        snapshot_mock.obj = obj_id
        message_mock.snapshot = snapshot_mock

        result = self.sampler.should_process(message_mock)

        self.assertTrue(result)
        self.assertEqual(
            self.sampler.messages[obj_id], mock_datetime.utcnow())

    @patch("data_processing.sub_sampler.datetime")
    def test_should_process_existing_obj_id_within_period(self, mock_datetime):
        initial_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.return_value = initial_time
        obj_id = "test_id"

        self.sampler.messages[obj_id] = initial_time
        mock_datetime.utcnow.return_value = initial_time + timedelta(
            seconds=5)

        snapshot_mock = MagicMock()
        message_mock = MagicMock()
        snapshot_mock.obj = obj_id
        message_mock.snapshot = snapshot_mock
        result = self.sampler.should_process(message_mock)

        self.assertFalse(result)
        self.assertEqual(self.sampler.messages[obj_id], initial_time)

    @patch("data_processing.sub_sampler.datetime")
    @patch("data_processing.sub_sampler.logging.info")
    def test_clean_up_unused_entries2(self, mock_logging, mock_datetime):
        """Test clean_up_unused_entries method."""
        self.sampler.messages = {}
        initial_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.utcnow.return_value = initial_time

        self.sampler.messages = {
            "obj1": initial_time - timedelta(seconds=35),
            "obj2": initial_time - timedelta(seconds=15),
            "obj3": initial_time,
        }
        self.sampler.last_time_cleanup_was_performed = (
            initial_time - timedelta(seconds=61))

        mock_datetime.utcnow.return_value = initial_time
        self.sampler.clean_up_unused_entries()

        self.assertNotIn("obj1", self.sampler.messages)
        self.assertIn("obj2", self.sampler.messages)
        self.assertIn("obj3", self.sampler.messages)
        self.assertEqual(self.sampler.last_time_cleanup_was_performed,
                         mock_datetime.utcnow())

        mock_logging.assert_has_calls(
            [
                call("Number of cleaned-up entries: 1"),
                call("Number of object ids in sub sampler: 2")
            ]
        )
