from pathlib import Path
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import patch

from config_manager.config_manager import ConfigStore
from config_manager.models import OTELMetricsAdapterConfig
from controller.controller import RLFController


class TestAsyncCodeForSetVNFData(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @patch("controller.controller.repair_json")
    @patch("controller.controller.httpx.Response")
    @patch("controller.controller.RLController")
    @patch("output_unit.metric_storage.MetricStore."
                "set_status_based_on_netcat_updates")
    async def test_process_vnf_state_data_async(
            self, set_nc_mock, rl_controller_mock, resp_mock, repair_json_mock):

        resp_mock = mock.MagicMock(status_code=200, text='{"data": "some_data",}')
        repair_json_mock.return_value = '{"data": "some_data"}'
        response = resp_mock
        rl_controller_mock.is_sdep = False
        self.rlf_c.rl_controller = rl_controller_mock
        startup_cfg = {"OTELMetricsAdapter": OTELMetricsAdapterConfig()}
        config_store = ConfigStore(startup_config=startup_cfg)
        self.rlf_c.config_store = config_store
        await self.rlf_c._RLFController__process_vnf_state_data_async(response)
        repair_json_mock.assert_called()
        set_nc_mock.assert_called()

    @patch("controller.controller.RLController")
    @patch('controller.controller.httpx.AsyncClient.get')
    async def test_set_vnf_state_data_async(
            self, get_req_mock, rl_controller_mock):
        ncat_config = mock.MagicMock(address="0.0.0.0", port=8000, timeout=1)
        rl_controller_mock.is_sdep = False
        startup_cfg = {"OTELMetricsAdapter": OTELMetricsAdapterConfig()}
        config_store = ConfigStore(startup_config=startup_cfg)
        self.rlf_c.config_store = config_store
        self.rlf_c.rl_controller = rl_controller_mock
        await self.rlf_c.set_vnf_state_data_async(ncat_config)
        get_req_mock.assert_called()
