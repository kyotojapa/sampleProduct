"""This module stores the Pydantic Models for validating inputs
(sections in startup.conf)"""
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import (
    BaseModel, Field, StrictBool, StrictInt, StrictStr, validator)

from input_unit.shared_data import DecoderErrorScheme, PurgeScheme
from output_unit.otel_conf_model import OTELConfigModel

# pylint: disable=E0213
# pylint: disable=R0903


class InputUnitType(str, Enum):
    """ Enum class for input unit type """
    TCP_LISTENER = "TCPListener"
    AMQP_SUBSCRIBER = "AMQPSubscriber"


class GeneralConfig(BaseModel):
    """General config model. Maps to [General] section in startup.conf"""

    listening_port: int = Field(default=4444, ge=1024, le=65535)
    listening_address: StrictStr = "0.0.0.0"
    cleanup_existing_tcp_connection: bool = True
    input_buffer_chunk_size: int = Field(default=4096, ge=2048, le=16384)
    input_buffer_size: int = Field(default=16384, ge=8192, le=1024*100)
    input_buffer_purge_scheme: PurgeScheme = PurgeScheme.ENABLED
    input_buffer_error_scheme: DecoderErrorScheme = DecoderErrorScheme.STRICT
    queue_max_items_metrics: int = Field(default=1000, ge=100, le=100000)
    queue_max_items_logs: int = Field(default=1000, ge=100, le=100000)
    cycle_queue_metrics: bool = True
    cycle_queue_logs: bool = True
    enabled_adapters: List[StrictStr] = Field(default_factory=list)
    enabled_input_unit: InputUnitType = Field(
        default=InputUnitType.TCP_LISTENER)
    vault_location: Literal['edge', 'shore'] = "edge"
    vault_env: Literal['dev', 'qa', 'prod', 'common', 'admin'] = "prod"
    ctx_release_exp_back_off_enabled: bool = Field(default=False)
    ctx_release_hold_off_period: int = Field(default=50, ge=0, le=600)
    ctx_release_start_at: int = Field(default=60, ge=60, le=600)
    ctx_release_max_setting: int = Field(default=28800, ge=600, le=86400)
    ctx_release_exp_factor: float = Field(default=1.3, ge=1.01, le=2.0)
    sub_sampler_enable: bool = Field(default=False)
    sub_sampling_period: int = Field(default=10, ge=10, le=120)
    sub_sampler_cleanup_period: int = Field(default=60, ge=60, le=300)
    service_index: int = Field(
        default=int(os.getenv("SERVICE_INDEX", 1)), ge=1
    )

    @validator("enabled_adapters", pre=True)
    def csv_string(cls, v):
        "restrict and format enabled adapters"

        return [x.strip() for x in v.split(",") if len(x) > 0]


class VaultConfig(BaseModel):
    """Vault config model. Maps to [Vault] section in startup.conf"""

    address: StrictStr = "https://edge.vault:8200"
    path: StrictStr = (
        "plain-text/prod/edge/fleet-edge/internal/snapi/private/token")
    key: StrictStr = "/var/vault.key"
    token: StrictStr = "/var/vault.token"
    cert_path: StrictStr = "/var/vault.crt"
    auth_method: Literal['service-account', 'iam', 'token'] = 'token'
    vault_role: Literal['deploy', 'view',
                        'admin_shared_tools',
                        'machine_cicd_deploy', 'scone-core'] = "deploy"


class SNAPIConfig(BaseModel):
    """SNAPI config model. Maps to [SNAPI] section in startup.conf"""

    address: StrictStr = "0.0.0.0"
    port: int = Field(default=8081, ge=1024, le=65535)


class NCATConfig(BaseModel):
    """SNAPI config model. Maps to [SNAPI] section in startup.conf"""

    address: StrictStr = Field(default="10.0.3.9")
    port: int = Field(default=8000, ge=1024, le=65535)
    timeout: int = Field(default=1, ge=1, le=5)


class FileAdapterConfig(BaseModel):
    """Adapter config model. Maps to FileAdapter section in startup.conf"""

    rolling_file_max_bytes: int = Field(default=10 * 1024**2,
                                        ge=1024, le=20 * 1024**2)
    rolling_file_count: int = Field(default=10, ge=1, le=30)
    file_path: Path = Path("/var/log/rl/log")


class MQTTAdapterConfig(BaseModel):
    """Adapter config model. Maps to MQTTAdapter section in startup.conf"""

    mqtt_version: int = Field(default=3, ge=3, le=5)
    broker_port: int = Field(default=1883, ge=1024, le=65535)
    broker_address: str = "127.0.0.1"

    @validator('mqtt_version')
    def restrict_mqtt_version(cls, val: int):
        "restricts mqtt version to 3 or 5"

        if val == 4:
            raise ValueError('mqtt_version 4 not allowed!')
        return val


class FluentdAdapterConfig(BaseModel):
    """Adapter config model. Maps to FluentdAdapter section in startup.conf"""

    instance_port: int = Field(default=24224, ge=1024, le=65535)
    instance_address: StrictStr = "127.0.0.1"
    log_app: str = 'SDEP.RLF'
    log_index: str = 'sdwan'
    buf_max: int = Field(default=10 * 1024**2, ge=1 * 1024**2,
                         le=100 * 1024**2)
    queue_circular: bool = False


class PrometheusAdapterConfig(BaseModel):
    """Adapter config model. Maps to PrometheusAdapter section in
    startup.conf"""


class MetricTransformConfig(BaseModel):
    """Adapter config model. Maps to MetricTransform section in startup.conf"""

    loss_use_bytes: bool = Field(default=False)
    include_timedout_in_loss: bool = Field(default=True)
    include_outbound_bytecount: bool = Field(default=True)
    include_outbound_pktcount: bool = Field(default=False)
    include_inbound_pktcount: bool = Field(default=False)
    include_inbound_bytecount: bool = Field(default=True)


class OTELMetricsAdapterConfig(BaseModel):
    # pylint: disable=too-few-public-methods
    """Adapter config model. Maps to OTELMetricsAdapter section in
    startup.conf"""
    # resource attributes
    app: StrictStr = "sdwan.assurance_agent.prod1_logs_forwarder"
    system_id: Optional[StrictStr] = None
    sdp_id: Optional[StrictStr] = None
    sdep_dns: StrictStr = "sdep-101.shore.prod.prod2.com"
    namespace: StrictStr = "sdwan"
    location: StrictStr = "telco-cloud"

    # OTLP Metric Exporter arguments
    collector_endpoint: StrictStr = "http://localhost:4318/v1/metrics"

    certificate_file: Optional[StrictStr] = None
    timeout: Optional[int] = 2000

    # Periodic Exporting Metric Reader arguments
    export_interval_millis: List[int] = Field(
        default=[120000, 10000])  # type: ignore
    export_timeout_millis: List[int] = Field(
        default=[10000, 8000])  # type: ignore

    purge_timeout: Optional[int] = 360

    mqtt_version: int = Field(default=3, ge=3, le=5)
    mqtt_port: int = Field(default=1883, ge=1024, le=65535)
    mqtt_address: str = "127.0.0.1"

    enabled_exporters: List[StrictStr] = Field(default=[
        "otlp", "mqtt"])  # type: ignore

    running_rlf_version: StrictStr = Field(default="0.0.0")
    use_local_agent_cfg: bool = Field(default=False)
    excluded_dig_interfaces: List[StrictStr] = Field(default=[])
    excluded_vrfs_from_bgp: List[StrictStr] = Field(default=[])

    @validator("running_rlf_version")
    def validate_to_version(cls, value):
        """Change default values if empty string is an input"""

        if value == "":
            value = "0.0.0"
        return value
    
    @validator("enabled_exporters", pre=True)
    def validate_enabled_exporters(cls, v):
        """restrict and format enabled exporters"""
        return [x.strip() for x in v.split(",") if len(x) > 0]
    
    @validator("excluded_dig_interfaces", pre=True)
    def validate_excluded_dig_interfaces(cls, v):
        """restrict and format interfaces that can use dig KL for
        status"""
        return [x.strip() for x in v.split(",") if len(x) > 0]

    @validator("excluded_vrfs_from_bgp", pre=True)
    def validate_excluded_vrfs_from_bgp(cls, v):
        """restrict and format which VRF can use BGP for status"""
        return [x.strip() for x in v.split(",") if len(x) > 0]

    @validator("export_interval_millis", pre=True)
    def validate_exporter_interval_millis(cls, v):
        """restrict and format export interval millis"""
        temp = [int(x.strip()) for x in v.split(",") if len(x) > 0]
        for t in temp:
            if 2000 > t > 150000:
                raise ValueError(
                    'exporter interval millis should be within [2000, 150000]')
        return temp

    @validator("export_timeout_millis", pre=True)
    def validate_exporter_timeout_interval_millis(cls, v):
        """restrict and format export interval millis"""
        temp = [int(x.strip()) for x in v.split(",") if len(x) > 0]
        for t in temp:
            if 2000 > t > 20000:
                raise ValueError(
                    'exporter timeout millis should be within [2000, 20000]')
        return temp

    @validator("enabled_exporters")
    def validate_both(cls, v, values):
        """check if enabled exporters and exporter interval millis length is
        the same"""
        if v:
            if values and len(v) != len(values["export_interval_millis"]):
                raise ValueError("export interval millis and enabled exporters"
                                 " lists should have the same length")
            elif values and len(v) != len(values["export_timeout_millis"]):
                raise ValueError("export timeout millis and enabled exporters"
                                 " lists should have the same length")
        return v


class AMQPSubscriberConfig(BaseModel):
    """AMQP Subscriber config model. Maps to [AMQPSubscriber] section in
    startup.conf
    """
    amqp_host: str = "localhost"
    amqp_port: int = Field(default=5672, ge=1024, le=65535)
    amqp_user: str = "guest"
    amqp_pass: str = "guest"
    amqp_vhost: str = "/"
    amqp_logs_routing_keys: List[StrictStr] = Field(default_factory=list)
    amqp_metrics_routing_keys: List[StrictStr] = Field(default_factory=list)
    amqp_exchange: str = "logging"
    amqp_exchange_type: str = "topic"
    amqp_queue_name: str = "StandardAuditTransient"

    # used at queue creation on shore
    amqp_queue_max_size: int = Field(default=100000, description="Max number of messages in the queue.")

    @validator("amqp_logs_routing_keys", "amqp_metrics_routing_keys", pre=True)
    def csv_string(cls, v):
        "restrict and format amqp_routing_keys"

        return [x.strip() for x in v.split(",") if len(x) > 0]


class ServiceIndexConfig(BaseModel):
    """Service Index config model. Maps to [service_index] sections in
    query.conf
    """
    handle_ota_config_change_interval: Optional[int] = Field(default=120,
                                                             gt=0)
    set_status_data_interval: Optional[int] = Field(default=15, gt=0)
    set_vnf_state_data_interval: Optional[int] = Field(default=30, gt=0)
    disabled_queries: Optional[List[str]] = None

    @validator("disabled_queries", pre=True, always=True)
    def csv_string(cls, v):
        """Ensure disabled_queries is a list even if empty"""
        if v is None or v == "":
            return []
        return [x.strip() for x in v.split(",") if x.strip()]


class AgentConfigResources(BaseModel):
    """Model used for validating the common part of the resources section
        in the agent configuration received from OTA"""
    app: StrictStr
    location: StrictStr
    system_id: StrictStr
    environment: StrictStr


class AgentConfigResourcesSEEP(AgentConfigResources):
    """Model used for validating the SEEP resources section
        in the agent configuration received from OTA"""
    sdp_id: StrictStr


class AgentConfigResourcesSDEP(AgentConfigResources):
    """Model used for validating the SDEP resources section
        in the agent configuration received from OTA"""
    kubernetes_namespace: StrictStr


class AgentConfigExporters(BaseModel):
    """Model used for validating the exporters section
    in the agent configuration received from OTA"""
    enabled: StrictBool
    interval: StrictInt


class AgentConfig(BaseModel):
    """Model used for validating agent configuration received from OTA"""
    exporters: Dict[str, AgentConfigExporters]
    resources: Union[AgentConfigResourcesSEEP, AgentConfigResourcesSDEP]
    logs_config: List[StrictStr] = Field(alias="logs-config")
    metrics_config: List[StrictStr] = Field(alias="metrics-config")
    instruments_config: OTELConfigModel = Field(alias="instruments-config")


@dataclass
class CorrelationConfig:
    """Carries the correlation data for the correlation instrument"""
    connection: Any
    location: str
    ctx_release_exp_back_off_enabled: bool
    ctx_release_hold_off_period: int
    ctx_release_start_at: int
    ctx_release_max_setting: int
    ctx_release_exp_factor: float
    sub_sampler_enable: bool
    sub_sampling_period: int
    sub_sampler_cleanup_period: int
    disabled_queries: List[str]


AnyConfig = Union[
    GeneralConfig,
    VaultConfig,
    SNAPIConfig,
    FileAdapterConfig,
    MQTTAdapterConfig,
    FluentdAdapterConfig,
    PrometheusAdapterConfig,
    MetricTransformConfig,
    OTELMetricsAdapterConfig,
]
