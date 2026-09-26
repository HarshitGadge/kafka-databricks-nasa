"""Pipeline configuration and the GCN Kafka connection.

Credentials are never held in code or notebook state. ``client_id`` and
``client_secret`` are read from a Databricks secret scope at job start, and the
only place the secret value appears is inside the JAAS string handed to the
Kafka consumer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

GCN_BOOTSTRAP_SERVERS = "kafka.gcn.nasa.gov:9092"
GCN_TOKEN_ENDPOINT = "https://auth.gcn.nasa.gov/oauth2/token"
FERMI_FIN_POS_TOPIC = "gcn.classic.text.FERMI_GBM_FIN_POS"

_LOGIN_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
_CALLBACK_HANDLER = (
    "org.apache.kafka.common.security.oauthbearer.secured."
    "OAuthBearerLoginCallbackHandler"
)


@dataclass(frozen=True)
class CatalogConfig:
    """Unity Catalog target for the three medallion layers."""

    catalog: str = "kafka_nasa_fermi"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def table(self, layer: str, name: str) -> str:
        schema = getattr(self, f"{layer}_schema")
        return f"{self.catalog}.{schema}.{name}"


@dataclass(frozen=True)
class SecretConfig:
    """Where the GCN client credentials live in Databricks secrets."""

    scope: str = "gcn"
    client_id_key: str = "client-id"
    client_secret_key: str = "client-secret"


@dataclass(frozen=True)
class StreamConfig:
    """Kafka source settings for the GCN broker."""

    topic: str = FERMI_FIN_POS_TOPIC
    bootstrap_servers: str = GCN_BOOTSTRAP_SERVERS
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 5_000
    checkpoints: str = "/Volumes/kafka_nasa_fermi/bronze/checkpoints"
    extra_options: Dict[str, str] = field(default_factory=dict)


def kafka_options(
    client_id: str,
    client_secret: str,
    stream: StreamConfig | None = None,
) -> Dict[str, str]:
    """Build the Spark Kafka source options for an OAuth GCN subscription.

    GCN authenticates with SASL/OAUTHBEARER over TLS: the consumer exchanges the
    client credentials for a bearer token at the GCN auth endpoint and refreshes
    it on its own, so the job holds no long-lived token itself.
    """
    if not client_id or not client_secret:
        raise ValueError(
            "GCN client_id and client_secret are required; read them from the "
            "Databricks secret scope rather than passing literals."
        )
    stream = stream or StreamConfig()
    jaas = (
        f'{_LOGIN_MODULE} required '
        f'clientId="{client_id}" '
        f'clientSecret="{client_secret}";'
    )
    options = {
        "kafka.bootstrap.servers": stream.bootstrap_servers,
        "subscribe": stream.topic,
        "startingOffsets": stream.starting_offsets,
        "maxOffsetsPerTrigger": str(stream.max_offsets_per_trigger),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.oauthbearer.token.endpoint.url": GCN_TOKEN_ENDPOINT,
        "kafka.sasl.login.callback.handler.class": _CALLBACK_HANDLER,
        "kafka.sasl.jaas.config": jaas,
        # GCN replays a bounded history; a lost partition should not kill the job.
        "failOnDataLoss": "false",
    }
    options.update(stream.extra_options)
    return options


def redacted(options: Dict[str, str]) -> Dict[str, str]:
    """Copy of Kafka options safe to log: the JAAS secret is masked."""
    safe = dict(options)
    if "kafka.sasl.jaas.config" in safe:
        safe["kafka.sasl.jaas.config"] = "<redacted>"
    return safe
