"""Pydantic models for Apache Kafka operations."""

from typing import Any

from pydantic import BaseModel, Field


class TopicSpec(BaseModel):
    """A Kafka topic to create or address."""

    topic: str = Field(description="Topic name.")
    partitions_count: int = Field(
        default=1, description="Number of partitions for the topic."
    )
    replication_factor: int = Field(
        default=1, description="Replication factor for the topic."
    )
    configs: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional topic config overrides, e.g. {'retention.ms': '60000'}.",
    )


class ProduceRecord(BaseModel):
    """A record to produce to a Kafka topic."""

    topic: str = Field(description="Destination topic name.")
    value: Any = Field(description="Record value payload.")
    key: Any = Field(default=None, description="Optional record key.")
    value_format: str = Field(
        default="JSON",
        description="Value format: STRING, JSON, BINARY, AVRO, JSONSCHEMA, PROTOBUF.",
    )
