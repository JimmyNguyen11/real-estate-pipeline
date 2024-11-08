

from __future__ import annotations

from airflow.example_dags.plugins import event_listener
from airflow.plugins_manager import AirflowPlugin


class MetadataCollectionPlugin(AirflowPlugin):
    name = "MetadataCollectionPlugin"
    listeners = [event_listener]
