from cat import plugin

from .ingestion.config import ConnectorsSettings


@plugin
def settings_schema():
    return ConnectorsSettings.model_json_schema()


@plugin
def settings_model():
    return ConnectorsSettings
