"""Registry of the available source connectors.

To add a provider: implement ``SourceConnector`` (or ``HttpSourceConnector``) in a
new module and add the class to ``_connector_classes``. Imports are resolved at
call time because the plugin loader reloads every module independently.
"""
from __future__ import annotations

from typing import Dict, List, Type

from .base import SourceConnector


def _connector_classes() -> List[Type[SourceConnector]]:
    from .azure_blob import AzureBlobConnector
    from .google_drive import GoogleDriveConnector
    from .presigned_url import PresignedUrlConnector
    from .s3 import S3Connector

    return [
        GoogleDriveConnector,
        S3Connector,
        AzureBlobConnector,
        PresignedUrlConnector,
    ]


def available_connectors() -> Dict[str, Type[SourceConnector]]:
    return {cls.provider: cls for cls in _connector_classes()}


def get_connector_class(provider: str) -> Type[SourceConnector] | None:
    return available_connectors().get(provider)
