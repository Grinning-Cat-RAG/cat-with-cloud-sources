"""REST API of the plugin.

The paths are static on purpose: the core activates custom endpoints per agent by
exact path match, so path parameters would bypass that check.

The body is validated manually: FastAPI's 422 responses echo the invalid input,
which here would contain the credential.
"""
from typing import Any, Dict, List, Literal
from uuid import uuid4

from fastapi import BackgroundTasks, Body
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from cat import AuthPermission, AuthResource, AuthorizedInfo, check_permissions, endpoint, log
from cat.exceptions import CustomForbiddenException, CustomNotFoundException, CustomValidationException

from .connectors.base import ConnectorError, CredentialError
from .connectors.registry import available_connectors, get_connector_class
from .ingestion.config import load_connectors_settings
from .ingestion.pipeline import ConnectorIngestionPipeline, IngestionJob, build_connector_options

TAGS = ["Source Connectors"]


class ConnectorIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    provider: str = Field(description="Connector key, see GET /custom/connectors/providers")
    credential: Dict[str, Any] = Field(
        repr=False, description="Provider-specific credential: short-lived, optionally with a refresh token"
    )
    references: List[str] = Field(min_length=1, max_length=100, description="Items to ingest (IDs, URLs, prefixes...)")
    recursive: bool = True
    visibility: Literal["owner", "agent"] | None = Field(
        default=None, description="'owner': only the requesting user can recall the content. 'agent': everyone."
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra scalar metadata for every chunk")


class ConnectorIngestResponse(BaseModel):
    job_id: str
    provider: str
    references: int
    visibility: str
    scope: str
    info: str


class ConnectorDescription(BaseModel):
    provider: str
    display_name: str
    reference_help: str
    credential_fields: List[str]


def _parse_request(raw: Dict[str, Any]) -> ConnectorIngestRequest:
    try:
        return ConnectorIngestRequest.model_validate(raw)
    except ValidationError as e:
        problems = sorted({f"{'.'.join(str(p) for p in err['loc']) or '<body>'}: {err['type']}" for err in e.errors()})
        raise CustomValidationException(f"Invalid request: {'; '.join(problems)}") from None


@endpoint.post(
    "/connectors/ingest", response_model=ConnectorIngestResponse, tags=TAGS, status_code=202,
)
async def ingest_from_connector(
    background_tasks: BackgroundTasks,
    body: Dict[str, Any] = Body(..., description="See ConnectorIngestRequest"),
    info: AuthorizedInfo = check_permissions(AuthResource.UPLOAD, AuthPermission.WRITE),
) -> ConnectorIngestResponse:
    ccat = info.cheshire_cat
    if ccat is None:
        raise CustomNotFoundException("Agent not found: set the X-Agent-ID header.")

    payload = _parse_request(body)
    body.clear()

    connector_cls = get_connector_class(payload.provider)
    if connector_cls is None:
        raise CustomValidationException(f"Unknown provider '{payload.provider}'")

    try:
        credential = connector_cls.parse_credential(payload.credential)
    except CredentialError as e:
        raise CustomValidationException(str(e)) from None
    finally:
        payload.credential.clear()

    settings = await load_connectors_settings(ccat.plugin_manager.get_plugin(), ccat.agent_key)
    visibility = payload.visibility or settings.default_visibility
    if visibility == "agent" and not settings.allow_agent_visibility:
        raise CustomForbiddenException("Agent-wide visibility is disabled for this agent")

    target = info.stray_cat or ccat
    accepted_mime_types = set((await target.file_handlers()).keys())

    options = build_connector_options(settings, accepted_mime_types)
    try:
        connector_cls.validate_refresh(credential, options)
        connector_cls.validate_request(credential, payload.references, options)
    except ConnectorError as e:
        raise CustomValidationException(str(e)) from None

    job = IngestionJob(
        job_id=uuid4().hex,
        provider=connector_cls.provider,
        references=payload.references,
        recursive=payload.recursive,
        visibility=visibility,
        user_id=info.user.id,
        agent_id=ccat.agent_key,
        chat_id=info.stray_cat.id if info.stray_cat else None,
        user_metadata=payload.metadata,
    )
    pipeline = ConnectorIngestionPipeline(ccat, target, settings, accepted_mime_types)
    background_tasks.add_task(pipeline.run, connector_cls, credential, job)

    log.info(
        f"[connectors] job {job.job_id} queued: provider={job.provider} agent={job.agent_id} "
        f"chat={job.chat_id or '-'} references={len(job.references)} visibility={visibility}"
    )
    return ConnectorIngestResponse(
        job_id=job.job_id,
        provider=job.provider,
        references=len(job.references),
        visibility=visibility,
        scope="chat" if job.chat_id else "agent",
        info="Ingestion running in the background. Search the logs for the job id.",
    )


@endpoint.get("/connectors/providers", response_model=List[ConnectorDescription], tags=TAGS)
async def list_connectors(
    info: AuthorizedInfo = check_permissions(AuthResource.UPLOAD, AuthPermission.READ),
) -> List[ConnectorDescription]:
    return [
        ConnectorDescription(
            provider=provider,
            display_name=cls.display_name,
            reference_help=cls.reference_help,
            credential_fields=list(cls.credential_model.model_fields.keys()),
        )
        for provider, cls in sorted(available_connectors().items())
    ]
