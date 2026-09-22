# Cat with Cloud Sources

Ingest documents from external storages into the Grinning Cat memory, using **short-lived credentials** passed by your backend.

| Provider key    | Source                                  | Credential passed to the Cat                     |
|-----------------|-----------------------------------------|--------------------------------------------------|
| `presigned_url` | Any object behind a pre-signed/SAS URL  | None: the signature is inside each URL           |
| `google_drive`  | Google Drive (My Drive, shared drives)  | User OAuth access token, optionally its refresh token |
| `s3`            | Amazon S3 and S3-compatible storages    | Temporary STS credentials                        |
| `azure_blob`    | Azure Blob Storage                      | Container SAS token, or Entra ID bearer token optionally with its refresh token |

Prefer `presigned_url` whenever the backend already knows which objects to ingest: no credential reaches the Cat at all. Use the other providers when the Cat must list folders, buckets or containers.

The Grinning Cat is meant to run as a microservice reachable only by your backend. The backend authenticates the user, keeps the long-lived secrets (cloud keys and, by default, refresh tokens) and hands the Cat only an access token that expires soon. For Google Drive and the Azure bearer token the backend may also send the refresh token, so that the Cat renews the access token during long jobs (see [Token expiry](#token-expiry)). The plugin never stores either of them.

## How it works

```
Frontend ──login──▶ Backend ──(API key + X-User-ID + X-Agent-ID)──▶ Grinning Cat
                     │  keeps refresh token (or sends it: Drive, Azure bearer)
                     └─ POST /custom/connectors/ingest ─────────────┘
                        { provider, credential: {access_token, [refresh_token]}, references }
```

Each request starts a background job with two phases:

1. **Collect**, which needs the credential. The job lists the referenced items, skips those already ingested at the same version and downloads the rest into a private temporary directory. Then the connector is closed and the credential is dropped.
2. **Ingest**, which runs without the credential. Each file goes through the configured ingestion engine. The chunks of the previous version are deleted only after the new version is confirmed in memory.

A slow embedding phase therefore cannot outlive the token, and a failed ingestion never loses the existing content.

### Token expiry

**Automatic refresh.** If the credential carries a `refresh_token` and the agent's settings contain the OAuth client that issued it, the connector renews the access token by itself:

| Provider       | Credential                           | OAuth client in the settings                                      |
|----------------|--------------------------------------|-------------------------------------------------------------------|
| `google_drive` | `access_token` + `refresh_token`     | `google_oauth_client_id`, `google_oauth_client_secret`            |
| `azure_blob`   | `bearer_token` + `refresh_token`     | `azure_tenant_id`, `azure_client_id`, `azure_client_secret` (Entra ID app; scope `https://storage.azure.com/.default offline_access`) |

The connector refreshes:

- before a request, when `expires_at` falls within the next 30 seconds (the new `expires_at` comes from the provider's `expires_in`);
- when the storage answers `401`: the token is refreshed once and the request is repeated. A second `401` stops the job. A `403` never triggers a refresh, because it means a missing scope or permission.

Not refreshable, because there is no refresh token to exchange: S3 STS credentials (new ones need a fresh `AssumeRole` with long-lived AWS keys) and Azure SAS tokens (a new SAS needs the account key or a user delegation key). A `refresh_token` sent with a `sas_token` is rejected with `400`. `presigned_url` has no credential at all.

With a `refresh_token`, even an already expired access token is accepted. Without the OAuth client in the settings, the request is rejected with `400`. If the identity provider refuses the refresh (for example `invalid_grant`, when the user revoked the access), the job stops. Only the OAuth error code is reported, never the tokens. If the provider rotates the refresh token (Entra ID always does), the new one is used for the rest of the job. The refreshed tokens live only in memory and are dropped with the connector at the end of phase 1: send the refresh token again with every request.

**Without automatic refresh** (S3, Azure SAS, or a credential without `refresh_token`) the backend is in charge of refreshing:

- **Before the job.** A credential whose `expires_at` falls within the next 30 seconds is rejected with `400`.
- **During phase 1.** `expires_at` is checked again before every request, retries included. If the provider answers that the credential is invalid or expired, the job stops in the same way.
- **When phase 1 stops.** No more items are listed or downloaded, but the files already downloaded are still ingested. The job report records the reason as `aborted`.
- **Phase 2** never needs the credential, so an expiring token cannot interrupt it.

`presigned_url` has no credential, so none of this applies to it: an expired signature fails only its own object.

`expires_at` is optional. Without it, the plugin only notices an expired token when the provider rejects it. Always send it when you know it.

If phase 1 may outlast the token (large folders, big files), refresh the token right before calling the Cat, or send the refresh token (Drive, Azure bearer). If a job is aborted, send the same request again with a new token: items already ingested at the same version are skipped as unchanged.

## Security model

- **Credentials are ephemeral.** They exist in memory only for the duration of phase 1, refresh tokens and refreshed access tokens included. They are never written to settings, logs, metadata, prompts or responses. Secrets are `SecretStr`, and validation errors never echo the input (the request body is validated manually for this reason).
- **Visibility is enforced on recall.** Every chunk carries `connector_owner` and `connector_visibility`:
  - `owner` (the default): only the user who ingested the item can recall it.
  - `agent`: every user of the agent can recall it. This can be disabled per agent.

  The filter runs in `after_cat_recalls_memories` with high priority and fails closed on errors. Chunks not produced by this plugin are untouched.
- **ACL metadata cannot be spoofed.** Keys starting with `connector_` and core keys (`source`, `chat_id`, ...) are stripped from caller metadata.
- **No SSRF.** Every caller-provided URL (pre-signed URLs, custom S3 endpoints, Azure account URLs) must use HTTPS and match `allowed_url_hosts`. IP literals and credentials embedded in URLs are refused, and redirects are not followed except by the Google Drive connector.
- **No secrets in the logs.** httpx logs every request URL, and query strings may carry SAS tokens or signatures, so the plugin strips them from httpx log records. Pre-signed URLs are treated as secrets: only their host and path are stored or reported.
- **Only the requested references.** A token or a refresh token often grants access to the whole storage, so the Cat limits itself to the references of the request. Every item is checked against them before it is downloaded, and containers outside them are never listed. An item outside the references is skipped and counted as `out_of_scope`. In detail:
  - Google Drive: a shortcut is followed only if its target lies inside a referenced file or folder, checked by walking up the target's parents. A shortcut given as the reference itself is always followed.
  - S3 and Azure: an object must be the referenced key or lie under the referenced prefix, in the same bucket or container.
  - Pre-signed URLs: only the referenced URLs.
  - Paths with `.` or `..` segments are refused in references and skipped in listings. httpx resolves them, so `shared/../admin/secret.pdf`, listed under `shared/`, would otherwise download `admin/secret.pdf`.

  This guards against plugin bugs, hostile content in the storage and malformed references. The token itself stays as broad as the provider issued it.
- **No collisions.** Source names are unique per provider, item and owner, so two `report.pdf` files in different folders, or the same file ingested by two users, never overwrite each other in the Cat storage.
- **Chat scope.** If the request carries `X-Chat-ID`, the content goes to that chat's memory, and the same visibility rules apply.

The visibility filter covers the standard recall flow only. Admin-level memory endpoints (points listing and search) and plugins that query the vector memory directly are not filtered, so keep those behind `MEMORY` permissions that end users do not have.

## API

### `POST /custom/connectors/ingest`

Requires `UPLOAD:WRITE`. The user is the one authenticated by the Cat: with the API key, pass `X-User-ID`.

Headers: `X-Agent-ID` is required. `X-Chat-ID` is optional and enables chat scope.

```json
{
  "provider": "google_drive",
  "credential": { "access_token": "ya29...", "expires_at": "2026-09-15T18:30:00Z" },
  "references": ["1AbC...folderId", "https://docs.google.com/document/d/1XyZ.../edit"],
  "recursive": true,
  "visibility": "owner",
  "metadata": { "team": "sales" }
}
```

For automatic refresh, add `"refresh_token": "..."` to a Google Drive credential or to an Azure credential with `bearer_token` (see [Token expiry](#token-expiry)).

Response (`202`):

```json
{ "job_id": "9f1c...", "provider": "google_drive", "references": 2, "visibility": "owner", "scope": "agent", "info": "..." }
```

Invalid or expired credentials, malformed references and URLs outside the allowed hosts are rejected with `400` before the job starts. The job outcome is logged with its `job_id`: discovered, unchanged, unsupported, over limit, downloaded, ingested, failed and out-of-scope counts, plus `truncated` when a limit stopped the enumeration and `aborted` with the reason when the job stopped early (for example, an expired credential). The Cat's standard ingestion hooks and webhooks keep working, because files go through the regular ingestion engine.

### `GET /custom/connectors/providers`

Requires `UPLOAD:READ`. Lists the providers, what a reference is for each one, and the credential fields they expect.

### Backend example (Python)

```python
import httpx

async def ingest_drive_folder(user, folder_id: str):
    access_token, expires_at = await token_store.fresh_google_token(user.id)  # refresh stays here
    async with httpx.AsyncClient(base_url=CAT_URL) as cat:
        r = await cat.post(
            "/custom/connectors/ingest",
            headers={"Authorization": f"Bearer {CAT_API_KEY}", "X-Agent-ID": AGENT_ID, "X-User-ID": user.cat_id},
            json={
                "provider": "google_drive",
                "credential": {"access_token": access_token, "expires_at": expires_at.isoformat()},
                "references": [folder_id],
            },
        )
        r.raise_for_status()
        return r.json()["job_id"]
```

## Google Drive

- **Token:** a user OAuth access token, obtained by the backend with the Authorization Code flow. The refresh token stays on the backend, unless you send it as `refresh_token` to enable [automatic refresh](#token-expiry). That also requires `google_oauth_client_id` and `google_oauth_client_secret` in the settings.
- **Scopes:**
  - `drive.readonly` is a *restricted* scope: public apps need Google verification.
  - `drive.file` is not sensitive, but only covers the files the user picked, for example with Google Picker.
- **References:** file or folder IDs, or Drive/Docs URLs. Shortcuts are followed only when their target lies inside the references (see [Security model](#security-model)), and shared drives are supported.
- **Native Google files** are exported to a format the Cat can parse, falling back to the next one if an export fails:
  - Docs: Markdown, then PDF, then plain text.
  - Sheets: CSV, then PDF. CSV exports only the first sheet.
  - Slides: PDF, then plain text.
  - Drawings: PDF.

  Forms, Sites and similar files are skipped. Google limits exports to about 10 MB.
- **Change detection:** `md5Checksum` for binary files, revision number and modified time for native files.
- Files whose owner disabled downloads are skipped. Rate limits and 5xx errors are retried with backoff.

## Pre-signed URLs

- **References:** one read-only, short-lived URL per object: an S3 pre-signed GET URL or an Azure blob URL with a SAS token. The credential is an empty object (`{}`). URLs with `.` or `..` path segments are refused.
- **Hosts:** by default only `amazonaws.com` and the Azure Blob domains (`blob.core.windows.net`, `blob.core.usgovcloudapi.net`, `blob.core.chinacloudapi.cn`) are accepted; extend `allowed_url_hosts` for other storages.
- **Change detection:** there is no way to check a pre-signed URL without downloading it (a HEAD request does not match a GET signature). The file is downloaded on every run, but it is re-ingested only if its ETag, or its SHA-256 when the ETag is missing, has changed.
- **Errors:** an expired or rejected signature fails only that object.

Backend example with boto3:

```python
url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300)
```

## Amazon S3

- **Credential:** `access_key_id`, `secret_access_key`, `session_token`, `region`. The optional `endpoint_url` and `force_path_style` are for S3-compatible services; the endpoint host must be in `allowed_url_hosts`.
- **Obtaining it:** the backend calls STS `AssumeRole`, or `AssumeRoleWithWebIdentity` with the user's OIDC token, passing a session policy and a short `DurationSeconds`:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::my-bucket/users/USER_ID/*"},
      {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::my-bucket",
       "Condition": {"StringLike": {"s3:prefix": "users/USER_ID/*"}}}
    ]
  }
  ```
- **References:**
  - `s3://bucket/key` ingests one object;
  - `s3://bucket/prefix/` ingests a folder;
  - `s3://bucket/prefix` without the trailing slash is tried as an object first, then as a folder. It never matches `prefix-other`.
  - keys with `.` or `..` segments are refused as references and skipped in listings.
- **Requests** are signed with SigV4 by the plugin (verified against botocore), with no boto dependency. Buckets with dots in the name use path-style addressing.
- **Change detection:** ETag.
- **Limits:** the listing does not report content types, so objects are filtered by extension, and objects without a recognizable extension are skipped. A bucket in a different region than `region` is reported as an error.
- **Errors:**
  - `ExpiredToken`, `InvalidAccessKeyId` and `SignatureDoesNotMatch` stop the job;
  - `AccessDenied` and `NoSuchKey` fail only that reference;
  - `SlowDown` is retried.

## Azure Blob Storage

- **Credential:** `account_url` (for example `https://acct.blob.core.windows.net`, which must be in `allowed_url_hosts`: the default accepts the public, US Government and China clouds) plus exactly one of:
  - `sas_token`: a container or directory SAS with permissions `rl` and a short expiry;
  - `bearer_token`: an Entra ID token for `https://storage.azure.com/.default`, with the *Storage Blob Data Reader* role. It may come with its `refresh_token` for [automatic refresh](#token-expiry), which also requires `azure_tenant_id`, `azure_client_id` and `azure_client_secret` in the settings. The Entra ID authority follows the cloud of `account_url`:

  | `account_url` host             | Cloud            | Entra ID authority          |
  |--------------------------------|------------------|-----------------------------|
  | `*.blob.core.windows.net`      | Public           | `login.microsoftonline.com` |
  | `*.blob.core.usgovcloudapi.net`| US Government    | `login.microsoftonline.us`  |
  | `*.blob.core.chinacloudapi.cn` | China (21Vianet) | `login.chinacloudapi.cn`    |

  The Entra ID app in the settings must be registered in the same cloud as the account. With any other host (for example a custom domain) a request with `refresh_token` is rejected with `400`, so the client secret is never sent to a guessed authority. Without `refresh_token`, custom domains keep working.
- **References:**
  - `container/blob` ingests one blob;
  - `container/prefix/` ingests a folder;
  - a blob URL of the same account is also accepted, without the SAS: the token goes only in the credential;
  - blob names with `.` or `..` segments are refused as references and skipped in listings.
- **Skipped entries:** hierarchical-namespace directories, folder placeholders and empty blobs.
- **Change detection:** ETag.
- **Errors:** authentication and authorization errors stop the job, missing containers or blobs fail only that reference, and `ServerBusy` is retried.

Backend example with `azure-storage-blob`:

```python
sas = generate_container_sas(account, container, user_delegation_key=key,
                             permission=ContainerSasPermissions(read=True, list=True),
                             expiry=datetime.now(timezone.utc) + timedelta(minutes=15))
```

## Settings

| Setting                  | Default | Description                                                          |
|--------------------------|---------|----------------------------------------------------------------------|
| `max_files_per_job`      | 500     | The enumeration stops when reached (`truncated`)                     |
| `max_items_scanned`      | 20000   | Stops listing very large folders, buckets or containers              |
| `max_file_size_mb`       | 50      | Enforced while streaming, even when the provider does not report the size |
| `max_total_size_mb`      | 1024    | Total downloaded bytes per job                                       |
| `store_files`            | true    | Keep files in the Cat storage (needed to re-embed on embedder change) |
| `enforce_owner_acl`      | true    | Apply the visibility filter on recall                                |
| `default_visibility`     | owner   | Used when the request does not specify it                            |
| `allow_agent_visibility` | true    | If false, requests with `visibility: agent` are refused (403)        |
| `allowed_url_hosts`      | `amazonaws.com,blob.core.windows.net,blob.core.usgovcloudapi.net,blob.core.chinacloudapi.cn` | Hosts or parent domains accepted in caller-provided URLs |
| `allow_http_urls`        | false   | Allow plain HTTP, only for local emulators (MinIO, Azurite)          |
| `google_oauth_client_id` | empty   | Google OAuth client, needed only to refresh Drive tokens             |
| `google_oauth_client_secret` | empty | Secret of the same OAuth client                                   |
| `azure_tenant_id`        | empty   | Entra ID tenant (ID or domain), needed only to refresh Azure bearer tokens |
| `azure_client_id`        | empty   | Entra ID app that issued the refresh tokens                          |
| `azure_client_secret`    | empty   | Secret of the same Entra ID app                                      |

The only secrets are `google_oauth_client_secret` and `azure_client_secret`. The core stores plugin settings as they are, so the plugin encrypts these two itself (`save_settings`/`load_settings` hooks in `settings.py`). It uses the core `StringCrypto`, whose key comes from `CAT_CRYPTO_KEY` and `CAT_CRYPTO_SALT`: Redis holds only the ciphertext, and the settings API and the plugin see the plaintext. The core also masks them towards users without write permission on the plugin settings. If a secret can no longer be decrypted (for example, `CAT_CRYPTO_KEY` changed), it is logged and ignored until it is saved again, and the other settings keep working. Leave the OAuth settings empty if the backend does the refreshing.

## Adding a provider

The plugin is organized in layers:

```
connectors/
  base.py          SourceCredential, SourceItem, DownloadResult, SourceConnector, HttpSourceConnector,
                   errors, URL allowlist, scope helpers, token refresh, log redaction
  google_drive.py  GoogleDriveConnector
  s3.py            S3Connector
  aws_sigv4.py     SigV4 request signing
  azure_blob.py    AzureBlobConnector
  presigned_url.py PresignedUrlConnector
  registry.py      provider key -> connector class
ingestion/
  pipeline.py      two-phase job, incremental updates, limits (provider-agnostic)
  metadata.py      metadata keys, source naming, visibility rule
  config.py        settings model, OAuth clients for token refresh
endpoints.py       REST API
access_control.py  recall-time visibility filter
settings.py        settings hooks, encryption of the secrets at rest
```

A new provider only touches `connectors/`:

1. **Credential.** Define a credential model with `SecretStr` fields for the secrets.
2. **Request validation.** Implement `validate_request` to check references and any caller-provided URL (with `check_url`) before the job is queued.
3. **Enumeration.** Implement `iter_items(reference, recursive)`, yielding one `SourceItem` per file:
   - a stable `item_id`;
   - a `version` that changes with the content;
   - a `mime_type` resolved with `resolve_mime_type`.

   If the version or the type is known only after the download, return it in `DownloadResult.version` or set `mime_type_known=False`. Never list containers outside the references.
4. **Scope.** Implement `in_scope(item)`: whether the item belongs to one of the job references (`self._references`, passed to the constructor). The pipeline calls it before every download, so it must fail closed. For flat namespaces, `within_prefix` and `has_dot_segments` do the work.
5. **Download.** Implement `download(item, destination)`. HTTP-based providers can extend `HttpSourceConnector`, which provides:
   - `_auth_headers` and `_auth_params` for header or query authentication;
   - `_get`, `_get_json` and `_stream_to`, with retries, size limits, RFC 3986 query encoding and no redirects;
   - `_raise_for_status`, to override so that provider errors map to `CredentialError` (stops the job), `TokenRejectedError` (an expired or revoked token: refreshed once if possible), `ItemNotFoundError` or `UnsupportedItemError` (skips one item) and `RetryableError`.
6. **Registration.** Add the class to `registry.py`.
7. **Refresh (optional).** If the credential can be renewed, override its `refreshable` property and implement `_obtain_refreshed_credential`, which returns the renewed credential. For OAuth providers, `_refresh_token_grant` performs the refresh_token grant, and its result's `credential_update` gives the fields to copy onto the credential. The OAuth client comes from `options.oauth_clients[provider]`, which is filled from the settings in `ingestion/config.py`: this is the only step outside `connectors/`. The endpoint already rejects a refreshable credential whose OAuth client is not configured (`validate_refresh`).

Pipeline, metadata, visibility and endpoints need no changes.

## Tests

The connector tests need only the plugin dependencies (no running Cat). From the plugin folder:

```bash
python -m unittest discover -s tests
```

## Limitations

- Visibility is applied after the recall, so a user may receive fewer than `k` chunks when many of the recalled ones belong to other users.
- Background jobs cannot be cancelled, and their status is reported only in the logs.
- Concurrent jobs on the same item and owner can duplicate its chunks: avoid launching them in parallel from the backend.
- Downloaded files are read into memory one at a time during ingestion, bounded by `max_file_size_mb`.

## License

Same license as the Grinning Cat Core.
