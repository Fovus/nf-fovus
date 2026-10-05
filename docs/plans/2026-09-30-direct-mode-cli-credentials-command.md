# Direct mode: `fovus storage credentials` (fovus-cli-python) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a hidden `fovus storage credentials --pipeline-id <pid>` command that validates the pipeline and prints one JSON document with temporary S3 read and write credentials for nf-fovus direct mode.

**Architecture:** One new click command in the existing `storage` group, hidden from help and docs. It refuses to print to a terminal, validates the pipeline ID, calls the two existing token endpoints (upload = write, pipeline-scoped download = read), and writes credential contract v1 to stdout and nothing else. Errors go to stderr through the CLI's existing exception handling, except `NotSignedInException`, which the command reports on stderr itself.

**Tech Stack:** Python 3.9, click 8.1, pytest 8, `unittest.mock`.

**Spec:** `/Users/jashminpatel/Desktop/Code/Nextflow-Plugin/nf-fovus/docs/specs/2026-09-30-direct-storage-mode-design.md` (§7 and §8 are the contract this plan implements). The companion plugin plan is `docs/plans/2026-09-30-direct-mode-nf-fovus.md` in the same repo.

**Repo:** `/Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python`. Work on a new branch from `master`:

```bash
git -C /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python switch -c feat/storage-credentials-command master
```

## Global Constraints

- Python 3.9: no `X | None` unions, no `datetime.UTC`; use `timezone.utc`.
- The command is registered with `hidden=True`. No `.rst` page under `src/fovus/docs/commands/storage/`, no mention in `README.md` or `README_PUBLIC.md`.
- stdout carries exactly one JSON document on success and nothing at all on any failure. Never call `print()` or `logger.critical()` in this command (the `fovus` logger sends CRITICAL to stdout).
- Contract v1, key for key: `Version` (int 1), `Bucket`, `Region`, `Prefix` (= `pipelines/<pid>/`), `Read` and `Write`, each `{AccessKeyId, SecretAccessKey, SessionToken, Expiration}`; `Expiration` is `%Y-%m-%dT%H:%M:%SZ` UTC.
- Token requests: upload `{"workspaceId", "durationSeconds": 3600, "jobId": "", "storageType": "FOVUS_STORAGE"}`; download adds `"storageType": "PIPELINE_STORAGE"` and `"pipelineId"`. Build them with `FovusApiAdapter.get_file_upload_download_token_request`.
- Exit codes: 0 success; 1 validation or API error (the CLI's `main()` prints `UserException`/`SystemException` to stderr); 2 stdout is a terminal; 3 not signed in.
- Deleted statuses rejected: `DELETED`, `DELETING`, `DELETE_FAILED`. Workflow host must be `LOCAL`.
- Never log token responses. No key material in any log record, on stderr, or on stdout of a failed run.
- Run tests with the checkout's own sources: `PYTHONPATH=src python -m pytest …` (the machine's installed `fovus` is an editable install of a different worktree).
- Commit messages: plain imperative sentence, like the repo's history (e.g. "Add fovus storage copy and move commands").

## Review Focus

1. A pipeline ID whose owner part contains hyphens (`p-1700000000000-user-with-dash`) — the owner is everything after the timestamp, so it must still match that user. Test added in Task 3.
2. A `get_pipeline` response whose `status` is lowercase (`"deleted"`) — must still be rejected. Test added in Task 3.
3. A download-token response without `s3Region` — `Region` falls back to the CLI's configured region instead of crashing. Test added in Task 2.
4. Signed-in-via-`FOVUS_EMAIL`/`FOVUS_PAT` runs print "Environment credentials detected" to stderr — stdout must still parse as one JSON document. Covered by Task 2's stdout/stderr separation test (the env notice is stderr-only by construction).
5. A failure after the tokens were fetched (bucket mismatch) — stdout stays empty and no key reaches stderr or logs. Test added in Task 2.

---

### Task 1: Spike — check what the existing tokens allow (throwaway)

This answers the open questions in spec §12 that code reading could not. Nothing from this task is committed to fovus-cli-python.

**Files:**
- Create (scratch, not committed): `/tmp/fovus-direct-mode-spike.py`
- Modify: `/Users/jashminpatel/Desktop/Code/Nextflow-Plugin/nf-fovus/docs/specs/2026-09-30-direct-storage-mode-design.md` §12 (record the answers)

**Interfaces:**
- Consumes: `FovusApiAdapter()`, `get_file_upload_token`, `get_file_download_token`, `FovusApiAdapter.get_file_upload_download_token_request`.
- Produces: answers to spec §12 questions 1, 2 and 5, which Task 5 and Task 6 of the plugin plan rely on (delete → warn, abort → best effort, `CopyObject` → fallback).

- [ ] **Step 1: Create a scratch pipeline on the beta account**

Sign in to the beta environment with the CLI first (`fovus auth login`), then:

```bash
fovus --silence pipeline create --name direct-mode-spike --workflow-host local
```

Expected: one line of JSON containing `pipelineId`. Note the ID (`p-<digits>-<userId>`).

- [ ] **Step 2: Write the spike script**

```python
"""Throwaway spike for nf-fovus direct mode. Never prints keys."""
import sys

import boto3
from botocore.exceptions import ClientError

from fovus.adapter.fovus_api_adapter import FovusApiAdapter

pipeline_id = sys.argv[1]
api = FovusApiAdapter()
workspace_id = api.workspace_id
write_token = api.get_file_upload_token(FovusApiAdapter.get_file_upload_download_token_request(workspace_id))
read_token = api.get_file_download_token(
    FovusApiAdapter.get_file_upload_download_token_request(
        workspace_id, pipeline_id=pipeline_id, storage_type="PIPELINE_STORAGE"
    )
)


def client(token):
    keys = token["credentials"]
    return boto3.client(
        "s3",
        aws_access_key_id=keys["accessKeyId"],
        aws_secret_access_key=keys["secretAccessKey"],
        aws_session_token=keys["sessionToken"],
        region_name=token.get("s3Region", "us-east-1"),
    )


writer, reader = client(write_token), client(read_token)
bucket = read_token["authorizedBucket"]
print("same bucket:", bucket == write_token["authorizedBucket"])
base = f"pipelines/{pipeline_id}/spike/"


def attempt(name, action):
    try:
        action()
        print(f"{name}: OK")
    except ClientError as error:
        print(f"{name}: {error.response['Error']['Code']}")


def multipart_then_abort():
    upload_id = writer.create_multipart_upload(Bucket=bucket, Key=base + "mp.bin")["UploadId"]
    writer.upload_part(
        Bucket=bucket, Key=base + "mp.bin", UploadId=upload_id, PartNumber=1, Body=b"x" * (5 * 1024 * 1024)
    )
    writer.abort_multipart_upload(Bucket=bucket, Key=base + "mp.bin", UploadId=upload_id)


attempt("write PutObject", lambda: writer.put_object(Bucket=bucket, Key=base + "a.txt", Body=b"hello"))
attempt("read GetObject", lambda: reader.get_object(Bucket=bucket, Key=base + "a.txt")["Body"].read())
attempt("read HeadObject", lambda: reader.head_object(Bucket=bucket, Key=base + "a.txt"))
attempt("read ListObjectsV2", lambda: reader.list_objects_v2(Bucket=bucket, Prefix=base, Delimiter="/"))
attempt(
    "Q5 write CopyObject",
    lambda: writer.copy_object(Bucket=bucket, Key=base + "b.txt", CopySource={"Bucket": bucket, "Key": base + "a.txt"}),
)
attempt("Q2 write multipart + AbortMultipartUpload", multipart_then_abort)
attempt("Q1 write DeleteObject", lambda: writer.delete_object(Bucket=bucket, Key=base + "a.txt"))
attempt("Q2 lifecycle rules", lambda: print(reader.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]))
```

- [ ] **Step 3: Run it**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python && PYTHONPATH=src python /tmp/fovus-direct-mode-spike.py p-XXXXXXXXXXXXX-yourUserId
```

Expected: `same bucket: True`, then one line per check ending in `OK` or an S3 error code such as `AccessDenied`. The first four lines must be `OK`; if any is not, stop and report it, because the design depends on them.

- [ ] **Step 4: Record the answers in the spec**

In spec §12, replace questions 1, 2 and 5 with the observed result, following the style already used for questions 3 and 4, e.g.:

```markdown
1. ~~Can the upload token delete objects?~~ Answered by the spike on 2026-10-01: `DeleteObject` → OK.
```

Then commit in the nf-fovus repo:

```bash
cd /Users/jashminpatel/Desktop/Code/Nextflow-Plugin/nf-fovus
git add docs/specs/2026-09-30-direct-storage-mode-design.md
git commit -m "Record direct mode token spike results in the spec"
```

- [ ] **Step 5: Clean up**

Delete `/tmp/fovus-direct-mode-spike.py`. Leave the scratch pipeline, or delete it from the Fovus web app.

---

### Task 2: The hidden command and its JSON output

**Files:**
- Create: `src/fovus/commands/storage/commands/credentials/__init__.py` (empty)
- Create: `src/fovus/commands/storage/commands/credentials/storage_credentials_command.py`
- Modify: `src/fovus/commands/storage/storage_command.py`
- Modify: `tests/commands/storage/storage_command_test.py`
- Test: `tests/commands/storage/commands/credentials/storage_credentials_command_test.py`

**Interfaces:**
- Consumes: `FovusApiAdapter()` (zero-arg), `adapter.workspace_id`, `adapter.get_file_upload_token(dict) -> dict`, `adapter.get_file_download_token(dict) -> dict`, `FovusApiAdapter.get_file_upload_download_token_request(workspace_id, job_id=None, pipeline_id=None, storage_type="FOVUS_STORAGE", duration_seconds=3600) -> dict`.
- Produces: `storage_credentials_command` (click command named `credentials`); module-level `_stdout_is_terminal() -> bool` and `_now() -> datetime` (patch points for tests); `_credentials_document(fovus_api_adapter, pipeline_id: str) -> dict`. Task 3 adds validation around these without changing them.

- [ ] **Step 1: Write the failing tests**

Create `tests/commands/storage/commands/credentials/storage_credentials_command_test.py`:

```python
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

import fovus
from fovus.adapter.fovus_api_adapter import FovusApiAdapter as RealFovusApiAdapter
from fovus.commands.storage.commands.credentials import storage_credentials_command as module
from fovus.commands.storage.commands.credentials.storage_credentials_command import storage_credentials_command
from fovus.commands.storage.storage_command import storage_command
from fovus.exception.system_exception import SystemException
from fovus.exception.user_exception import NotSignedInException

PIPELINE_ID = "p-1700000000000-user1"
NOW = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
SECRETS = ["READ-SECRET", "READ-TOKEN", "WRITE-SECRET", "WRITE-TOKEN"]


def _token(prefix, bucket="fovus-user1-ws1-us-east-2"):
    return {
        "credentials": {
            "accessKeyId": f"{prefix}-KEY-ID",
            "secretAccessKey": f"{prefix}-SECRET",
            "sessionToken": f"{prefix}-TOKEN",
        },
        "authorizedBucket": bucket,
        "authorizedFolder": "pipelines/" if prefix == "READ" else "jobs/",
        "s3Region": "us-east-2",
    }


def _keys(prefix):
    return {
        "AccessKeyId": f"{prefix}-KEY-ID",
        "SecretAccessKey": f"{prefix}-SECRET",
        "SessionToken": f"{prefix}-TOKEN",
        "Expiration": "2026-09-30T13:00:00Z",
    }


def _runner():
    try:
        return CliRunner(mix_stderr=False)
    except TypeError:  # click >= 8.2 keeps stdout and stderr apart by default
        return CliRunner()


def _invoke(*args):
    return _runner().invoke(storage_credentials_command, list(args))


@pytest.fixture
def api():
    """The command's FovusApiAdapter class, with a signed-in adapter and a valid local pipeline."""
    with patch.object(module, "FovusApiAdapter") as adapter_class, patch.object(module, "_now", return_value=NOW):
        adapter_class.get_file_upload_download_token_request.side_effect = (
            RealFovusApiAdapter.get_file_upload_download_token_request
        )
        adapter = adapter_class.return_value
        adapter.workspace_id = "ws-1"
        adapter.get_user_id.return_value = "user1"
        adapter.get_pipeline.return_value = {"pipelineId": PIPELINE_ID, "status": "RUNNING", "workflowHost": "LOCAL"}
        adapter.get_file_upload_token.return_value = _token("WRITE")
        adapter.get_file_download_token.return_value = _token("READ")
        yield adapter_class


class TestRegistration:
    def test_is_registered_and_hidden(self):
        assert storage_command.commands["credentials"] is storage_credentials_command
        assert storage_credentials_command.hidden is True

    def test_storage_help_does_not_list_it(self):
        result = _runner().invoke(storage_command, ["--help"])

        assert result.exit_code == 0
        assert "credentials" not in result.output

    def test_has_no_docs_page(self):
        docs = Path(fovus.__file__).parent / "docs" / "commands" / "storage"

        assert not (docs / "credentials.rst").exists()


class TestTerminalRefusal:
    def test_refuses_to_print_credentials_to_a_terminal(self, api):
        with patch.object(module, "_stdout_is_terminal", return_value=True):
            result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 2
        assert result.stdout == ""
        assert "does not print credentials to a terminal" in result.stderr
        api.assert_not_called()


class TestOutput:
    def test_prints_one_contract_v1_document(self, api):
        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout) == {
            "Version": 1,
            "Bucket": "fovus-user1-ws1-us-east-2",
            "Region": "us-east-2",
            "Prefix": f"pipelines/{PIPELINE_ID}/",
            "Read": _keys("READ"),
            "Write": _keys("WRITE"),
        }

    def test_requests_the_existing_tokens_for_one_hour(self, api):
        _invoke("--pipeline-id", PIPELINE_ID)

        adapter = api.return_value
        adapter.get_file_upload_token.assert_called_once_with(
            {"workspaceId": "ws-1", "durationSeconds": 3600, "jobId": "", "storageType": "FOVUS_STORAGE"}
        )
        adapter.get_file_download_token.assert_called_once_with(
            {
                "workspaceId": "ws-1",
                "durationSeconds": 3600,
                "jobId": "",
                "storageType": "PIPELINE_STORAGE",
                "pipelineId": PIPELINE_ID,
            }
        )

    def test_region_falls_back_to_the_cli_region(self, api):
        read_token = _token("READ")
        del read_token["s3Region"]
        api.return_value.get_file_download_token.return_value = read_token

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 0, result.stderr
        assert json.loads(result.stdout)["Region"] == "us-east-1"

    def test_tokens_for_different_buckets_are_an_error(self, api):
        api.return_value.get_file_download_token.return_value = _token("READ", bucket="some-other-bucket")

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 1
        assert isinstance(result.exception, SystemException)
        assert result.stdout == ""


class TestNotSignedIn:
    def test_reports_on_stderr_and_exits_3(self, api):
        api.side_effect = NotSignedInException("test")

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 3
        assert result.stdout == ""
        assert "not signed in" in result.stderr


class TestSecretHygiene:
    @pytest.fixture
    def log_lines(self):
        """Every log record the CLI would write to ~/.fovus/logs, formatted with tracebacks."""
        lines = []

        class Collect(logging.Handler):
            def emit(self, record):
                lines.append(self.format(record))

        handler = Collect(level=logging.DEBUG)
        loggers = [logging.getLogger(), logging.getLogger("fovus")]
        levels = [logger.level for logger in loggers]
        for logger in loggers:
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        yield lines
        for logger, level in zip(loggers, levels):
            logger.removeHandler(handler)
            logger.setLevel(level)

    def test_no_key_reaches_logs_or_stderr(self, api, log_lines):
        succeeded = _invoke("--pipeline-id", PIPELINE_ID)
        api.return_value.get_file_download_token.return_value = _token("READ", bucket="some-other-bucket")
        failed = _invoke("--pipeline-id", PIPELINE_ID)

        seen = "\n".join(log_lines) + succeeded.stderr + failed.stderr + failed.stdout
        for secret in SECRETS:
            assert secret not in seen
```

Add to `tests/commands/storage/storage_command_test.py`, inside `TestStorageCommandRegistration`:

```python
    def test_credentials_command_is_registered(self):
        assert "credentials" in storage_command.commands
```

- [ ] **Step 2: Run the tests to see them fail**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python && PYTHONPATH=src python -m pytest tests/commands/storage -v
```

Expected: collection error `ModuleNotFoundError: No module named 'fovus.commands.storage.commands.credentials'`.

- [ ] **Step 3: Write the command**

Create the empty `src/fovus/commands/storage/commands/credentials/__init__.py`.

Create `src/fovus/commands/storage/commands/credentials/storage_credentials_command.py`:

```python
"""``fovus storage credentials``: temporary S3 credentials for nf-fovus direct storage mode.

Internal to the nf-fovus plugin and not a supported interface, so it is hidden from ``--help`` and the
docs. On success it writes exactly one JSON document (credential contract v1) to stdout and nothing
else, because the plugin reads it over a private pipe.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import click

from fovus.adapter.fovus_api_adapter import FovusApiAdapter
from fovus.config.config import Config
from fovus.constants.cli_constants import AWS_REGION
from fovus.exception.system_exception import SystemException
from fovus.exception.user_exception import NotSignedInException

CONTRACT_VERSION = 1
DURATION_SECONDS = 3600
EXIT_STDOUT_IS_TERMINAL = 2
EXIT_NOT_SIGNED_IN = 3
SOURCE = "storage_credentials_command"


def _stdout_is_terminal() -> bool:
    return sys.stdout.isatty()


def _now() -> datetime:
    return datetime.now(timezone.utc)


@click.command("credentials", hidden=True)
@click.option(
    "--pipeline-id",
    type=str,
    metavar="PIPELINE_ID",
    required=True,
    help="The pipeline whose work directory the credentials are for.",
)
def storage_credentials_command(pipeline_id: str):
    """
    Internal to the nf-fovus plugin; not a supported interface.

    Print temporary S3 credentials for one pipeline's work directory as a single JSON document.
    """
    if _stdout_is_terminal():
        click.echo(
            "This command is used internally by nf-fovus and does not print credentials to a terminal.",
            err=True,
        )
        sys.exit(EXIT_STDOUT_IS_TERMINAL)

    try:
        fovus_api_adapter = FovusApiAdapter()
        document = _credentials_document(fovus_api_adapter, pipeline_id)
    except NotSignedInException as exc:
        # The CLI's main() prints this exception to stdout; keep stdout for the JSON document only.
        click.echo(str(exc), err=True)
        sys.exit(EXIT_NOT_SIGNED_IN)

    sys.stdout.write(json.dumps(document))
    sys.stdout.flush()


def _credentials_document(fovus_api_adapter: FovusApiAdapter, pipeline_id: str) -> dict:
    issued_at = _now()
    workspace_id = fovus_api_adapter.workspace_id
    write_token = fovus_api_adapter.get_file_upload_token(
        FovusApiAdapter.get_file_upload_download_token_request(workspace_id, duration_seconds=DURATION_SECONDS)
    )
    read_token = fovus_api_adapter.get_file_download_token(
        FovusApiAdapter.get_file_upload_download_token_request(
            workspace_id,
            pipeline_id=pipeline_id,
            storage_type="PIPELINE_STORAGE",
            duration_seconds=DURATION_SECONDS,
        )
    )
    if write_token["authorizedBucket"] != read_token["authorizedBucket"]:
        raise SystemException(
            HTTPStatus.INTERNAL_SERVER_ERROR, SOURCE, "The read and write storage credentials name different buckets."
        )

    # The API returns no expiration: count the hour from just before the requests, so it errs early.
    expiration = (issued_at + timedelta(seconds=DURATION_SECONDS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "Version": CONTRACT_VERSION,
        "Bucket": read_token["authorizedBucket"],
        "Region": read_token.get("s3Region") or Config.get(AWS_REGION),
        "Prefix": f"pipelines/{pipeline_id}/",
        "Read": _session_keys(read_token, expiration),
        "Write": _session_keys(write_token, expiration),
    }


def _session_keys(token_response: dict, expiration: str) -> dict:
    credentials = token_response["credentials"]
    return {
        "AccessKeyId": credentials["accessKeyId"],
        "SecretAccessKey": credentials["secretAccessKey"],
        "SessionToken": credentials["sessionToken"],
        "Expiration": expiration,
    }
```

In `src/fovus/commands/storage/storage_command.py`, add the import after the `copy` import and register the command after `storage_copy_command`:

```python
from fovus.commands.storage.commands.credentials.storage_credentials_command import (
    storage_credentials_command,
)
```

```python
storage_command.add_command(storage_credentials_command)
```

- [ ] **Step 4: Run the tests to see them pass**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python && PYTHONPATH=src python -m pytest tests/commands/storage -v
```

Expected: all tests PASS, including the existing copy/move/download tests.

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python
pre-commit run --files src/fovus/commands/storage/storage_command.py src/fovus/commands/storage/commands/credentials/__init__.py src/fovus/commands/storage/commands/credentials/storage_credentials_command.py tests/commands/storage/storage_command_test.py tests/commands/storage/commands/credentials/storage_credentials_command_test.py
git add src/fovus/commands/storage tests/commands/storage
git commit -m "Add hidden fovus storage credentials command for nf-fovus direct mode"
```

Expected: every pre-commit hook passes (black, isort, flake8, pylint, mypy). Fix anything it reports before committing.

---

### Task 3: Validate the pipeline before issuing credentials

**Files:**
- Modify: `src/fovus/commands/storage/commands/credentials/storage_credentials_command.py`
- Test: `tests/commands/storage/commands/credentials/storage_credentials_command_test.py`

**Interfaces:**
- Consumes: `adapter.get_user_id() -> str`, `adapter.get_pipeline(pipeline_id) -> dict` (keys `status`, `workflowHost`; raises `UserException` for a missing pipeline), `WORKFLOW_HOST` and `WorkflowHost` from `fovus.validator.pipeline_config_validator`.
- Produces: `_pipeline_owner(pipeline_id: str) -> str` (raises `UserException`), `_validate_pipeline(fovus_api_adapter, pipeline_id: str, owner_user_id: str) -> None` (raises `UserException`).

- [ ] **Step 1: Write the failing tests**

Add to the imports of the test file:

```python
from http import HTTPStatus

from fovus.exception.user_exception import UserException
```

Append this class to the test file:

```python
class TestPipelineValidation:
    def test_rejects_a_malformed_id_before_any_network_call(self, api):
        result = _invoke("--pipeline-id", "not-a-pipeline")

        assert result.exit_code == 1
        assert isinstance(result.exception, UserException)
        assert result.stdout == ""
        api.assert_not_called()

    def test_rejects_another_users_pipeline(self, api):
        result = _invoke("--pipeline-id", "p-1700000000000-someoneelse")

        assert isinstance(result.exception, UserException)
        api.return_value.get_pipeline.assert_not_called()
        api.return_value.get_file_upload_token.assert_not_called()

    def test_accepts_an_owner_id_containing_hyphens(self, api):
        api.return_value.get_user_id.return_value = "user-with-dash"

        result = _invoke("--pipeline-id", "p-1700000000000-user-with-dash")

        assert result.exit_code == 0, result.stderr

    def test_rejects_an_unknown_pipeline(self, api):
        api.return_value.get_pipeline.side_effect = UserException(
            HTTPStatus.NOT_FOUND, "FovusApiAdapter", "Pipeline not found."
        )

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert isinstance(result.exception, UserException)
        api.return_value.get_file_upload_token.assert_not_called()
        api.return_value.get_file_download_token.assert_not_called()

    @pytest.mark.parametrize("status", ["DELETED", "DELETING", "DELETE_FAILED", "deleted"])
    def test_rejects_deleted_pipelines(self, api, status):
        api.return_value.get_pipeline.return_value = {"status": status, "workflowHost": "LOCAL"}

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert isinstance(result.exception, UserException)
        assert result.stdout == ""
        api.return_value.get_file_upload_token.assert_not_called()

    @pytest.mark.parametrize("status", ["CREATED", "RUNNING", "COMPLETED", "FAILED"])
    def test_accepts_pipelines_the_plugin_reuses(self, api, status):
        api.return_value.get_pipeline.return_value = {"status": status, "workflowHost": "LOCAL"}

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert result.exit_code == 0, result.stderr

    def test_rejects_fovus_hosted_pipelines(self, api):
        api.return_value.get_pipeline.return_value = {"status": "RUNNING", "workflowHost": "REMOTE"}

        result = _invoke("--pipeline-id", PIPELINE_ID)

        assert isinstance(result.exception, UserException)
        assert "REMOTE" in result.exception.message
        api.return_value.get_file_upload_token.assert_not_called()
```

- [ ] **Step 2: Run the tests to see them fail**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python && PYTHONPATH=src python -m pytest tests/commands/storage/commands/credentials -v
```

Expected: FAIL for every `TestPipelineValidation` test except `test_accepts_an_owner_id_containing_hyphens` and `test_accepts_pipelines_the_plugin_reuses` (no validation exists yet, so bad IDs succeed with exit 0).

- [ ] **Step 3: Add the validation**

In `storage_credentials_command.py`, extend the imports:

```python
import re

from fovus.exception.user_exception import NotSignedInException, UserException
from fovus.validator.pipeline_config_validator import WORKFLOW_HOST, WorkflowHost
```

Add constants after `SOURCE`:

```python
PIPELINE_ID_PATTERN = re.compile(r"^p-\d+-(.+)$")
DELETED_STATUSES = frozenset({"DELETED", "DELETING", "DELETE_FAILED"})
```

Replace the body of `storage_credentials_command` from `try:` to the end of the `except` block with:

```python
    owner_user_id = _pipeline_owner(pipeline_id)
    try:
        fovus_api_adapter = FovusApiAdapter()
        _validate_pipeline(fovus_api_adapter, pipeline_id, owner_user_id)
        document = _credentials_document(fovus_api_adapter, pipeline_id)
    except NotSignedInException as exc:
        # The CLI's main() prints this exception to stdout; keep stdout for the JSON document only.
        click.echo(str(exc), err=True)
        sys.exit(EXIT_NOT_SIGNED_IN)
```

Add the two helpers above `_credentials_document`:

```python
def _pipeline_owner(pipeline_id: str) -> str:
    """The user ID embedded in a pipeline ID (``p-<timestamp>-<userId>``), checked before any network call."""
    match = PIPELINE_ID_PATTERN.match(pipeline_id)
    if match is None:
        raise UserException(HTTPStatus.BAD_REQUEST, SOURCE, f"'{pipeline_id}' is not a pipeline ID.")
    return match.group(1)


def _validate_pipeline(fovus_api_adapter: FovusApiAdapter, pipeline_id: str, owner_user_id: str) -> None:
    if owner_user_id != fovus_api_adapter.get_user_id():
        raise UserException(HTTPStatus.FORBIDDEN, SOURCE, f"Pipeline {pipeline_id} does not belong to you.")

    pipeline = fovus_api_adapter.get_pipeline(pipeline_id)

    status = str(pipeline.get("status", "")).upper()
    if status in DELETED_STATUSES:
        raise UserException(HTTPStatus.BAD_REQUEST, SOURCE, f"Pipeline {pipeline_id} is {status}.")

    workflow_host = str(pipeline.get(WORKFLOW_HOST, WorkflowHost.LOCAL.api_value())).upper()
    if workflow_host != WorkflowHost.LOCAL.api_value():
        raise UserException(
            HTTPStatus.BAD_REQUEST,
            SOURCE,
            f"Pipeline {pipeline_id} runs on Fovus ({workflow_host}); direct storage credentials are only for "
            "pipelines launched on your own machine.",
        )
```

- [ ] **Step 4: Run the tests to see them pass**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python && PYTHONPATH=src python -m pytest tests/commands/storage tests/cli -v
```

Expected: all PASS (including `tests/cli/main_exit_codes_test.py`, which must be unaffected).

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python
pre-commit run --files src/fovus/commands/storage/commands/credentials/storage_credentials_command.py tests/commands/storage/commands/credentials/storage_credentials_command_test.py
git add src/fovus/commands/storage/commands/credentials tests/commands/storage/commands/credentials
git commit -m "Validate the pipeline before issuing storage credentials"
```

- [ ] **Step 6: Check the real command by hand (beta account)**

Use the scratch pipeline from Task 1:

```bash
cd /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python
PYTHONPATH=src python -m fovus.cli.fovus_cli --silence storage credentials --pipeline-id p-XXXXXXXXXXXXX-yourUserId
PYTHONPATH=src python -m fovus.cli.fovus_cli --silence storage credentials --pipeline-id p-XXXXXXXXXXXXX-yourUserId | python -c "import json,sys; d=json.load(sys.stdin); print(d['Version'], d['Bucket'], d['Prefix'], sorted(d['Read']), d['Read']['Expiration'])"
PYTHONPATH=src python -m fovus.cli.fovus_cli storage --help
```

Expected, in order: the first command (stdout is your terminal) prints the refusal on stderr and exits 2; the second prints `1 <bucket> pipelines/<pid>/ ['AccessKeyId', 'Expiration', 'SecretAccessKey', 'SessionToken'] <time about an hour from now>` without printing any key; the third lists `copy download mount move unmount upload` and not `credentials`.
