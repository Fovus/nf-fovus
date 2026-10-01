# Direct storage mode for nf-fovus — design

- **Date:** 2026-09-30
- **Status:** Draft for review
- **Revised:** 2026-10-01, after review of the implementation: D9–D11
- **Repos affected:** `nf-fovus` (most of the work), `fovus-cli-python` (one hidden command)
- **Backend (fovus-infra):** no change

## 1. Summary

Today nf-fovus requires Fovus storage to be FUSE-mounted on the machine running Nextflow (the
control node). Some users cannot mount FUSE there. This design adds a second storage mode,
**direct mode**, in which the plugin reads and writes the pipeline's work directory directly with
the AWS S3 SDK for Java, using short-lived credentials obtained from the Fovus CLI. Users select it
by pointing Nextflow's `workDir` at `fovus:///fovus-storage/pipelines` instead of at a mount.
In direct mode the same `fovus://` filesystem also reads Fovus storage `files/` and `jobs/` and
writes `files/`, so inputs can come from, and `publishDir` can publish to, Fovus storage without a
mount.

The existing mount mode is unchanged: a local `workDir` keeps working exactly as today.

## 2. Background

### How mount mode works today

- `FovusExecutor.validateWorkDir()` runs `fovus storage mount --mount-storage-path <workDir.parent>
  --no-auto-remount` ([FovusExecutor.groovy:94](../../src/main/groovy/fovus/plugin/FovusExecutor.groovy),
  [FovusStorageClient.groovy:26](../../src/main/groovy/fovus/plugin/storage/FovusStorageClient.groovy)).
  `workDir` must end with `pipelines`.
- The executor work directory is `<mount>/pipelines/<pid>/fovus-work`. Nextflow writes
  `.command.run`, `.command.sh`, `.command.fovus.env` and (for array tasks) `run.sh` with plain
  file writes. `FovusTaskHandler.readExitFile()` and Nextflow's output collection read
  `.exitcode` and outputs from the same mount.
- `FovusExecutor.getRemotePath()` rewrites the local mount prefix to `/fovus-storage` so the
  compute node can reach the same files through its own mount.
- Files outside the mount are "foreign" and Nextflow's `FilePorter` copies them into the stage
  directory on the mount.

### Facts from the Fovus CLI that shape the design

- `fovus storage mount` uses mountpoint-s3 (`mount-s3`), one mount per prefix, mapping objects
  one to one:

  | Mount path | S3 location | Mode on the mount |
  |---|---|---|
  | `/fovus-storage/files/X` | `s3://<bucket>/files/X` | 0770 |
  | `/fovus-storage/jobs/<jobId>/X` | `s3://<bucket>/jobs/<jobId>/X` | 0550 |
  | `/fovus-storage/pipelines/<pid>/X` | `s3://<bucket>/pipelines/<pid>/X` | 0770 |

  The bucket is `fovus-<userId>-<workspaceId>-<region>` (or `authorizedBucket` from the API).
  Objects written with the SDK therefore appear on the compute node's `/fovus-storage` mount
  with no extra step, and `pipelines/` files are executable there.
- `fovus job create --pipeline-id` uploads nothing. It derives the run folder from the text of
  the job directory after the pipeline ID (`FileUtil.get_run_folders_for_pipeline_jobs`), so a
  path such as `/fovus-storage/pipelines/<pid>/fovus-work/ab` works without existing locally
  (to be confirmed in the spike, §12).
- Temporary S3 credentials come from two existing API endpoints, both 1-hour STS credentials:
  - `get-file-upload-token` (`storageType = FOVUS_STORAGE`, `jobId = ""`): writes. The CLI uses
    it for uploads to both `files/` and `pipelines/`, so its policy is wider than one pipeline.
    The upload role grants only `s3:PutObject` on the bucket, so `DeleteObject`,
    `AbortMultipartUpload` and `CopyObject` are denied. `CreateMultipartUpload`, `UploadPart`
    and `CompleteMultipartUpload` work under `PutObject`.
  - `get-file-download-token` (`storageType = PIPELINE_STORAGE`, `pipelineId`): reads and
    listings. The request names the pipeline, but the download role grants `s3:GetObject` on
    the whole bucket and `s3:ListBucket`, so the token can read the user's whole bucket.

  These permissions come from reading fovus-infra (`create-s3-rw-roles.ts`,
  `get-file-download-token.ts`) and are to be confirmed by the spike (§12).

  The API returns no expiration; the CLI assumes 3600 s and re-fetches after 55 minutes.
- No CLI command prints S3 credentials today.
- `FovusUtil.executeCommand` logs the CLI's full stdout at debug level
  ([FovusUtil.groovy:73](../../src/main/groovy/fovus/plugin/FovusUtil.groovy)), so it must not
  be used to fetch credentials.

### Facts from Nextflow 25.10 that shape the design

- Plugins declared in the config are loaded before the `Session` is created and `workDir` is
  parsed (`CmdRun.groovy:362-396`), so a `fovus://` `workDir` is parsed by this plugin's
  filesystem provider.
- `Session.init()` calls `workDir.mkdirs()` (`Session.groovy:440`) before any executor registers,
  that is before the pipeline ID or any credentials exist.
- `cleanup = true` is skipped, with a warning, when `workDir` is not on the local filesystem
  (`Session.groovy:1177`).
- `publishDir` switches `symlink` / `link` modes to `copy`, with a warning, when the source is on
  another filesystem (`PublishDir.groovy:566-576`).

## 3. Goals and non-goals

### Goals

1. Run an nf-fovus pipeline end to end on a control node with no FUSE mount.
2. In direct mode, upload inputs and all Nextflow-generated task files with the S3 SDK, and read
   the exit code and check output files with the S3 SDK after a job finishes.
3. Authenticate with temporary credentials from the Fovus CLI, handed to the plugin without
   exposing them in terminals, logs, arguments, files or task environments.
4. Keep `-resume`, `publishDir` to local paths, array tasks, `errorStrategy` and input staging
   behaving as they do in mount mode.
5. Leave mount mode and Fovus-hosted runs unchanged.
6. In direct mode, read inputs from Fovus storage `files/` and `jobs/` (`fovus://` paths) and
   publish into `files/`.

### Non-goals (first version)

- `fovus://` paths in mount mode. With a mount, inputs and publish targets use the mounted path.
- Carrying the `-resume` cache across a switch between mount and direct mode.
- A new backend endpoint for pipeline-scoped read/write credentials (possible follow-up, §13).
- Automatic fallback to direct mode when FUSE is unavailable. The user chooses the mode.
- Changes to the compute-node side or the job run command.

## 4. Decisions

| # | Decision | Reason |
|---|---|---|
| D1 | Use the existing upload and download token endpoints; no backend change. | Ships from nf-fovus and the CLI alone. |
| D2 | Make the plugin's own `fovus://` filesystem read and write S3, and use it as the work directory in direct mode. | Nextflow keeps its normal staging, output collection and `-resume`. Fovus credentials stay isolated from the user's own AWS credentials. Two separate tokens (read/write) are possible. |
| D3 | Rejected: Nextflow's `nf-amazon` `s3://` work directory. | One global credential chain: cannot use separate read/write tokens, and would override the user's AWS credentials for other `s3://` inputs. |
| D4 | Rejected: local work directory synced at fixed points. | Every output would be downloaded in full to the control node. |
| D5 | The CLI command is hidden (`hidden=True`) and undocumented. | Not a public interface. This deters casual use only; the real limit is the plugin's prefix guard (both tokens are bucket-wide), not the credentials' server-side scope. |
| D6 | The CLI validates the pipeline ID before issuing credentials. | Stops credentials being issued for another user's, a deleted, or a hosted pipeline. |
| D7 | The mode follows the scheme of Nextflow's `workDir`: a local path (the mount) is mount mode, `fovus:///fovus-storage/pipelines` is direct mode. There is no `fovus.storageMode` setting. | Still an explicit choice, with no automatic fallback. Matches how Nextflow treats `s3://` work directories, works with `-w`, and keeps `session.workDir` equal to where task files actually live. Suggested in review. |
| D8 | The mode is called "direct" in docs and messages, not "S3". | Describes what users get (storage reached directly, no mount) rather than the AWS service behind it. "Remote" was avoided because it already means a Fovus-hosted run (`WORKFLOW_HOST=REMOTE`); "sync" because it suggests a local mirror. |
| D9 | One S3-backed storage serves every `fovus://` area (`pipelines`, `files`, `jobs`). The old `files`/`jobs` filesystem, which listed and downloaded through `fovus` subprocesses, is removed, so `fovus://` paths work only in direct mode. | One code path instead of two. Suggested in review. The read token already reaches the whole bucket, and one `HeadObject` replaces a CLI subprocess per file lookup, which `-resume` does for every input. The CLI-backed areas predate the mount and were undocumented; mount mode uses mounted paths. |
| D10 | Direct mode also writes into `files/` (`publishDir`, uploads). `jobs/` stays read-only. | Suggested in review. The upload token already writes `files/` (the CLI uses it for `fovus storage upload`), and without it a run with no mount could not put results in Fovus storage. |
| D11 | File transfers and streamed writes use the AWS SDK's S3 Transfer Manager and the Java-based S3 async client with multipart enabled (Netty HTTP client). The plugin has no multipart or ranged-download code of its own. | Suggested in review: the SDK's tested part handling, retries and parallelism instead of our own. The Java-based client (parallel multipart since SDK 2.27.5) avoids the CRT's native libraries on users' control nodes. |

## 5. User-facing configuration

```groovy
// mount mode (unchanged)
workDir = '/home/me/fovus-mount/pipelines'

// direct mode (or on the command line: nextflow run … -w fovus:///fovus-storage/pipelines)
workDir = 'fovus:///fovus-storage/pipelines'

fovus {
    pipelineName = 'my-pipeline'
}
```

- A `fovus://` `workDir` must be exactly `fovus:///fovus-storage/pipelines`. Anything else, such as
  `fovus:///fovus-storage/files/…` or `fovus:///fovus-storage/pipelines/x`, fails at start-up.
- In direct mode other `fovus://` paths are ordinary inputs and outputs:
  `fovus:///fovus-storage/files/…` and `fovus:///fovus-storage/jobs/<jobId>/…` can be read, and
  `publishDir` can target `fovus:///fovus-storage/files/…`. In mount mode they fail with a message
  pointing to the mounted path.
- Task folders live under `fovus:///fovus-storage/pipelines/<pid>/fovus-work/`, the same S3 prefix
  mount mode uses.
- Direct mode is only for pipelines launched on the user's machine. A `fovus://` `workDir` on a
  Fovus-hosted run (`WORKFLOW_HOST=REMOTE`) fails at start-up with a clear message.
- `fovus.auth` works the same in both modes.
- The plugin README documents the `fovus://` `workDir` and the minimum Fovus CLI version. It does
  not mention the CLI credentials command.

## 6. Architecture

### 6.1 Components

| Component | Repo | New / changed | Responsibility |
|---|---|---|---|
| `WorkDirStorage` (interface) | nf-fovus | new | Everything the executor and task handler need to know about where the work directory lives. A factory picks the implementation from `session.workDir`'s scheme and validates it. |
| `MountedWorkDirStorage` | nf-fovus | new (moved code) | Today's mount behaviour, moved without change. |
| `DirectWorkDirStorage` | nf-fovus | new | Direct mode: credentials, S3 client, attaching the client to the `fovus://` filesystem provider, path rules. |
| `FovusFileSystemProvider`, `FovusPath`, `FovusFileSystem` | nf-fovus | changed | Add the `pipelines` storage area. Every area reads and writes through one S3-backed `S3Storage` once direct mode attaches an S3 client; `jobs` is read-only. The filesystems exist before credentials do. The CLI-backed listing and download code is removed. |
| `S3Storage` | nf-fovus | new | NIO operations (streams, channels, listing, attributes, copy, move, upload, download) for any `fovus://` path, on top of `FovusS3Client`. |
| `FovusS3Client` | nf-fovus | new | Two AWS SDK v2 `S3Client`s (read, write) for metadata and small objects, two S3 Transfer Managers for file transfers and streamed writes, the access guard, error mapping. |
| `FovusStorageCredentialsSource` | nf-fovus | new | Runs the CLI command with a dedicated, non-logging runner and parses the JSON. |
| `RefreshingStorageCredentials` | nf-fovus | new | Caches one fetch; exposes read and write `AwsCredentialsProvider`s; refreshes before expiry. |
| `FovusExecutor`, `FovusTaskHandler`, `FovusFileCopyStrategy` | nf-fovus | changed | Delegate mount-specific logic to `WorkDirStorage`; defer completion on temporary errors. |
| `fovus storage credentials` | fovus-cli-python | new (hidden) | Validate the pipeline ID, fetch both tokens, print one JSON document. |

### 6.2 `WorkDirStorage`

The executor and handler currently assume a mount in three places: preparing the work directory,
deciding which files are foreign, and rewriting paths for the compute node. They move behind one
interface:

```groovy
interface WorkDirStorage {
    static WorkDirStorage forSession(Session session)  // picks and validates the mode from session.workDir
    void prepare(Session session, String pipelineId)   // mount, or fetch credentials + attach the S3 client
    boolean isForeignFile(Path path)
    Path remotePath(Path path)                           // the path as the compute node sees it
}
```

| | `MountedWorkDirStorage` | `DirectWorkDirStorage` |
|---|---|---|
| Chosen when `session.workDir` is | a local path | a `FovusPath` |
| `workDir` check | must end with `pipelines` (as today) | must be exactly `/fovus-storage/pipelines`; not allowed on a Fovus-hosted run |
| `prepare` | `fovus storage mount … --no-auto-remount` | first credential fetch (fails fast); build `FovusS3Client`; attach it to the `fovus://` filesystem provider |
| `isForeignFile` | outside the mount folder, or a different scheme | not a `FovusPath` |
| `remotePath` | swap the mount prefix for `/fovus-storage` | `Path.of(path.toString())`, which is already `/fovus-storage/…` |

Any other `workDir` scheme (for example `s3://`) fails at start-up, as it effectively does today.

`FovusExecutor.isForeignFile()` and `getRemotePath()` delegate to it. `FovusExecutor.getWorkDir()`
does not change: `session.workDir.resolve(<pid>).resolve('fovus-work')` gives
`<mount>/pipelines/<pid>/fovus-work` in mount mode and
`fovus:///fovus-storage/pipelines/<pid>/fovus-work` in direct mode.

**`chmod` calls are removed in both modes.** mountpoint-s3 does not support changing
permissions ("Modifying file metadata (`chmod`, `chown`, `chgrp`) is not supported"), and the CLI
fixes them at mount time with `--file-mode` / `--dir-mode` (0770 for `files/` and `pipelines/`).
The plugin's current `chmod` calls in `FovusTaskHandler.submit()`, `prepareArrayTasks()` and
`FovusExecutor.uploadBinDir()` run through `.execute()` without checking the result, so they
already have no effect in mount mode. Direct mode has no local files to change either. Scripts are
executable on the compute node because its mount shows `pipelines/` files as 0770.

### 6.3 Filesystem provider

- `FovusPath` accepts a third storage area, `pipelines`, alongside `files` and `jobs`. Its
  `toString()` already yields `/fovus-storage/<area>/<key>`, the compute-node path, and its S3 key
  is `<area>/<key>`, the object the mount shows at that path.
- Every area reads and writes through `S3Storage` on top of `FovusS3Client` (D9):
  - The filesystems are created when Nextflow parses a `fovus://` path (the `workDir` first),
    before the pipeline ID or any credentials exist, so they start without an S3 client.
    `DirectWorkDirStorage.prepare()` attaches one client to the provider, shared by all areas.
  - Area roots (`/fovus-storage/files`, `/fovus-storage/jobs`, `/fovus-storage/pipelines`) need no
    S3 call: they exist, are folders, and `mkdirs()` on them does nothing. This is what
    `Session.init()` does with the `workDir`.
  - Any other access before the client is attached, or in mount mode, raises a clear error.
  - Access by area (enforced by `FovusS3Client`, §6.4): `pipelines/<pid>/` and the session scratch
    folders read/write; `files/` read/write; `jobs/` read-only; other pipelines' folders are
    invisible.
- Operations:

  | NIO operation | S3 |
  |---|---|
  | `newByteChannel` / `newInputStream` (read) | `GetObject`, streamed |
  | `newByteChannel` / `newOutputStream` (write, create, truncate) | streamed upload of unknown length by the multipart-enabled async client: one `PutObject` below 16 MiB, multipart above; bounded memory, no local spooling |
  | `createDirectory` | zero-byte `key/` marker object; no-op for an area root |
  | `newDirectoryStream` | `ListObjectsV2` with delimiter `/`, paginated; each entry carries size and last-modified |
  | `readAttributes` | cached listing metadata, else `HeadObject`, else prefix listing (implicit folder) |
  | `exists` / `checkAccess` | as `readAttributes` |
  | `delete` | `DeleteObject`; a folder's own marker |
  | `copy` / `move` | between any readable and any writable path, including across areas (`pipelines` → `files` for `publishDir`): `CopyObject` (write client) when allowed, else a streamed `GetObject` + upload; `move` = copy + delete, a folder object by object |
  | `upload` (`FileSystemTransferAware`) | file or folder, into `pipelines` or `files`; Transfer Manager `uploadFile` (parallel multipart above 16 MiB); other file systems (https, `s3://`) streamed |
  | `download` (`FileSystemTransferAware`) | file or folder; Transfer Manager `downloadFile` into a temp file, moved into place on success |
  | symlinks, locks, `setAttribute` | `UnsupportedOperationException` naming the operation |

- Inputs in `files`/`jobs` are never foreign in direct mode, so Nextflow never stages them; the
  compute node reads them in place through its mount.

### 6.4 `FovusS3Client`

- Two sync `S3Client`s from AWS SDK v2: the reader uses the download token, the writer the
  upload token. HTTP client: `url-connection-client`. They serve listings, `HeadObject`, streamed
  `GetObject`, small `PutObject`s (folder markers), `CopyObject` and `DeleteObject`.
- Two `S3TransferManager`s (D11), one per token, on the Java-based `S3AsyncClient` with multipart
  enabled and the Netty HTTP client (no CRT), built with the same credentials, endpoint and
  settings. They serve file uploads and downloads, and streamed writes of unknown length
  (`AsyncRequestBody.forBlockingOutputStream`). Multipart threshold and part size are 16 MiB; the
  SDK uses larger parts when a file would otherwise need more than 10,000.
- Standard retry mode, up to 10 attempts, matching the CLI's boto settings.
- Checksum calculation and validation set to `WHEN_REQUIRED` (TLS already protects the transfer,
  and it keeps S3-compatible test servers simple).
- Bucket, region and prefix come from the first credential fetch and are fixed for the run.
- The user's own AWS configuration never applies: an explicit endpoint
  `https://s3.<region>.amazonaws.com` (so `AWS_ENDPOINT_URL[_S3]` or a profile's `endpoint_url`
  cannot redirect Fovus-signed requests), an empty profile file, and FIPS and dual-stack off.
- **Access guard:** every key is checked before any call, and a key with a `.` or `..` segment is
  refused.
  - Reads: `Prefix` (`pipelines/<pid>/`), Nextflow's session scratch folders next to it
    (`pipelines/tmp/`, `pipelines/collect-file/`; see §8), `files/` and `jobs/`. Reads elsewhere
    raise `NoSuchFileException`.
  - Writes: the same except `jobs/`. Writes elsewhere are refused before calling S3; a `jobs/`
    write fails as read-only.
- Maps S3 errors to NIO exceptions and messages as described in §10.
- On `ExpiredToken` / `InvalidToken`: force a credential refresh and retry the request, or the
  whole file transfer, once. A streamed write cannot be replayed and fails.
- The SDK aborts a failed multipart upload, best effort: the write token cannot abort (§12), so
  leftover parts stay invisible until the bucket's lifecycle rule removes them.

### 6.5 Credentials in the plugin

- `FovusStorageCredentialsSource.fetch()` returns a `StorageCredentials` value (bucket, region,
  prefix, read and write session credentials with expirations). See §8 for how it runs the CLI.
- `RefreshingStorageCredentials` caches it in a small lock-guarded cache with an injectable clock,
  so the timing rules below are unit-testable:
  - prefetch 10 minutes before expiry; callers block for fresh credentials in the last 2 minutes;
  - thread-safe, single fetch under concurrency;
  - a failed prefetch keeps the current credentials while they have more than 2 minutes left.
- It exposes `readProvider()` and `writeProvider()` (`AwsCredentialsProvider`), both backed by
  the same cached fetch.

## 7. CLI command contract (fovus-cli-python)

```
fovus --silence storage credentials --pipeline-id <pid>
```

- **Hidden:** registered with `hidden=True`, so it is absent from `fovus --help`,
  `fovus storage --help` and the `sphinx_click` docs. No page under `docs/commands/storage/`,
  no README mention. Docstring: internal to nf-fovus, not a supported interface. The command's
  help text says the credentials are for nf-fovus to use with the pipeline's work directory and
  are not limited to it.
- **Steps:**
  1. If `sys.stdout.isatty()`, exit 2 with a message on stderr. No override flag.
  2. Validate the pipeline ID:
     1. Format `p-<digits>-<userId>` and `<userId>` equals the signed-in user (local, before
        any API call).
     2. `get_pipeline(pid)` succeeds in the user's workspace.
     3. Status is not `DELETED`, `DELETING` or `DELETE_FAILED`. `CREATED`, `RUNNING`,
        `COMPLETED`, `FAILED` are allowed (the plugin reuses cached pipelines in these states
        and fetches credentials before setting `RUNNING`).
     4. Workflow host is `LOCAL`. `get_pipeline` always returns `workflowHost` (the server schema
        defaults it to `LOCAL`).
  3. Request both tokens for 3600 s: upload (`FOVUS_STORAGE`, `jobId = ""`) and download
     (`PIPELINE_STORAGE`, `pipelineId`).
  4. Require both responses to name the same bucket.
  5. Write exactly one JSON document to stdout, nothing else:

     ```json
     {
       "Version": 1,
       "Bucket": "fovus-<user>-<workspace>-<region>",
       "Region": "us-east-2",
       "Prefix": "pipelines/<pid>/",
       "Read":  {"AccessKeyId": "…", "SecretAccessKey": "…", "SessionToken": "…", "Expiration": "2026-09-29T18:04:05Z"},
       "Write": {"AccessKeyId": "…", "SecretAccessKey": "…", "SessionToken": "…", "Expiration": "…"}
     }
     ```

- **Region:** `s3Region` from the response, else the CLI's configured region.
- **Expiration:** the API returns none, so it is the time captured just before the token requests
  plus 3600 s (errs early).
- **Not signed in:** the command catches `NotSignedInException` itself, writes it to stderr and
  exits 3. The CLI's `main()` would otherwise print it to stdout.
- **Exit codes:** 0 success (JSON on stdout); 1 validation or API error (stderr); 2 stdout is a
  terminal; 3 not signed in (existing CLI behaviour).
- **Logging:** the command never logs either response. Nothing is printed to stdout except the
  final JSON, including when signed in through `FOVUS_EMAIL` / `FOVUS_PAT`.

## 8. Credential hand-off and secret hygiene

### Channel

The plugin starts the CLI as a subprocess and reads the JSON from its stdout. That stdout is an
anonymous pipe held only by the Nextflow JVM; it never reaches a terminal. The risks are the
places the data could be copied to, and each is closed below.

Alternatives rejected: a temporary file (credentials on disk); a named pipe or extra file
descriptor (no protection beyond the anonymous pipe, and Java cannot pass extra descriptors);
the plugin calling the Fovus API itself (duplicates CLI sign-in and needs its token cache).

### Plugin runner (`FovusStorageCredentialsSource`)

- Its own `ProcessBuilder`, not `FovusUtil.executeCommand`.
- Arguments: `[cliPath, '--silence', 'storage', 'credentials', '--pipeline-id', pid]`. The
  pipeline ID is not secret. `FOVUS_EMAIL` / `FOVUS_PAT` go through the environment
  (`config.cliEnv()`), never arguments.
- stdin closed; stdout read into memory with a 64 KiB cap; 60 s timeout, then the process is
  destroyed.
- The JSON must have `Version == 1`, `Prefix == "pipelines/<pid>/"`, and all fields present.
- stdout is never logged or put in an exception, on success or failure. On failure only stderr
  is used, passed through `config.redactSecret`.
- Exit 3 → "Fovus CLI is not signed in; run `fovus auth login` or configure `fovus.auth`".
  "No such command" → "Direct mode (fovus:// workDir) needs a newer Fovus CLI that provides storage
  credentials; upgrade it with `pip install --upgrade fovus`".
- Three attempts with a 2 s backoff, like other CLI calls.
- Credential holder classes have no `@ToString` / `@Canonical`; `toString()` prints only the
  expiry.
- Debug log on success: "Fetched Fovus storage credentials, expires at <time>". Nothing else.

### Where credentials never go

- command-line arguments
- the task environment, `.command.run`, `.command.fovus.env`, `run.sh`
- the job config JSON sent to Fovus
- Nextflow's session config
- any file on disk
- `.nextflow.log`, the CLI's `~/.fovus/logs/`
- exception messages (S3 errors are reported by error code, HTTP status, request ID and key, never
  the raw S3 error body, which can echo the access key ID)

The plugin never enables AWS SDK request logging.

### Limits of this design

- The hidden command and the pipeline ID checks run in the CLI on the user's machine. A
  determined user can get the same credentials by calling the API with their own sign-in token.
  This is acceptable because the credentials only reach that user's own bucket.
- Neither token is scoped to the pipeline. The read token can read, and the write token can
  write, anywhere in the user's bucket for up to an hour. The plugin's access guard is the only
  thing keeping the plugin inside `pipelines/<pid>/`, `files/` and (read-only) `jobs/`, and a
  leaked credentials document grants that bucket-wide access. A pipeline-scoped backend endpoint
  (§13) would remove the gap, and it is now the main open hardening item.
- Reading `files/` and `jobs/` (D9) relies on the read token reaching the whole bucket. A
  pipeline-scoped endpoint would have to keep read access to those areas, or the CLI command
  would return a second read credential for them.
- Direct mode also reads and writes Nextflow's session scratch folders `pipelines/tmp/` and
  `pipelines/collect-file/` (e.g. `collectFile` without `storeDir`), exactly as mount mode does.

## 9. Lifecycle in direct mode

### Start-up (`FovusExecutor.register()`)

1. Before any executor exists, Nextflow parses `workDir = fovus:///fovus-storage/pipelines`, which
   creates the `pipelines` filesystem without an S3 client, and calls `mkdirs()` on it, which
   succeeds without an S3 call.
2. `FovusExecutor.register()`: `WorkDirStorage.forSession()` sees a `FovusPath` and checks it is
   exactly `/fovus-storage/pipelines` and that the run is not Fovus-hosted.
3. Warm up `fovus.auth` and get or create the pipeline, as today.
4. `DirectWorkDirStorage.prepare()`: first credential fetch, build `FovusS3Client`, attach it to
   the `fovus://` filesystem provider, for every area. No mount.
5. Work directory: `fovus:///fovus-storage/pipelines/<pid>/fovus-work`, from the unchanged
   `getWorkDir()`.
6. `bin/` is uploaded to `…/fovus-work/tmp/<rand>/bin`; `remoteBinDir` is
   `/fovus-storage/pipelines/<pid>/fovus-work/tmp/<rand>/bin`.

### Preparing each task

1. Nextflow picks `…/fovus-work/ab/cdef…`, checks `exists()`, calls `mkdirs()`, which writes the
   `ab/cdef…/` marker. Nextflow's task-folder clash check depends on `exists()` being correct.
2. Foreign inputs (not `fovus://`) are staged by `FilePorter` into
   `…/fovus-work/stage-<sessionId>/…`:
   - local files: provider `upload()` (`PutObject` or parallel multipart);
   - http or the user's own `s3://`: provider `upload()` streams it, and publishes the object only
     once the source was read in full (and matches its size, when known); any failure discards it;
   - already-staged files with the same size are skipped (Nextflow's `FilePorter` compares sizes).
3. Inputs already in Fovus storage (`fovus:///fovus-storage/files|jobs|pipelines/…`) are not
   foreign; they are linked on the compute node with `fovus_link /fovus-storage/…`. Nothing is
   copied. Nextflow still reads their attributes (for `-resume` hashing) and lists folders for
   globs, with `HeadObject` and `ListObjectsV2`.
4. `BashWrapperBuilder` writes `.command.sh`, `.command.run` (and `.command.in` /
   `.command.stage` when needed); `FovusScriptLauncher` writes `.command.fovus.env`. One
   `PutObject` each. Script contents are unchanged, since `FovusFileCopyStrategy` refers to
   files by name only.
5. Array tasks: each child's `run.sh` is one `PutObject`.

### Submitting

- Job directory `task.workDir.parent.toString()` = `/fovus-storage/pipelines/<pid>/fovus-work/ab`.
- `fovus job create cfg.json <that> --pipeline-id <pid> --include-paths cdef…/`, as today.

### Compute node

Unchanged. As in mount mode, the design relies on the job reporting Completed only after Fovus
has synced `/compute_workspace` back to `pipelines/<pid>/fovus-work/ab/cdef…/`. S3 reads are
strongly consistent, so reads after that point see the final objects.

### Completion (`checkIfCompleted()`)

1. Job and task status from CLI polling, unchanged.
2. `readExitFile()` reads `.exitcode` (`GetObject`); missing → `MAX_VALUE`, as today.
3. `path` outputs: `exists()` and glob walks. One `ListObjectsV2` (paginated) per folder level;
   entries carry size and last-modified, so no `HeadObject` per file.
4. `env` / `eval` outputs: `.command.env` (`GetObject`).
5. Trace metrics (`.command.trace`) and error reports (tail of `.command.err`, `.command.log`):
   `GetObject`.

### `publishDir`

- Local target: provider `download()` (Transfer Manager `downloadFile`, temp file then move).
  Folders via listing.
- `symlink` / `link` / `rellink` modes: Nextflow switches remote work directories to `copy` with a
  warning; an unset mode becomes `copy` too.
- `move` mode: the copy succeeds, but the source objects stay in Fovus storage, since the write
  token cannot delete; the plugin warns once and logs further ones at debug level.
- Fovus storage `files/` target (`publishDir 'fovus:///fovus-storage/files/results'`, D10): copied
  inside the bucket, `CopyObject` when allowed, else streamed. With `overwrite` (the default) the
  old object cannot be deleted first (warned once); the new object replaces it. With `mode 'move'`
  the source stays in place, as for a local target. A `jobs/` target fails as read-only.

### `-resume`

- The cache database (`.nextflow/cache/`) stays local; entries hold
  `fovus://fovus-storage/pipelines/<pid>/…` and parse back to `FovusPath`s. They are only read
  when a task is about to run, which is after the executor has attached the S3 client.
- Cached tasks are checked with `exists()`, `.exitcode` and outputs.
- Pipeline ID changed: reads outside the current prefix are "not found", so those tasks re-run.
- Switching between mount and direct mode: old entries are local mount paths, so tasks re-run.

### End of run

- Pipeline status updates unchanged.
- `cleanup = true`: Nextflow skips it for non-local work directories and logs a warning
  (`Session.groovy:1177`), so nothing is deleted. This matches how Nextflow treats `s3://` work
  directories.

## 10. Error handling

Rules: fail fast at start-up; during the run temporary failures delay rather than fail; a missing
object is `NoSuchFileException`; objects are all-or-nothing; errors never carry secrets.

### Start-up (stops the run)

| Failure | Message |
|---|---|
| `fovus://` `workDir` other than `fovus:///fovus-storage/pipelines` | "In direct mode, workDir must be fovus:///fovus-storage/pipelines" |
| `workDir` with any other scheme (for example `s3://`) | "The Fovus executor needs workDir to be a Fovus storage mount (…/pipelines) or fovus:///fovus-storage/pipelines" |
| `fovus://` `workDir` on a Fovus-hosted run | "Direct mode is only for pipelines launched on your own machine; Fovus-hosted runs use the mount" |
| CLI lacks `storage credentials` | "Direct mode (fovus:// workDir) needs a newer Fovus CLI …; upgrade it with `pip install --upgrade fovus`" |
| CLI exit 3 | "Run `fovus auth login` or configure `fovus.auth`" |
| CLI exit 1 | CLI stderr, redacted |
| Malformed JSON, other `Version`, wrong `Prefix`, bucket mismatch | "Unexpected response from Fovus CLI (expected contract v1)"; stdout never included |

### Credential refresh during the run

- Refresh fails with > 2 min left: warning, keep current credentials, retry on the next call.
- Refresh fails with < 2 min left: the S3 call fails with "Unable to refresh Fovus storage
  credentials: <redacted stderr>".
- `ExpiredToken` / `InvalidToken` from S3: force refresh, retry once.
- Pipeline deleted mid-run: refresh fails validation; once credentials expire the run fails
  naming the pipeline and its status.

### S3 calls

| Failure | Behaviour |
|---|---|
| 5xx, `SlowDown`, timeouts | SDK standard retries, up to 10 attempts |
| `NoSuchKey` / 404 | `NoSuchFileException` |
| `AccessDenied` (not expiry) | no retry; "Fovus storage credentials don't allow `<op>` on `<key>` (read/write token)" |
| `AccessDenied` on delete (only `publishDir` with `mode: 'move'` deletes) | warning only; the file is published and the source is left in place |
| Write outside `pipelines/<pid>/`, the scratch folders and `files/` | refused before calling S3 |
| Write into `jobs/` | refused before calling S3: "Fovus storage jobs/ is read-only" |
| Read outside `pipelines/<pid>/`, the scratch folders, `files/` and `jobs/` | `NoSuchFileException` |
| Any access to a `fovus://` path (other than an area root) before the S3 client is attached, or in mount mode | "Fovus storage paths (fovus://) can only be used in direct mode (workDir = 'fovus:///fovus-storage/pipelines'), after the Fovus executor has started. With a Fovus storage mount, use the mounted path instead." |
| Unsupported operation | `UnsupportedOperationException` naming it |

### Where a failure lands

- **Preparing or submitting** (wrapper, `run.sh`, staging): after retries, the task fails and the
  user's `errorStrategy` applies.
- **Checking completion** (`.exitcode`, outputs, `.command.env`): a temporary error makes
  `checkIfCompleted()` return `false`; the next poll (10 s) retries. The task fails only after
  30 consecutive failed polls (about 5 minutes) or when credentials can no longer be refreshed. This
  avoids re-running a task that succeeded. The check reads `.exitcode` and lists the task folder
  once; the output collection Nextflow runs afterwards relies on the SDK's retries (up to 10
  attempts).

### All-or-nothing

- Objects become visible only when `PutObject` / `CompleteMultipartUpload` finishes. A streamed
  write that is discarded (failure, or a remote source that ended early) is cancelled before it
  completes.
- Failed or interrupted multipart uploads are aborted by the SDK, best effort. If abort is not
  permitted, leftover parts stay invisible until the bucket lifecycle rule removes them (spike).
- Downloads go to a temp file, moved on success, deleted on failure.
- Ctrl-C: submitted jobs keep running (`killTask` stays a no-op); in-progress uploads are aborted.

## 11. Testing

### nf-fovus unit tests (Spock, `./gradlew check`, no Docker)

- Mode selection (`WorkDirStorage.forSession`): a local `workDir` gives mount mode;
  `fovus:///fovus-storage/pipelines` gives direct mode; any other `fovus://` `workDir` is rejected;
  a `fovus://` `workDir` on a Fovus-hosted run is rejected.
- `fovus://` filesystems before the S3 client is attached: area roots exist and `mkdirs()` on
  them succeeds with no S3 call; any other read or write raises the clear error, in every area.
- `files/` and `jobs/` read through S3; `jobs/` writes refused; a `publishDir`-style copy and move
  from `pipelines` into `files/`; local uploads into `files/`.
- `MountedWorkDirStorage`: existing tests keep passing; new tests pin today's path rewriting and
  foreign-file rules. `DirectWorkDirStorage`: work directory, foreign-file rule, compute path.
  No `chmod` command is started in either mode.
- `FovusStorageCredentialsSource` against a fake `fovus` script: valid JSON; wrong `Version`,
  `Prefix`, bucket mismatch; exit 1, 2, 3; "No such command"; timeout; stdout over 64 KiB. Each
  case asserts the fixture secrets appear in no log line and no exception message.
- `RefreshingStorageCredentials` with a fake clock: prefetch at T-10 min, blocking in the last
  2 min, failed refresh keeps valid credentials, 50 concurrent callers cause one fetch.
- Credential classes' `toString()` contains no key.
- Access guard: writes outside the writable areas refused, `jobs/` read-only, reads outside the
  readable areas "not found".
- Error mapping with a mocked `S3Client` and Transfer Manager: `ExpiredToken` refresh and single retry;
  `AccessDenied` message and no retry; delete denial is a warning; no raw S3 error body in any
  message.
- `FovusTaskHandler.checkIfCompleted()`: temporary read error defers, fails after the bound;
  missing `.exitcode` gives `MAX_VALUE`. `submit()` passes
  `/fovus-storage/pipelines/<pid>/fovus-work/ab` to `job create`.

### nf-fovus S3 tests (Spock + Testcontainers MinIO, `./gradlew integrationTest`)

Separate Gradle task so `check` needs no Docker. CI (`ubuntu-latest`) has Docker.

- Small write and read back; multipart write at 3× part size through the Transfer Manager;
  `newOutputStream` streaming of unknown length; interrupted write leaves no object.
- Reads from `files/` and `jobs/`; `publishDir`-style copy from `pipelines` into `files/`.
- Folder marker makes `exists()` true after `mkdirs()`; paginated listing (small page size);
  `walkFileTree` with globs; an SDK call-counting interceptor confirms no `HeadObject` per file.
- Upload and download of files and folders; failed download leaves no partial file.
- Copy, move, delete; missing object raises `NoSuchFileException`.
- `Files.size()` of a staged object equals the local file's size, which is what `FilePorter`'s
  re-staging check compares.
- Executor-level flow (`prepareLauncher` → `submit` → `checkIfCompleted`) with a fake job client
  that writes `.exitcode` and outputs into MinIO on `job create`: wrapper upload, staging, exit
  code, output collection, local `publishDir` download.

### fovus-cli-python tests (pytest)

- Hidden: `hidden=True`; absent from `fovus --help` and `fovus storage --help`; no docs page.
- `isatty()` true: exit 2, stdout empty.
- Pipeline ID: bad format; user mismatch; `get_pipeline` 404; each deleted status rejected;
  allowed statuses accepted; `REMOTE` host rejected (if the field exists).
- Requests: upload `FOVUS_STORAGE` with empty `jobId`; download `PIPELINE_STORAGE` with the
  pipeline ID; both 3600 s.
- Output: exactly one JSON document, including with `FOVUS_EMAIL` / `FOVUS_PAT`; `Expiration`
  from the pre-request time; bucket mismatch is an error.
- Secret hygiene: after success and failure, stderr and the `~/.fovus/logs/` DEBUG log contain
  none of the fixture secret values.

### End-to-end acceptance (manual, beta account)

Docker container started without `/dev/fuse` or `SYS_ADMIN`; Fovus CLI signed in with a PAT;
`workDir = 'fovus:///fovus-storage/pipelines'`.

1. Small pipeline with local inputs (one over 100 MiB), an input from
   `fovus:///fovus-storage/files/…`, glob and `env` outputs, an array task, a failing task with
   `errorStrategy 'retry'`, and `publishDir` to a local folder and to
   `fovus:///fovus-storage/files/…`.
2. `-resume` immediately after: every task cached.
3. Ctrl-C mid-run, then `-resume`: no partial objects, correct re-run.
4. One run longer than 70 minutes (credential refresh).
5. The same pipeline in mount mode (no regression).
6. Search `.nextflow.log` and `~/.fovus/logs/` for the access key ID and session token: absent.

## 12. Open questions (spike before implementation)

Run against a beta account first; record answers here before planning.

1. Can the upload token delete objects (`s3:DeleteObject` on `pipelines/<pid>/*`)? Only
   `publishDir` with `mode: 'move'` needs it, since Nextflow skips `cleanup` for remote work
   directories. Expected from fovus-infra's code: denied (the upload role has only
   `s3:PutObject`); the plugin's fallbacks cover it. Confirm with the spike.
2. Can the upload token abort a multipart upload? Does the bucket have a lifecycle rule for
   incomplete multipart uploads? Expected from fovus-infra's code: denied (the upload role has
   only `s3:PutObject`); the plugin's fallbacks cover it. Confirm with the spike.
3. ~~Does the `get_pipeline` response include `workflowHost`?~~ Answered: yes, always. The server's
   `PipelineSchema` defaults `workflowHost` to `LOCAL`.
4. ~~Does `fovus job create --pipeline-id` accept a job directory that does not exist locally?~~
   Answered by code reading: for pipeline jobs the CLI only splits the path text
   (`FileUtil.get_run_folders_for_pipeline_jobs`) and skips its cache file when absent. Confirmed
   again by the end-to-end run.
5. Can the upload token `CopyObject` within `pipelines/<pid>/` (it needs read access to the
   source)? If not, `copy` uses `GetObject` with the read client and `PutObject` with the write
   client. Expected from fovus-infra's code: denied (the upload role has only `s3:PutObject`);
   the plugin's fallbacks cover it. Confirm with the spike.
6. Confirm the read token can `ListObjectsV2` and `GetObject` outside `pipelines/<pid>/`
   (fovus-infra's code says yes: the download role reads the whole bucket). Direct mode relies on
   it for `files/` and `jobs/` inputs (D9).
7. Confirm the upload token can `PutObject` and multipart-upload into `files/` (the CLI's
   `fovus storage upload` does). Direct mode relies on it for `publishDir` into `files/` (D10).

Also confirm while there: the download token (`PIPELINE_STORAGE`) allows `ListObjectsV2` and
`GetObject` under `pipelines/<pid>/`, and the upload token allows `PutObject` and multipart there.

## 13. Out of scope and follow-ups

- **Pipeline-scoped backend endpoint:** one read/write credential limited to
  `pipelines/<pid>/`. This is the main hardening follow-up, because both current tokens are
  bucket-wide. It would also need read access to `files/` and `jobs/` and write access to `files/`
  (D9, D10), or separate credentials for them. The plugin would only need
  `FovusStorageCredentialsSource` and the access guard to change.
- **`fovus://` paths in mount mode:** map them to the mounted path instead of failing.
- **Resume across a mode switch:** map mount-mode cache paths to `fovus://` paths.

## 14. Compatibility and rollout

- Existing pipelines use a local `workDir` and are unaffected. Direct mode is opt-in by setting a
  `fovus://` `workDir`.
- Direct mode needs the Fovus CLI release that adds `storage credentials`. When the command is
  missing the plugin tells the user to run `pip install --upgrade fovus`; the README names the
  first CLI release that includes it once that release exists.
- `fovus://` inputs in mount mode, an undocumented use of the removed CLI-backed filesystem, stop
  working (D9); the error names the mounted path as the replacement.
- New nf-fovus dependencies: `software.amazon.awssdk:s3`, `s3-transfer-manager`,
  `url-connection-client` and `netty-nio-client`, bundled in the plugin (isolated by the plugin
  classloader from any `nf-amazon` copy).
- Release order: CLI first, then nf-fovus.
