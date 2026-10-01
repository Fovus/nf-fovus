# Direct mode review changes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the decisions from the review of nf-fovus #78: one S3-backed storage for every `fovus://` area (D9), writes into Fovus storage `files/` (D10), and file transfers through the AWS SDK's S3 Transfer Manager (D11).

**Architecture:** `PipelinesStorage` becomes the area-agnostic `S3Storage`, attached once to the `fovus://` filesystem provider and used for `pipelines`, `files` and `jobs`. The CLI-backed `files`/`jobs` filesystem is deleted. `FovusS3Client` keeps its sync clients for metadata and small objects and its access guard (now per area), and delegates file transfers and streamed writes to a new `S3Transfers` seam implemented with `S3TransferManager` on the multipart-enabled Java async client (Netty).

**Tech Stack:** Groovy 4 `@CompileStatic` + Java (joint compilation in `src/main/groovy`), Nextflow 25.10.0 plugin, AWS SDK for Java v2 2.31.0 (`s3`, `s3-transfer-manager`, `url-connection-client`, `netty-nio-client`), Spock 2.3, Gradle 8.14.

**Spec:** `docs/specs/2026-09-30-direct-storage-mode-design.md` (D9–D11, §6.3, §6.4, §9 `publishDir`, §10, §11). The earlier plan `docs/plans/2026-09-30-direct-mode-nf-fovus.md` built what this plan changes.

## Global Constraints

- Credentials never reach command-line args, the task environment, any file on disk, `.nextflow.log` or exception messages. S3 errors are reported by code, HTTP status, request ID and key only, never the S3 error body. Do not enable SDK request logging.
- The user's AWS configuration never applies to Fovus clients: explicit endpoint `https://s3.<region>.amazonaws.com`, empty profile file, FIPS and dual-stack off, standard retry strategy with 10 attempts, checksum calculation and validation `WHEN_REQUIRED`. This applies to the new async clients exactly as to the sync ones.
- Access guard (spec §6.4). Reads: `pipelines/<pid>/`, `pipelines/tmp/`, `pipelines/collect-file/`, `files/`, `jobs/`. Writes: the same minus `jobs/`. Keys with a `.` or `..` segment are refused. Reads elsewhere: `NoSuchFileException` (or "absent"), no S3 call. Writes elsewhere: `AccessDeniedException` before any S3 call; `jobs/` writes say `Fovus storage jobs/ is read-only`.
- Unattached message (spec §10), exactly: `Fovus storage paths (fovus://) can only be used in direct mode (workDir = 'fovus:///fovus-storage/pipelines'), after the Fovus executor has started. With a Fovus storage mount, use the mounted path instead.`
- Mount mode behaviour is otherwise unchanged. Fovus-hosted runs are unchanged.
- `./gradlew cleanTest test` must stay green (unit tests, no Docker). Integration tests (`@Tag('integration')`, MinIO) are updated to match but are not run: there is no MinIO image on this machine (`FOVUS_MINIO_IMAGE` selects one; the user runs them).
- Match the surrounding code: `@CompileStatic` Groovy, `[FOVUS]` log prefix, Spock `def 'sentence'()` test names, comment density as in the files you touch.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A `fovus:///fovus-storage/files/...` input in direct mode: `exists`, `readAttributes` (what `-resume` hashing uses), and a glob over its folder must all work through S3, with the folder listing at the area root (`files/`, never `files//`).
2. `publishDir 'fovus:///fovus-storage/files/results', mode: 'copy'` for a file and for a folder output: same-provider copy across two `FovusFileSystem`s (pipelines → files), with the denied `CopyObject` falling back to a streamed copy. `mode: 'move'` of a folder output must copy every object, then leave the source with one warning.
3. Mount mode with a `fovus://` input: the first access fails with the unattached message, not a `NullPointerException` or a CLI error.
4. A streamed write that is abandoned (remote source ends early, exception while writing) must not publish an object: the Transfer Manager–based stream is cancelled, not completed.
5. A failed or interrupted file download leaves no file and no temp file; a successful one has default permissions (not the 0600 of a temp file).

---

### Task 1: One S3 storage for every `fovus://` area (reads), CLI-backed filesystem removed

**Files:**
- Rename (git mv): `src/main/groovy/fovus/plugin/nio/PipelinesStorage.groovy` → `src/main/groovy/fovus/plugin/nio/S3Storage.groovy` (class `S3Storage`)
- Rename (git mv): `src/test/groovy/fovus/plugin/nio/PipelinesStorageTest.groovy` → `S3StorageTest.groovy`, `PipelinesStorageIT.groovy` → `S3StorageIT.groovy`, `PipelinesTestSupport.groovy` → `StorageTestSupport.groovy`, `FovusPipelinesAreaTest.groovy` → `FovusStorageAreasTest.groovy`
- Modify: `src/main/groovy/fovus/plugin/nio/FovusFileSystemProvider.java`, `FovusFileSystem.java`, `FovusPath.java`
- Modify: `src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy` (read guard only)
- Modify: `src/main/groovy/fovus/plugin/storage/DirectWorkDirStorage.groovy`, and any other main/test file referencing the renamed or removed members (`grep -rn "PipelinesStorage\|attachS3Client\|hasS3Client\|pipelinesStorage\|isPipelinesAreaRoot\|getJobClient\|NOT_ATTACHED_MESSAGE" src`)
- Delete: `src/main/groovy/fovus/plugin/util/FovusFileMetadataLookup.java`, `src/main/groovy/fovus/plugin/nio/FovusPathIterator.java`
- Modify: `src/main/groovy/fovus/plugin/job/FovusJobClient.groovy` — delete `downloadFile`, `getJobFileDownloadCommand`, `getStorageFileDownloadCommand`, `listFileObjects` and any other method only they used (check with grep; keep everything job submission and status use)

**Interfaces:**
- Produces (used by Tasks 2–3):
  - `class S3Storage` (package `fovus.plugin.nio`), same public methods as `PipelinesStorage` today, plus `static String keyOf(FovusPath path)` returning `<area>` for an area root and `<area>/<key>` otherwise.
  - `FovusFileSystemProvider`: `public void attachS3Client(FovusS3Client client)`, `public boolean hasS3Client()`, package-private `S3Storage storage()` (throws `IllegalStateException(NOT_ATTACHED_MESSAGE)` when none is attached), `public static final String NOT_ATTACHED_MESSAGE` (the exact text in Global Constraints).
  - `FovusFileSystem(FovusFileSystemProvider provider, URI uri)` — no job client, no attach methods; `isReadOnly()` is true only for `jobs`.
  - `FovusPath.isAreaRoot()` — true for `/fovus-storage/<area>` itself, any area. Replaces `isPipelinesAreaRoot()` everywhere.

**Behaviour:**
- `newFileSystem(uri, env)`: every area gets `new FovusFileSystem(this, uri)`; `FovusConfig` is no longer read here.
- Every NIO operation of the provider goes to `storage()` for every area. The old CLI-backed code paths (`readAttr0`, `readAttr1`, `createFileSystem`, the CLI `download`, `FovusPathIterator`, `fovusFileMetadataLookup`) are deleted.
- Area roots, any area, without an S3 call and without an attached client: `exists` → true; `readAttributes` → a folder; `checkAccess` → ok; `createDirectory` → no-op; `newDirectoryStream` on an area root does need the client and lists `<area>/`.
- `FovusS3Client` read guard: add `files/` and `jobs/` to the readable folders. Writes stay exactly as today in this task (Task 2 opens `files/`).
- `DirectWorkDirStorage.prepare()` attaches to `((FovusFileSystemProvider) workDir.getFileSystem().provider())`; idempotence and the start-up probe are unchanged.
- Mount mode needs no code change: a `fovus://` input reaches the provider, which throws the unattached message.

- [ ] **Step 1: Write the failing tests** (Spock, `FovusStorageAreasTest`, `S3StorageTest`, `FovusS3ClientTest`, `DirectWorkDirStorageTest`; mock `S3Client` as the existing tests do)

  - `def 'every area root should exist and be a folder before any client is attached'()` — for `files`, `jobs`, `pipelines`: `Files.exists`, `Files.isDirectory`, `Files.createDirectories` succeed with no S3 interaction.
  - `def 'any other access before the S3 client is attached should explain direct mode'()` — `where: area << ['files', 'jobs', 'pipelines']`; `Files.exists(path)` is false or throws, and `Files.readAttributes`, `Files.newInputStream`, `Files.newDirectoryStream(parent)` throw `IllegalStateException` whose message is exactly `FovusFileSystemProvider.NOT_ATTACHED_MESSAGE`. (Keep the semantics `exists` has today for pipelines paths before attach; assert what it does, consistently for all areas.)
  - `def 'a files/ input should be read through S3'()` — attach a client over a mocked `S3Client`; `Files.size(fovus:///fovus-storage/files/data/in.txt)` sends one `HeadObject` for key `files/data/in.txt`; `Files.newInputStream` sends `GetObject` for the same key.
  - `def 'a jobs/ output should be read through S3'()` — same for `jobs/<jobId>/out.txt`.
  - `def 'listing the files area root should list files/ once'()` — `Files.newDirectoryStream(fovus:///fovus-storage/files)` sends `ListObjectsV2` with prefix `files/` (never `files//`) and returns the children.
  - `def 'a glob over a files/ folder should not look up each file'()` — two-level listing; no `HeadObject` per entry (mirror the existing pipelines glob test).
  - `FovusS3ClientTest`: reads of `files/x` and `jobs/j-1/x` reach S3; reads of `pipelines/p-2-user/x` still look missing with no S3 call; writes of `files/x` and `jobs/j-1/x` are still refused in this task. Update `NEAR_SCRATCH_KEYS`, which today lists `files/tmp/x` as unreadable.
  - `DirectWorkDirStorageTest`: prepare attaches to the provider; a second prepare does not fetch again.
  - `def 'the file systems should hold no CLI job client'()` is not needed; the deleted classes are proven by compilation.

- [ ] **Step 2: Run them and see them fail**

  Run: `./gradlew test --tests 'fovus.plugin.nio.*' --tests 'fovus.plugin.s3.FovusS3ClientTest' --tests 'fovus.plugin.storage.*'`
  Expected: compilation errors or failures naming the new members.

- [ ] **Step 3: Implement** as described under Behaviour. Notes:
  - `S3Storage.keyOf`:

    ```groovy
    /** The S3 key of a path: {@code <area>} for an area root, else {@code <area>/<key>}, the object the mount shows at {@code /fovus-storage/<area>/<key>}. */
    static String keyOf(FovusPath path) {
        final key = path.getKey()
        return key.isEmpty() ? path.getFileType() : path.getFileType() + '/' + key
    }
    ```
  - Update `S3Storage`'s class comment: it serves every area; `jobs/` is read-only by the client's guard.
  - In `FovusFileSystemProvider`, keep one `private static FovusPath fovus(Path)` style helper rather than repeating casts; keep the Javadoc at the top of the class accurate (all areas through `S3Storage`, direct mode only).
  - `FovusFileSystemProvider.storage()` reads a `volatile S3Storage` field.

- [ ] **Step 4: Run the full unit suite**

  Run: `./gradlew cleanTest test`
  Expected: PASS, no test skipped that passed before. Update the renamed integration tests so they compile (`./gradlew compileTestGroovy`), without running them.

- [ ] **Step 5: Commit**

  `git commit -m "Serve every fovus:// area from one S3 storage and remove the CLI-backed file system"` (+ trailer)

### Task 2: Writes into Fovus storage `files/`, copies and moves across areas

**Files:**
- Modify: `src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy` (write guard)
- Modify: `src/main/groovy/fovus/plugin/nio/FovusFileSystemProvider.java` (`canUpload`, `copy`, `move`, `checkAccess`)
- Modify: `src/main/groovy/fovus/plugin/nio/S3Storage.groovy` (`move` of a folder)
- Test: `src/test/groovy/fovus/plugin/nio/FovusStorageAreasTest.groovy`, `S3StorageTest.groovy`, `ForeignSourceUploadTest.groovy`, `src/test/groovy/fovus/plugin/s3/FovusS3ClientTest.groovy`, `S3StorageIT.groovy` (compile only)

**Interfaces:**
- Consumes: Task 1's `S3Storage`, `FovusFileSystemProvider.storage()`, `FovusPath.isAreaRoot()`.
- Produces: no new public names.

**Behaviour:**
- Write guard: writable folders are the pipeline folder, the two scratch folders and `files/`. A `jobs/` key → `AccessDeniedException(uri(key), null, 'Fovus storage jobs/ is read-only')`. Any other key → `AccessDeniedException(uri(key), null, "Refusing to write outside ${prefix}, the session scratch folders and files/")`. Dot segments refused as today. Update the class comment.
- `canUpload(source, target)`: true when `target` is a `FovusPath` outside `jobs`, for a source on any file system (the streamed upload handles https/s3). `canDownload` unchanged.
- `copy` / `move` between two `FovusPath`s go to `storage()` whatever their areas (e.g. `pipelines` → `files`, as `publishDir` does through `FileHelper.copyPath`/`movePath` because both paths share this provider). The source must be readable and the target writable — the client's guard already enforces both.
- `S3Storage.move` of a folder: copy every object under it (`listAll`, keeping relative keys and folder markers), then delete the source objects; a denied delete warns once, as `delete` does. A file move is unchanged (copy, then delete).
- `checkAccess(path, AccessMode.WRITE)` on a `jobs` path throws `AccessDeniedException` with the read-only reason; other modes and areas as before.

- [ ] **Step 1: Write the failing tests**

  - `def 'publishDir into files/ should copy a pipeline output across areas'()` — attach a client over a mocked `S3Client` whose `copyObject` throws 403; `FileHelper.copyPath(pipelines/.../out.txt, files/results/out.txt)` falls back to `GetObject` on `pipelines/p-1-user/...` plus an upload to `files/results/out.txt`. (With Task 3 not done yet the upload is the current `PutObject`; assert on the key and body, not the request type.)
  - `def 'publishDir into files/ should copy a folder output object by object'()` — `FileHelper.copyPath` of a folder: target marker and each file land under `files/results/`.
  - `def 'a move of a folder into files/ should copy every object and leave the source with one warning'()` — `deleteObject` throws 403; every object copied; exactly one WARN log line naming the left-in-place path (capture logs as `S3StorageTest` does for the denied delete).
  - `def 'a local file and folder should upload into files/'()` — `FileHelper.copyPath(localFile, fovus:///fovus-storage/files/in/x.txt)` and the same for a folder.
  - `def 'writes into jobs/ should be refused as read-only before calling S3'()` — `where:` newOutputStream, createDirectory, upload, copy target, move target, delete; message contains `Fovus storage jobs/ is read-only`; zero S3 interactions.
  - `def 'canUpload should accept pipelines and files targets from any file system, and refuse jobs'()` — matrix over source FS (default, a mocked `https` path) × target area.
  - `FovusS3ClientTest`: `files/x` writes reach S3; `jobs/x` writes refused with the read-only message; `pipelines/p-2-user/x` writes refused with the new outside message. Update the parametrized lists.
  - `ForeignSourceUploadTest`: replace `'the pipelines area should take uploads from any file system; files/ keeps its current rule'` with the new rule (pipelines and files accept any source; jobs refuses).

- [ ] **Step 2: Run them and see them fail**

  Run: `./gradlew test --tests 'fovus.plugin.nio.*' --tests 'fovus.plugin.s3.FovusS3ClientTest'`

- [ ] **Step 3: Implement** as described under Behaviour.

- [ ] **Step 4: Run the full unit suite**

  Run: `./gradlew cleanTest test` — PASS. `./gradlew compileTestGroovy` for the ITs; add one IT case to `S3StorageIT` (not run): a file and a folder copied from `pipelines/<pid>/` into `files/` read back.

- [ ] **Step 5: Commit**

  `git commit -m "Write into Fovus storage files/ in direct mode and copy across storage areas"` (+ trailer)

### Task 3: File transfers and streamed writes through the S3 Transfer Manager

**Files:**
- Modify: `build.gradle`
- Create: `src/main/groovy/fovus/plugin/s3/S3Transfers.groovy` (interface), `src/main/groovy/fovus/plugin/s3/S3UploadStream.groovy` (abstract `OutputStream` with `abort()`), `src/main/groovy/fovus/plugin/s3/TransferManagerTransfers.groovy`
- Modify: `src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy`, `S3WriteChannel.groovy` (comment), `src/main/groovy/fovus/plugin/nio/S3Storage.groovy` (stream type)
- Delete: `src/main/groovy/fovus/plugin/s3/S3MultipartOutputStream.groovy`, `src/main/groovy/fovus/plugin/s3/FileSliceProvider.groovy`
- Test: rewrite `src/test/groovy/fovus/plugin/s3/S3TransferTest.groovy` and `S3TransferFailureTest.groovy` against a mocked `S3Transfers`; create `src/test/groovy/fovus/plugin/s3/TransferManagerTransfersTest.groovy`; extend `FovusS3ClientEndpointTest.groovy` to the async clients; update `FovusS3ClientIT.groovy` and `MinioSupport.groovy` so they compile (not run)

**Interfaces:**
- Consumes: Task 2's guard (`writable`, `readable`) and `call()` / `mapError()` in `FovusS3Client`.
- Produces:

  ```groovy
  package fovus.plugin.s3

  /** File transfers and streamed writes for one bucket; implemented with the SDK's S3 Transfer Manager. */
  interface S3Transfers extends Closeable {
      /** Upload a local file to {@code key} (multipart above the threshold). SDK failures are thrown unwrapped. */
      void uploadFile(Path file, String key) throws IOException
      /** Download {@code key} to {@code destination}, which the caller owns (a temp file). SDK failures are thrown unwrapped. */
      void downloadFile(String key, Path destination) throws IOException
      /** A stream whose bytes become the object at {@code key} only when it is closed without error. */
      S3UploadStream newUploadStream(String key) throws IOException
  }

  /** An upload in progress: {@link #close()} publishes the object, {@link #abort()} discards it. */
  abstract class S3UploadStream extends OutputStream {
      /** Discard the upload; nothing becomes visible. A no-op once {@link #close()} has run. */
      abstract void abort()
  }
  ```

  - `FovusS3Client(S3Client reader, S3Client writer, S3Transfers transfers, String bucket, String prefix, RefreshingStorageCredentials credentials, int listPageSize = DEFAULT_LIST_PAGE_SIZE)` — the part size and the transfer pool move out of this class.
  - `FovusS3Client.newOutputStream(String key)` returns `S3UploadStream`.
  - `FovusS3Client.close()` closes the transfers (tests and an orderly shutdown).
  - `TransferManagerTransfers.create(String bucket, String region, AwsCredentialsProvider read, AwsCredentialsProvider write, List<ExecutionInterceptor> interceptors, long partSize = 16 MiB)` and a package-scope constructor taking `S3TransferManager reader, S3TransferManager writer, S3AsyncClient writerClient, String bucket` for tests.

**Behaviour:**
- `build.gradle`: add `software.amazon.awssdk:s3-transfer-manager:2.31.0` and `software.amazon.awssdk:netty-nio-client:2.31.0`; keep `apache-client` excluded everywhere (also from the transfer manager's transitive `s3`); keep `url-connection-client`. Update the comment above the SDK block (sync client over HttpURLConnection for metadata; Netty for the async multipart client; no Apache, no CRT). Report the plugin zip size before and after (`./gradlew assemble`, `ls -l build/distributions`).
- Async clients (`TransferManagerTransfers.create`): `S3AsyncClient.builder()` with the same region, explicit endpoint, FIPS/dual-stack off, credentials provider, checksum settings, retry strategy (10 attempts), empty profile file and interceptors as `FovusS3Client.buildClient` — extract the shared override configuration into one helper so the two builders cannot drift; `.multipartEnabled(true)`, `.multipartConfiguration(thresholdInBytes = partSize, minimumPartSizeInBytes = partSize)`; `httpClientBuilder(NettyNioAsyncHttpClient.builder().connectionTimeout(30 s).readTimeout(5 min).writeTimeout(5 min))`. One `S3TransferManager` per client (`S3TransferManager.builder().s3Client(client).build()`): the reader's for downloads, the writer's for uploads.
- `uploadFile` → `writer.uploadFile(UploadFileRequest(putObjectRequest bucket/key, source file))`; `downloadFile` → `reader.downloadFile(DownloadFileRequest(getObjectRequest bucket/key, destination))`. Wait with `future.get()`: on `InterruptedException` cancel the transfer, keep the interrupt flag, throw `InterruptedIOException`; on `ExecutionException`/`CompletionException` unwrap to the first non-`CompletionException` cause and rethrow it if it is an `SdkException` or `IOException`, else wrap in `IOException` naming the class only.
- `newUploadStream(key)`: `BlockingOutputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingOutputStream(null)` (unknown length), `future = writerClient.putObject(PutObjectRequest(bucket, key), body)`, and a stream that writes to `body.outputStream()`. `close()`: close the body stream, then wait for `future` as above and rethrow mapped. `abort()`: `body.outputStream().cancel()` and `future.cancel(true)`; idempotent; a no-op after `close()`. A write that fails because the upload already failed throws an `IOException` carrying the upload's failure (unwrapped), never a raw `IllegalStateException`. Verify in `TransferManagerTransfersTest` with a mocked `S3AsyncClient` whose `putObject` future fails, and check the SDK's actual behaviour of `forBlockingOutputStream(null)` against the 2.31.0 jar (unknown length is required for the multipart client to split the stream; confirm `contentLength()` is empty).
- `FovusS3Client`:
  - `uploadFile(file, key)`: `writable(key)`, then `call('write', key) { transfers.uploadFile(file, key) }` (an expired token refreshes and retries the whole upload once).
  - `downloadFile(key, target)`: as today — `head`, `NoSuchFileException`, temp file `.<name>.<uuid>.part` next to the target created with `Files.createFile` (default permissions), `call('read', key) { transfers.downloadFile(key, temp) }`, move into place, delete the temp file on any failure.
  - `newOutputStream(key)`: `writable(key)` then `transfers.newUploadStream(key)`; failures from `close()` are mapped by `mapError` (wrap the transfers' stream in a small mapping stream, or map inside `TransferManagerTransfers` by passing a mapper — one place only).
  - `copy`: unchanged logic (CopyObject when allowed, else streamed `getObject` → `newOutputStream`, `abort()` in `finally`).
  - Delete: `createMultipart`, `uploadPart`, `completeMultipart`, `abortMultipart`, `uploadFilePart`, `uploadPartSize`, `downloadRanges`, `readRange`, `cancel`, `await`, the transfer pool, `shutdownTransfers`, `MAX_PARTS`, `TRANSFER_THREADS`, `MIN_PART_SIZE`, `DEFAULT_PART_SIZE` (moves to `TransferManagerTransfers`), and the `partSize` field. `S3ReadChannel` and `getObject(key, fromByte)` stay.
  - `create()` / `createWithInterceptors()` build the sync clients and `TransferManagerTransfers.create(...)`.
- `S3Storage.uploadFile` for a foreign source keeps its rule (stream, compare with the known size, `abort()` unless complete) on the new stream type.
- Threads: Netty's event loop and the SDK's async response threads are daemon threads in SDK 2.31.0; assert it in `TransferManagerTransfersTest` with a real client built against `http://localhost:1` (no request needed if the event loop starts on build; if it starts lazily, document that in a comment instead of asserting). Nextflow ends the JVM with `System.exit`, so no shutdown hook is added.

- [ ] **Step 1: Write the failing tests**

  `S3TransferTest` / `S3TransferFailureTest` (mock `S3Transfers`, mock `S3Client`):
  - `def 'uploadFile should hand the file to the transfer manager under the guard'()`.
  - `def 'an expired token during a file upload should refresh and retry the upload once'()`.
  - `def 'a download should land in a temp file next to the target and move into place'()` — assert the destination passed to `transfers.downloadFile` is a sibling `.part` file and the final file has the content.
  - `def 'a failed download should leave no file and no temp file behind'()` and `def 'an interrupted download should leave no file behind'()`.
  - `def 'a #kind download should get the default permissions rather than the owner-only mode of a temp file'()` (keep the existing case).
  - `def 'a denied CopyObject should fall back to a streamed copy'()`, `def 'a CopyObject failure other than access denied should not fall back'()`, `def 'a streamed copy whose source fails part way should abort the upload'()` — `abort()` called on the `S3UploadStream`, `close()` not called.
  - Guard cases kept: `newOutputStream`, `uploadFile`, `copy` target refuse writes outside the writable areas before any call; `downloadFile` and `copy` source treat unreadable keys as missing.

  `TransferManagerTransfersTest` (mocks of `S3TransferManager`, `S3AsyncClient`, `FileUpload`, `FileDownload`):
  - requests carry the bucket, key and file;
  - a future failing with `CompletionException(S3Exception)` rethrows the `S3Exception`;
  - an interrupted wait cancels the future and throws `InterruptedIOException`;
  - the upload stream: `close()` waits for the `putObject` future and rethrows its failure; `abort()` cancels and makes a later `close()` a no-op; a write after the upload failed throws `IOException` with the upload's failure;
  - the request body passed to `putObject` has no content length.

  `FovusS3ClientEndpointTest`: the async clients use the explicit endpoint and ignore `AWS_ENDPOINT_URL_S3` / a profile `endpoint_url` / `AWS_USE_FIPS_ENDPOINT`, mirroring the sync cases (reuse the existing technique).

- [ ] **Step 2: Run them and see them fail**

  Run: `./gradlew test --tests 'fovus.plugin.s3.*'`

- [ ] **Step 3: Implement** as described under Behaviour. Update `S3BodyStream`/`S3ReadChannel` comments only if they mention the removed classes.

- [ ] **Step 4: Run the full unit suite and the build**

  Run: `./gradlew cleanTest test assemble` — PASS. Update `FovusS3ClientIT` (multipart upload at 3 × 5 MiB parts through `TransferManagerTransfers.create(..., partSize = 5 MiB)` against MinIO, streamed `newOutputStream` of ~11 MiB of unknown length, abort leaves no object) and `MinioSupport` so `./gradlew compileTestGroovy` passes; do not run them.

- [ ] **Step 5: Commit**

  `git commit -m "Transfer files through the S3 Transfer Manager instead of our own multipart code"` (+ trailer)

### Task 4: Documentation

**Files:**
- Modify: `README.md` (Direct mode section)
- Modify: `docs/plans/2026-09-30-direct-mode-nf-fovus.md` (Task 11 E2E checklist only)

**Behaviour:**
- README, Direct mode:
  - Inputs already in Fovus storage: `fovus:///fovus-storage/files/...` and `fovus:///fovus-storage/jobs/<jobId>/...` are read in place (not copied); globs work.
  - `publishDir` can target `fovus:///fovus-storage/files/...`; with `overwrite` the old object is replaced; `mode 'move'` leaves the source in the work directory (the credentials cannot delete), with a warning; `jobs/` is read-only.
  - `fovus://` paths work only in direct mode; with a mount, use the mounted path.
  - Dependencies note if the README lists them; the "Tests" section stays accurate (integration tests need `FOVUS_MINIO_IMAGE`).
- E2E checklist (Task 11 of the earlier plan): add `publishDir` to `fovus:///fovus-storage/files/<test-folder>` with `mode 'copy'` for a file and a folder output, and with `mode 'move'`; an input read from `fovus:///fovus-storage/jobs/<jobId>/...`; a mount-mode run with a `fovus://` input shows the direct-mode message; a file over 100 MiB published to a local folder (Transfer Manager download).
- Keep the README's tone and length; no mention of the hidden CLI credentials command.

- [ ] **Step 1: Edit the two files.**
- [ ] **Step 2: Check every statement against the code from Tasks 1–3** (messages quoted exactly).
- [ ] **Step 3: Commit** — `git commit -m "Document fovus:// inputs, publishing into files/ and the Transfer Manager in direct mode"` (+ trailer)
