---
topic: ext-closure-resolution
status: decided — option A
nextflow_version: 25.10.0 (sources jar); experiments run on 25.10.4
---

# Resolving dynamic `ext` values before tasks run — research

## The problem in one paragraph

`FovusTraceObserver.onFlowCreate` (src/main/groovy/fovus/plugin/observers/FovusTraceObserver.groovy:49-64)
walks the raw `session.config.process` map (`collectResourceConfigurations`, :72-115) and turns
each `ext` block into a `ResourceConfiguration` (`parseExtensionObject`, :153+). A config value
written as a closure (`ext.benchmarkingProfileName = { params.x ? 'A' : 'B' }`) is a plain
`groovy.lang.Closure` in that map. The `ResourceConfiguration` map constructor turns it into its
`toString()` (`_nf_config_…$_run_closure6$_closure22$_closure38@…`). Numeric fields guarded by
`instanceof Number` (:186-255) silently drop the closure. At job submission the same values are
read through `task.config.get('ext')` (src/main/groovy/fovus/plugin/job/FovusJobConfig.groovy:92-99),
which resolves closures, so jobs are right and only the pre-configuration is wrong.

Two side effects make it worse:
- The backend checks every `benchmarkingProfileName` and rejects the **whole** request when one
  cannot be mapped (fovus-infra `applications/pipeline-service/apis/pre-config-resources.ts:98-115`,
  `modules/update-pre-config-environment-payload.ts:20-27`). One bad entry cancels pre-config
  for every process.
- The plugin logs that failure at `trace` level only (FovusTraceObserver.groovy:61), so users
  never see it.

Source paths below are relative to `modules/nextflow/src/main/groovy/` in
https://github.com/nextflow-io/nextflow/tree/v25.10.0 (the same files are in the 25.10.0 sources
jar). Plugin paths are relative to the repo root at the same tag.

---

## 1. How `task.ext.X` closures are resolved at task time

**Chain.** `ProcessConfig.createTaskConfig()` copies the merged process config into a new
`TaskConfig` (nextflow/script/ProcessConfig.groovy:154-156). `TaskConfig extends LazyMap`
(nextflow/processor/TaskConfig.groovy:45). Putting `ext` as a Map wraps it in its own `LazyMap`
(TaskConfig.groovy:172-173), and `get('ext')` returns that LazyMap (:139-157).

**Context.** `TaskConfig.setContext(Map)` stores the context as the binding of the TaskConfig
**and** of the `ext` LazyMap, and puts `task` → the TaskConfig itself into the context
(TaskConfig.groovy:73-90). TaskProcessor sets it once the task inputs are bound:
`task.config.context = task.context` (nextflow/processor/TaskProcessor.groovy:1924).

**Evaluation.** `LazyMap.get(key)` → `resolve` → `resolveImpl` (TaskConfig.groovy:628-711):
- a `Closure` is cloned with `cloneWith(binding)` and called (:691-704);
- a `GString` with closures is made lazy against the binding (:706-707);
- a nested `Map` value under a key other than `ext` has its closures **cloned, not called**
  (`resolveParams`, :651-653, :671-679). Closures nested inside `ext.someMap` are not resolved.

`cloneWith` (nf-commons `nextflow/extension/Bolts.groovy:697-706`,
https://github.com/nextflow-io/nextflow/blob/v25.10.0/modules/nf-commons/src/main/nextflow/extension/Bolts.groovy#L697-L706)
clones the closure, sets the delegate to the context and the strategy to `DELEGATE_FIRST`.

**What is in scope.** The delegate is a `TaskContext` (nextflow/processor/TaskContext.groovy):
- properties: the holder map first (task inputs such as `meta`, plus `task`), then the main
  script binding (`params`, globals) (:132-146). Unknown names throw `MissingPropertyException`
  (:144, :171-173), so Groovy falls back to the closure owner (the config script).
- methods: forwarded to the **main script** (:163-168). If that fails, Groovy falls back to the
  owner, which is how helper functions defined in a v1 config file (`def tier() {…}`) resolve.
- `task.attempt` is `get('attempt') ?: 1` (TaskConfig.groovy:489-491), so it is `1` even before
  any retry sets it.

**When a closure throws.** The exception comes out of `LazyMap.get`. With a `null` binding a
`MissingPropertyException` becomes `IllegalStateException("Directive … doesn't support dynamic
value (or context not yet initialized)")` (TaskConfig.groovy:700-702). In nf-fovus the read
happens in `submit()`; `TaskPollingMonitor.submitPendingTasks` catches it and calls
`handleException` → `TaskProcessor.resumeOrDie` (nextflow/processor/TaskPollingMonitor.groovy:646-671,
674+). Retry/ignore strategies only apply to `ProcessException` (TaskProcessor.groovy:1091);
anything else keeps the default `TERMINATE` (:1054), so the run stops.

## 2. How config closures are stored in `session.config`

**Both parsers store a plain `groovy.lang.Closure`.** The `_nf_config_<hash>` prefix is the
generated config script class name in both parsers (v1: nextflow/config/parser/v1/ConfigParserV1.groovy:201-208;
v2: nextflow/config/parser/v2/ConfigCompiler.java:204-210). The only wrapper type,
`ConfigClosurePlaceholder`, is used only when `renderClosureAsString` is on, i.e. for
`nextflow config` output (v1: ConfigTransformImpl.groovy:62-66, 107-178; v2:
ClosureToStringVisitor.java). Default parser is v1 (`NXF_SYNTAX_PARSER` defaults to `v1`,
nextflow/NF.groovy:36).

**How `params` resolves inside them.** At task time the delegate (`TaskContext`) wins: `params`
comes from the main script binding (TaskContext.groovy:139-141). Called without a delegate,
the owner answers:
- **v1:** the config script's metaClass looks the name up in the parsed config. For an unknown
  name it **creates and returns an empty `ConfigObject`** instead of throwing
  (ConfigParserV1.groovy:327-346, fallback at :335-340). `params` resolves (with CLI overrides
  merged, :342-344).
- **v2:** `ConfigDsl.getProperty` returns `target.params` for `params`; unknown names return
  `null` unless strict, in which case they throw (nextflow/config/parser/v2/ConfigDsl.groovy:104-118).
- Strict (v2) config does **not support functions** (docs/strict-syntax.md:565,
  https://nextflow.io/docs/latest/strict-syntax.html). `def tier()` in a config file is v1-only.

**Experiment** (Nextflow 25.10.4, a small pipeline that evaluates the raw closures, run with
`--mem_tier big`):

| closure | raw `.call()` v1 | raw `.call()` v2 | `TaskConfig` + `TaskContext` (v1 / v2) | real task |
|---|---|---|---|---|
| `{ params.mem_tier == 'tiny' ? … }` | `BATCH CORRECT 2` | `BATCH CORRECT 2` | `BATCH CORRECT 2` | `BATCH CORRECT 2` |
| `{ tier().heavy }` (v1 only) | `64` | n/a | `64` | `64` |
| `{ 2 * task.attempt }` | throws (`Integer * ConfigObject`) | NPE | `2` | `2` |
| `{ meta.id }` (task input) | empty `ConfigObject` | NPE | empty `ConfigObject` / NPE | `s1` |
| `{ pramas.x }` (typo) | empty `ConfigObject` | NPE | empty `ConfigObject` / NPE | — |

So: values that depend only on `params` and config functions resolve correctly before any task
exists. Values that depend on task inputs do **not** always throw: on v1 they quietly return an
empty `ConfigObject`. A resolver must type-check its results, not only catch exceptions.

**v1 parser trap, also seen in the experiment.** If `ext.X = …` is set *inside* `process { }`
before a selector, the selector's `ext.Y = …` is written into the global `process.ext`, and
`nextflow run` fails with `Unknown config attribute process.withName:foo`. Writing
`process.ext.X = …` outside the block, or after the selectors, works. v2 does not have this
problem. This matters because the example in CLAUDE.md uses the first form.

## 3. How selectors are applied

`ProcessDef.run()` → `initialize()` calls
`new ProcessConfigBuilder(processConfig).applyConfig(session.config.process, baseName, simpleName, processName)`
**when the process is invoked** in the workflow (nextflow/script/ProcessDef.groovy:97-101, 146-148).
`applyConfig` (nextflow/script/dsl/ProcessConfigBuilder.groovy:47-71) applies, from lowest to highest priority:
1. `withLabel:` matching any label (:88-106). `matchesLabels` compiles the pattern as a full-match
   regex, and a leading `!` negates it (:115-128).
2. `withName:` against the base name, then the alias, then the fully qualified name (`wf:sub:proc`)
   (:53-61). `matchesSelector` is the same regex/`!` logic (:172-177), so `'A|B'` and `'!align.*'` work.
3. Global `process.*` defaults, only where not set yet. For `ext`, keys are merged one by one
   (`applyConfigDefaults`, :219-235; `ext` in `applyConfigSettings`, :198-205).

The docs describe the same order (docs/config.md:205-262, https://nextflow.io/docs/latest/config.html#selector-priority).

- **Nested selectors.** v2 rejects them: `withName`/`withLabel` are allowed only directly in
  `process` (ConfigDsl.groovy:151-162, 241-247). v1 parses them but `applyConfigSettings` skips
  nested selector keys (ProcessConfigBuilder.groovy:189-190), so they have no effect (from reading
  the code; not tested).
- **Profiles.** Selected profiles are merged into the `process` scope at parse time, so the
  merged `session.config.process` already includes them.
- **Labels and `ext` from the process definition** (`label 'big'`, `ext args: …` in a module) are
  only in the per-process `ProcessConfig`, never in `session.config`. The hand-rolled walk misses them.

**Can a plugin get the per-process result without copying the matching logic?** Yes:
`TaskProcessor.getConfig()` returns the already-merged `ProcessConfig` (TaskProcessor.groovy:319),
and an observer receives every `TaskProcessor` in `onProcessCreate`. `ProcessConfigBuilder.matchesSelector`
and `matchesLabels` are also `public static` if matching is ever needed directly. Nextflow itself
uses `matchesSelector` to validate selectors (nextflow/Session.groovy:994-1005).

## 4. Observer timing in 25.10

`TraceObserverV2` is marked `@Beta` (nextflow/trace/TraceObserverV2.groovy:32). It has
`onFlowCreate(Session)` (:41), `onFlowBegin()` (:46) and `onProcessCreate(TaskProcessor)` (:62).

Order inside `ScriptRunner.execute` (nextflow/script/ScriptRunner.groovy:126-140, 242-251):
1. `session.start()` → `notifyFlowCreate()` → **onFlowCreate** (Session.groovy:584-599, :596).
   The script has **not been parsed yet**: no processes, labels or script-level `params`.
2. `parseScript` and `scriptLoader.runScript()`. Each process call runs `ProcessDef.run` →
   `TaskProcessor.run()`/`runV2()` → **onProcessCreate** (TaskProcessor.groovy:479, 643). The
   process's operator is **not started** here. `start(op)` only registers an igniter (:589-594),
   and channel factories also emit through igniters (nextflow/extension/CH.groovy:200-215,
   nextflow/Channel.groovy:307 etc.).
3. `session.fireDataflowNetwork()` → `checkConfig()` → `notifyFlowBegin()` → **onFlowBegin**,
   and only then `callIgniters()` (Session.groovy:539-566).

Consequences:
- Every process the workflow invokes gets `onProcessCreate` **before** `onFlowBegin`, and no
  task can be created or submitted before `onFlowBegin` returns, because observers are called
  synchronously (Session.groovy:1147-1157) before the igniters. The Seqera Platform plugin
  relies on this: it collects names in `onProcessCreate` and sends them in the `onFlowBegin`
  request (plugins/nf-tower/src/main/io/seqera/tower/plugin/TowerClient.groovy:362-372, 593).
- A process that is defined but **never invoked** (unused module, branch not taken) gets no
  `onProcessCreate`. That is correct for pre-config: it will never submit a job.
  `ScriptMeta.allProcessNames()` lists every defined name if that is ever needed
  (nextflow/script/ScriptMeta.groovy:71-78).
- Exceptions thrown by an observer are **swallowed at debug level** (Session.groovy:1150-1155).
  The plugin must catch and log its own errors.

## 5. Evaluating a process config for a "pseudo task"

There is no documented public API for this, but two public methods do it, and Nextflow uses one
of them itself:

- `TaskProcessor.createTaskPreview()` (TaskProcessor.groovy:398-410). It builds a `TaskRun` with
  `config.createTaskConfig()` and `new TaskContext(this)`, sets the context, and fills in
  `task.process` and `task.executor`. Its javadoc says inputs and outputs are **not** resolved.
  `nextflow inspect` uses it to evaluate the `container` directive of every process
  (nextflow/container/inspect/ContainersInspector.groovy:82-97).
- The manual equivalent: `processor.getConfig().createTaskConfig()` plus
  `setContext(new TaskContext(processor))`.

With that context:
- names in scope: the main script binding (`params` including script defaults, since the script
  has already run up to the process call), main-script functions, config-file functions (v1,
  via the owner), and `task` (`task.process`, `task.executor`, and `task.attempt` = 1).
- `task.exitStatus`, `task.previousTrace`, `task.hash`, `task.workDir` are `null`, so
  dereferencing them throws an NPE. That is a clean failure.
- task inputs (`meta`, `sample`) are missing. **v2 throws an NPE; v1 returns an empty
  `ConfigObject`** (see the table). A type check must reject that.

To make `task.attempt` fail instead of returning 1, you would have to supply your own `task`
object. That is not worth it: `attempt = 1` is exactly what the first submission will use, so
it is the right value for pre-config.

## 6. What other plugins do

None of the official plugins read per-process directives or `ext` before task time. They all
read `task.config` at submit:
- nf-amazon: `AwsBatchTaskHandler` reads `task.config.getResourceLabels()`, `getCpus()`,
  `getMemory()` while building the job request (plugins/nf-amazon/src/main/nextflow/cloud/aws/batch/AwsBatchTaskHandler.groovy:776, 824-825).
- nf-google: `GoogleBatchTaskHandler` (plugins/nf-google/src/main/nextflow/cloud/google/batch/GoogleBatchTaskHandler.groovy:210, 297).
- nf-azure: `AzBatchService` (plugins/nf-azure/src/main/nextflow/cloud/azure/batch/AzBatchService.groovy:690).
- nf-k8s: `K8sTaskHandler` (plugins/nf-k8s/src/main/nextflow/k8s/K8sTaskHandler.groovy:238, 297).
- nf-tower: `onProcessCreate` only records process names (TowerClient.groovy:362-366).
- nf-wave reads only its own config scopes (plugins/nf-wave/src/main/io/seqera/wave/plugin/WaveClient.groovy:122-126).

The only ahead-of-time evaluation in the codebase is core's `nextflow inspect` via
`createTaskPreview()`.

---

## Design options

### Option A — evaluate per process in `onProcessCreate`, send once in `onFlowBegin` (recommended)

Fixes both problems. Nextflow does the selector matching, and closures are resolved by the same
code that resolves them at task time.

```groovy
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import java.util.concurrent.ConcurrentHashMap

private final Map<String, ResourceConfiguration> processConfigurations = new ConcurrentHashMap<>()
private final AtomicBoolean isPreConfigSent = new AtomicBoolean(false)

@Override
void onFlowCreate(Session session) {
    log.info "Pipeline is starting! 🚀"
    FovusPipelineCache.getOrCreatePipelineId(pipelineClient, fovusConfig, fovusConfig.getPipelineName(),
                                             session?.getCommandLine())
    // pre-config moves to onFlowBegin: processes do not exist yet here
}

@Override
void onProcessCreate(TaskProcessor process) {
    // Session.notifyEvent swallows observer exceptions at debug level, so handle them here
    try {
        if( !(process.getExecutor() instanceof FovusExecutor) ) return
        final configuration = parseExtensionObject(resolveExtBeforeRun(process))
        if( configuration ) processConfigurations.put(process.getName(), configuration)
    }
    catch( Exception e ) {
        log.warn "[FOVUS] Cannot read the resource settings of process `${process.getName()}`: ${e.message}"
    }
}

@Override
void onFlowBegin() {
    sendPreConfigOnce()      // runs before any igniter, so before the first task
    hasFlowError = false
    lastFlowErrorEvent = null
    pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.RUNNING,
                                        session?.getCommandLine())
}

private void sendPreConfigOnce() {
    if( !isPreConfigSent.compareAndSet(false, true) ) return
    try {
        final configurations = new ArrayList<ResourceConfiguration>(new LinkedHashSet<>(processConfigurations.values()))
        final nextflowConfig = FovusUtil.readNextflowConfig(session?.getConfigFiles())
        pipelineClient.preConfigResources(fovusConfig, pipelineClient.getPipeline(), configurations, nextflowConfig)
    }
    catch( Exception e ) {
        log.warn "[FOVUS] Cannot configure pipeline resources: ${e.message}"   // was trace
    }
}

/** Resolve `ext` the way a first-attempt task would, keeping only values that do not need task inputs. */
@PackageScope
static Map<String,Object> resolveExtBeforeRun(TaskProcessor process) {
    // Same call `nextflow inspect` uses: merged ProcessConfig + TaskContext, task.attempt == 1
    final TaskConfig taskConfig = process.createTaskPreview().getConfig()
    final ext = (Map<String,Object>) taskConfig.get('ext')       // LazyMap with context set
    final result = new LinkedHashMap<String,Object>()
    for( String key : new ArrayList<String>(ext.keySet()) ) {
        try {
            final value = ext.get(key)                                // closures / lazy GStrings resolved here
            if( isResolvedValue(value) )
                result.put(key, value)
            else
                log.warn "[FOVUS] `ext.${key}` of process `${process.getName()}` depends on task inputs; " +
                         "it is left out of the pre-configuration and applied when the job is submitted"
        }
        catch( Exception e ) {
            log.warn "[FOVUS] `ext.${key}` of process `${process.getName()}` cannot be resolved before the task runs: ${e.message}"
        }
    }
    return result
}
```

Notes:
- `collectResourceConfigurations` and `ResourceConfiguration.mergeWith` are no longer needed for
  pre-config, because Nextflow has already merged the global `ext`, labels, names and aliases.
  The existing specs in `FovusTraceObserverTest` move to `resolveExtBeforeRun`/`parseExtensionObject`.
- The result now matches what the job will actually get: label + name merges, `ext` from the
  process definition, aliases, fully qualified names. Selectors that match nothing no longer
  cause pre-config.
- Processes on another executor are skipped. Today every `ext` block is sent, whatever its executor.
- Risks: `TraceObserverV2` is `@Beta`, and `createTaskPreview()` is public but meant for preview.
  If it ever changes, fall back to `getConfig().createTaskConfig().setContext(new TaskContext(process))`.
  Both are pinned by `nextflowVersion = '25.10.0'` in build.gradle; add one test per API.
- Pre-config now waits for script parsing and evaluation (seconds), but it still happens before
  the first submission.

### Option B — keep `onFlowCreate`, evaluate closures from the raw map with a `params` delegate

Smallest change. It fixes the closure-to-string problem but **not** the hand-rolled selectors.

```groovy
static Object evalConfigValue(Object value, Session session) {
    if( !(value instanceof Closure) ) return value
    final copy = (Closure) ((Closure) value).clone()
    // A Map delegate returns null for unknown names, so `meta.id` fails with an NPE instead of
    // falling back to the v1 owner (which would return an empty ConfigObject). Config-file
    // functions still resolve through the owner.
    copy.setDelegate([params: session.getParams(), task: [attempt: 1]])
    copy.setResolveStrategy(Closure.DELEGATE_FIRST)
    return copy.call()
}
```

Limits: `session.getParams()` (Session.groovy:309) only holds config and CLI params at this point
(set in `Session.init`, :467). `params.x = …` defaults in `main.nf` are assigned later, during
`runScript`, so a closure that depends on them resolves wrongly. There are still no process
names, labels or definition-level `ext`, and the "one config per selector block" model stays
(a process matched by both a label and a name gets two partial configs, neither of which is
what the job uses).

### Option C — send only literal values; skip anything dynamic

In `parseExtensionObject`, drop any `Closure`/`GString` value with a warning. If
`benchmarkingProfileName` itself is dynamic, skip that block. The job still gets the right value
at submission. Trivial and safe, but processes whose profile is chosen by a closure lose
pre-warming, which is the case the user hit.

### Recommendation

**Option A**, with the guard below as defense in depth. It is the only option that pre-configures
exactly what the jobs will request, and it removes the hand-rolled selector code instead of
patching it. Use Option C's behaviour (skip and warn) as the fallback for each value that cannot
be resolved before the task runs.

## Defensive guard (all options)

Never send a value whose string form looks like a closure. Check it both where values are read
and just before the CLI call.

```groovy
private static final List<String> CLOSURE_MARKERS = ['_nf_config_', '$_closure', '$_run_closure']

static boolean isResolvedValue(Object value) {
    if( value == null ) return true
    if( value instanceof Closure || value instanceof ConfigObject ) return false   // ConfigObject: v1 unknown-name fallback
    if( value instanceof Map ) return ((Map) value).values().every { isResolvedValue(it) }
    if( value instanceof Collection ) return ((Collection) value).every { isResolvedValue(it) }
    final text = value.toString()
    return !CLOSURE_MARKERS.any { text.contains(it) }
}
```

- In `parseExtensionObject`: require `benchmarkingProfileName instanceof CharSequence &&
  isResolvedValue(...)`, otherwise return `null` and warn. Today the map constructor turns any
  object into a String (FovusTraceObserver.groovy:159). `timeToCostPriorityRatio` is assigned
  without a type check too (FovusTraceObserver.groovy:231-232), so it has the same bug.
- In `FovusPipelineClient.preConfigResources` (FovusPipelineClient.groovy:124-147): before
  `toJson`, drop and warn about any configuration whose string fields fail `isResolvedValue`.
  One bad entry must not make the backend reject the whole batch.

## Calling `preConfigResources` more than once or later

- **Only caller:** `FovusTraceObserver.onFlowCreate` (FovusTraceObserver.groovy:59). Everything else is tests.
- **Retries already exist:** `runCli` → `FovusUtil.executeCommand` retries up to 3 times on a
  non-zero exit (FovusUtil.groovy:45-51). A request that reached the backend can therefore be
  sent again.
- **Backend is not idempotent in general.** Each call re-validates, overwrites `pipelineConfig`
  and asynchronously invokes the pre-configure-compute-environment lambda again
  (pre-config-resources.ts:177-188, 279-300). Only the capacity-reservation path guards against
  repeats: an existing live pool is a no-op (:192-201), and a finished pipeline is rejected (:207-256).
- **So:** call it exactly once per run (the `AtomicBoolean` in Option A), and do not call it
  from `onProcessCreate` per process or from task events. Calling it late (after tasks start)
  defeats the purpose and races the first jobs. `-resume` already calls it again per run against
  the same cached pipeline id (FovusPipelineCache). That is existing behaviour; Option A does not
  change it.
