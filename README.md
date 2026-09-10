# nf-fovus plugin

## Configuration

```groovy
plugins { id 'nf-fovus' }

fovus {
    cliPath      = '/opt/miniconda3/bin/fovus'  // optional, defaults to 'fovus'
    pipelineName = 'my-pipeline'                // required
    projectName  = 'my-project'                 // optional
}
```

## Automated authentication (`fovus.auth`)

By default the plugin assumes `fovus` is already authenticated on the host, ie a human has already
run `fovus auth login`. That works for interactive local runs, but an internal tool that triggers
Nextflow pipelines automatically (a headless/CI/service context) has no interactive session and no
way to establish one.

Set `fovus.auth` to let such a run authenticate without any interactive login step:

```groovy
fovus {
    pipelineName = 'my-pipeline'

    auth {
        email               = secrets.FOVUS_EMAIL
        personalAccessToken = secrets.FOVUS_PAT
    }
}
```

Store the values first with Nextflow's local secrets store:

```bash
nextflow secrets set FOVUS_EMAIL 'automation@corp.com'
nextflow secrets set FOVUS_PAT   'your-personal-access-token'
```

Notes:

- **Both fields are required together**, and **both must be set via `secrets.X`** — a literal string
  or a `System.getenv(...)`-sourced value fails at startup with a clear error. This is enforced
  structurally (not by inspecting the resolved value), so there is exactly one supported pattern.
- Neither set → the feature is inactive and the plugin behaves exactly as it does today: any existing
  interactive session, or an ambient `FOVUS_EMAIL`/`FOVUS_PAT` already exported by the launching
  process, continues to work unchanged.
- The plugin never calls `fovus auth login` and never touches `~/.fovus/.credentials` — it only
  injects `FOVUS_EMAIL`/`FOVUS_PAT` into the environment of every `fovus` CLI subprocess it spawns,
  and relies on the Fovus CLI's own environment-variable login to authenticate, cache, and refresh the
  session. This keeps an automated run fully isolated from any interactive session on the same host,
  and never puts the token on a process's command-line arguments.
- This only applies when Nextflow is running in `LOCAL` workflow-host mode; it has no effect on a
  pipeline already running inside Fovus's own managed `REMOTE` execution context.
- On a **shared host**, run each automated pipeline under its own OS user or container — environment
  variables are readable by other processes running as the same OS user via `/proc/<pid>/environ`.
- **Requires a Fovus CLI release that supports `FOVUS_EMAIL`/`FOVUS_PAT` environment-variable login.**
  There is no fallback for older CLI versions. The minimum CLI version will be documented here once
  that release is tagged.

## Storage connectors

A storage connector grants a Fovus job IAM access to an S3 bucket. Declare the connectors a process
needs with the `storageConnectors` process directive:

```groovy
process alignReads {
    ext.storageConnectors = ['reference-genomes', 'team-shared-2024']

    script:
    """
    aws s3 cp s3://my-reference-bucket/hg38.fa .
    """
}
```

The attribute is **optional**. A pipeline that never mentions `storageConnectors` behaves exactly as
before: the job is created with an empty list and nothing else changes.

Set it for every process at once in `nextflow.config`:

```groovy
process {
    ext.storageConnectors = ['reference-genomes']
}
```

...or per process, which overrides the pipeline-wide value:

```groovy
process {
    ext.storageConnectors = ['reference-genomes']

    withName: 'publishResults' {
        ext.storageConnectors = ['reference-genomes', 'results-archive']
    }
}
```

It can also come from a JSON job config file (`ext.jobConfigFile`) or a benchmarking profile:

```json
{
  "storageConnectors": ["reference-genomes"]
}
```

`ext.storageConnectors` on the process wins over either of those, matching the precedence used by
every other Fovus job attribute.

### Pre-configured resources

At the start of a run the plugin sends the resource configurations declared in `process.ext` to
`fovus pipeline pre-config-resources`, so Fovus can provision the pipeline's resources ahead of the
first job. `storageConnectors` travels with those configurations, which is what lets an on-demand
pcluster be created with the IAM roles its jobs will need.

A configuration is only sent when its `ext` block also declares a `benchmarkingProfileName`. A
process that declares `storageConnectors` without one still gets the connectors on its job, but it
contributes nothing to the pre-configuration step. Per-process connectors override the pipeline-wide
ones in the payload, and a process that declares none inherits the pipeline-wide list; setting
`ext.storageConnectors = []` on a process clears the pipeline-wide list for it, in the payload as
well as on the job.

The payload follows the same precedence as the job, so connectors named in a process's
`ext.jobConfigFile` are read from that file and sent too. Connectors that come from a benchmarking
profile are not read locally - the profile name travels with the configuration, so Fovus resolves
them server side.

### What a connector grants, per execution path

`storageConnectors` means three different things depending on how the work is executed. **Nextflow
work is pipeline-shaped, so the union behaviour is the one that applies to nf-fovus runs.**

| Execution path | Behaviour |
| --- | --- |
| **Pipeline job** (every nf-fovus process) | **Union** - connectors declared on **any** job in a pipeline are reachable from **every** job in that pipeline |
| Standalone batch job | **Enforcing** - the job gets access to exactly the connectors it lists |
| Standalone pcluster / Slurm job | **Enforcing** - the job gets access to exactly the connectors it lists |

Because an nf-fovus run is a pipeline, declaring a connector on one process makes it reachable from
all of them. Restricting a bucket to a single process is not something `storageConnectors` can
express - do not rely on it for isolation between processes of the same pipeline.

Storage connectors grant **IAM access only**; they do not mount anything into the task work
directory. Reach the bucket with the AWS CLI or an SDK from inside your process script.

### Validation

The plugin checks the **name shape only**, before the job is submitted. A connector name may contain
letters, digits and hyphens (`^[a-zA-Z0-9-]+$`) - no spaces, underscores or slashes. A malformed
entry fails the run immediately and names the offending value:

```
[Fovus] Invalid storage connector name: 'team_shared'. A storage connector name may only contain
letters, digits and hyphens (^[a-zA-Z0-9-]+$).
```

Whether a connector actually exists, and whether you are entitled to use it, is decided **server
side** at submission time and is deliberately not checked or cached locally - entitlement can change
between a local check and the submission. The server's rejection is surfaced verbatim, and it
distinguishes an unknown connector from one you are not entitled to.


## Building

To build the plugin:
```bash
make assemble
```

## Testing with Nextflow

The plugin can be tested without a local Nextflow installation:

1. Build and install the plugin to your local Nextflow installation: `make install`
2. Run a pipeline with the plugin: `nextflow run hello -plugins nf-fovus@0.1.0`

## Publishing

Plugins can be published to a central plugin registry to make them accessible to the Nextflow community. 


Follow these steps to publish the plugin to the Nextflow Plugin Registry:

1. Create a file named `$HOME/.gradle/gradle.properties`, where $HOME is your home directory. Add the following properties:

    * `npr.apiKey`: Your Nextflow Plugin Registry access token.

2. Use the following command to package and create a release for your plugin on GitHub: `make release`.


> [!NOTE]
> The Nextflow Plugin registry is currently available as preview technology. Contact info@nextflow.io to learn how to get access to it.
> 
