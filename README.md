# nf-fovus plugin

Fovus plugin for integration with Nextflow.

## Plugin structure

- `settings.gradle`

  Gradle project settings.

- `plugins/nf-fovus`

  The plugin implementation base directory.

- `plugins/nf-fovus/build.gradle`

  Plugin Gradle build file. Project dependencies should be added here.

- `plugins/nf-fovus/src/resources/META-INF/MANIFEST.MF`

  Manifest file defining the plugin attributes e.g. name, version, etc. The attribute `Plugin-Class`
  declares the plugin main class. This class should extend the base class
  `nextflow.plugin.BasePlugin` e.g. `nextflow.fovus.FovusPlugin`.

- `plugins/nf-fovus/src/resources/META-INF/extensions.idx`

  This file declares one or more extension classes provided by the plugin. Each line should contain
  the fully qualified name of a Java class that implements the `org.pf4j.ExtensionPoint` interface (
  or a sub-interface).

- `plugins/nf-fovus/src/main`

  The plugin implementation sources.

- `plugins/nf-fovus/src/test`

  The plugin unit tests.

## Plugin classes

- `FovusConfig`: configuration class for Fovus executor such as CLI path.

- `FovusExecutor`: executor class responsible for creating task handlers and handling job lifecycle.

- `FovusTaskHandler`: task handler class responsible for executing Fovus CLI commands to submit jobs
  to Fovus platform and check job statuses.

- `FovusJobClient`: client class for executing Fovus CLI commands.

- `FovusJobConfig`: configuration class for Fovus job. Responsible for mapping NextFlow task configs
  to Fovus job configs and save the job configuration file into a JSON file.

## Unit testing

To run your unit tests, run the following command in the project root directory (ie. where the file
`settings.gradle` is located):

```bash
./gradlew check
```

## Testing and debugging

To build and test the plugin during development, configure a local Nextflow build with the following
steps:

1. Clone the Nextflow repository in your computer into a sibling directory:
    ```bash
    git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow
    ```

2. Configure the plugin build to use the local Nextflow code:
    ```bash
    echo "includeBuild('../nextflow')" >> settings.gradle
    ```

   (Make sure to not add it more than once!)

3. Compile the plugin alongside the Nextflow code:
    ```bash
    make compile
    make assemble
    ```

4. Run Nextflow with the plugin, using `./launch.sh` as a drop-in replacement for the `nextflow`
   command, and adding the option `-plugins nf-hello` to load the plugin:
    ```bash
    ./launch.sh -trace nextflow,nf-fovus,fovus run example/hello-world.nf -plugins nf-fovus
    ```

## Testing without Nextflow build

The plugin can be tested without using a local Nextflow build using the following steps:

1. Build the plugin: `make buildPlugins`
2. Copy `build/plugins/<your-plugin>` to `$HOME/.nextflow/plugins`
3. Create a pipeline that uses your plugin and run it: `nextflow run ./my-pipeline-script.nf`

## Package, upload, and publish

The project should be hosted in a GitHub repository whose name matches the name of the plugin, that
is the name of the directory in the `plugins` folder (e.g. `nf-hello`).

Follow these steps to package, upload and publish the plugin:

1. Create a file named `gradle.properties` in the project root containing the following attributes (
   this file should not be committed to Git):

    * `github_organization`: the GitHub organisation where the plugin repository is hosted.
    * `github_username`: The GitHub username granting access to the plugin repository.
    * `github_access_token`: The GitHub access token required to upload and commit changes to the
      plugin repository.
    * `github_commit_email`: The email address associated with your GitHub account.

2. Use the following command to package and create a release for your plugin on GitHub:
    ```bash
    ./gradlew :plugins:nf-hello:upload
    ```

3. Create a pull request
   against [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) to
   make the plugin accessible to Nextflow.
