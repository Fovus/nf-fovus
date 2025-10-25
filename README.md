# 🔌 nf-fovus Nextflow Plugin

The `nf-fovus` plugin integrates [Fovus](https://fovus.co) with [Nextflow](https://www.nextflow.io) to enable intelligent job configuration and optimization based on constraints, environments, and workload profiles.

---

## 📦 Setup

- Install the Fovus CLI

  `pip install fovus`

- Login into Fovus CLI

  `fovus auth login`

- Install the `nf-fovus` plugin

  `nextflow plugin install nf-fovus`

## How to use

- Define your Fovus Job Config file. (Example templates - `plugins/nf-fovus/fovus_provided_configs`)
- Use the Plugin in Your Nextflow Workflow
  - In your main.nf or custom process, the plugin can read and apply configuration logic.
      
  - Example:
         
  -     process FovusLaunch {
  
            ext (
                 jobConfigFile: '<Fovus job config file path>'
            )

            input:
            // Provide Input

            script:
            """
              // execution script
            """
        }
- Run Nexflow Pipeline:
    
   - `nextflow run main.nf -plugins nf-fovus`

## Reference documents:

- Fovus CLI : https://help.fovus.co/cli/get_started.html
  
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

# 🚀 NF Plugin Release Notes

## 📦 Plugin Packaging, Upload, and Publishing

### 1. Verify Plugin Version
Update the plugin manifest file to confirm the correct version before release:
```
plugins/nf-hello/src/resources/META-INF/MANIFEST.MF
```

---

### 2. Build the Plugin
Build the plugin artifacts using:
```bash
make buildPlugins
```

---

### 3. Create `gradle.properties`
Create a file named `gradle.properties` in the project root directory **(do not commit this file to Git)** with the following content:

```properties
# DO NOT COMMIT this file to git
github_organization=Fovus
github_username=<username>
github_access_token=ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
github_commit_email=abc@example.com
```

---

### 4. Upload the Plugin
Run the following Gradle task to upload and create a release for the plugin on GitHub:
```bash
./gradlew :plugins:nf-fovus:upload
```

---

### 5. Verify GitHub Release
Ensure the release process was successful:
- A new GitHub **release** should be triggered automatically.
- The **ZIP artifact** containing the updated version should be available in the release assets.

---

### 6. Retrieve Metadata File
After the release, obtain the generated metadata file:
```
fovus-<version>-meta.json
```

---

## 🧾 Update `fovus-nextflow-plugin` Repository for Official Release

### 1. Create a New Branch
In the `fovus-nextflow-plugin` repository, create a new branch following this naming convention:
```
<Ownername>-nf-fovus-<version>
```
Example:
```
<name>-nf-fovus-1.2.0
```

---

### 2. Update `plugins.json`
Append the contents of the `fovus-<version>-meta.json` file under the `nf-fovus` array in `plugins.json`.

Example:
```json
{
  "nf-fovus": [
    {
        "version": "1.0.0",
        "url": "https://github.com/nextflow-io/nf-fovus/releases/download/1.0.0/nf-fovus-1.0.0.zip",
        "date": "2025-10-25T12:19:41.477854805-07:00",
        "sha512sum": "3982cfcfef1e20716197419eb660dfb173062e8a1a7e0d12c2af702accde049a06953d49868389a0b936be15e9c5bfc906611aa587d1e5dcc61b0aa7123f2e24",
        "requires": ">=25.01.0-edge"
     }
  ]
}
```

Then, **create a Pull Request** to submit the update for an official release.
