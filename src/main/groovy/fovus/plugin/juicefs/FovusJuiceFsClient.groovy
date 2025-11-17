package fovus.plugin.juicefs

import fovus.plugin.FovusConfig
import fovus.plugin.FovusUtil
import nextflow.util.Escape

import java.nio.file.Path

class FovusJuiceFsClient {
    private FovusConfig config

    FovusJuiceFsClient(FovusConfig config) {
        this.config = config
    }

    void validateOrMountJuiceFs(Path path) {
        def command = [config.getCliPath(), 'storage-cached', 'mount', '--mount-storage-path', Escape.path(path.toAbsolutePath())]

        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("[FOVUS] Fail to mount working directory at ${path}")
        }
    }
}
