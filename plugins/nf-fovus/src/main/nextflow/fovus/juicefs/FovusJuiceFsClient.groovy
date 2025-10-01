package nextflow.fovus.juicefs

import groovy.json.JsonSlurper
import nextflow.fovus.FovusConfig
import nextflow.fovus.FovusUtil

class FovusJuiceFsClient {
    private FovusConfig config

    FovusJuiceFsClient(FovusConfig config) {
        this.config = config
    }

    JuiceFsStatus getJuiceFsStatus() {
        // TODO: Update CLI command once finalize
        def command = [config.getCliPath(), 'storage', 'juicefs-status']
        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to get JuiceFS status: ${result.error}")
        }
        final jsonData = new JsonSlurper().parseText(result.output)
        if (!(jsonData instanceof Map)) {
            throw new RuntimeException("No JuiceFs mount was found")
        }
        final juiceFsSettingData = jsonData["Setting"] as Map
        final juiceFsSetting = new JuiceFsSetting(name: juiceFsSettingData['Name'], storage: juiceFsSettingData['Storage'], bucket: juiceFsSettingData['Bucket'])

        final juiceFsSessionsData = jsonData["Sessions"] as List<Map>
        List<JuiceFsSession> sessions = juiceFsSessionsData.collect { it -> new JuiceFsSession(hostname: it['HostName'], mountPoint: it['MountPoint']) }

        return new JuiceFsStatus(setting: juiceFsSetting, sessions: sessions)
    }
}
