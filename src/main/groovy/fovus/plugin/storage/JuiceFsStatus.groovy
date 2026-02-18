package fovus.plugin.storage

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor

@CompileStatic
@Canonical
@MapConstructor
class JuiceFsStatus {
    JuiceFsSetting setting
    List<JuiceFsSession> sessions
}

@Canonical
@MapConstructor
class JuiceFsSetting {
    String name
    String storage
    String bucket
}

@Canonical
@MapConstructor
class JuiceFsSession {
    String mountPoint
    String hostname
}
