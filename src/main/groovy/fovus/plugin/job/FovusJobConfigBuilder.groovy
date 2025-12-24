package fovus.plugin.job

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.ObjectNode
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper

@CompileStatic
class FovusJobConfigBuilder {
    static FovusJobConfig fromJsonString(String jsonData) {

        def mapper = new ObjectMapper()
        SimpleModule module = new SimpleModule()
        module.addDeserializer(Environment, new EnvironmentDeserializer())
        mapper.registerModule(module)

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        def defaultJobConfig = mapper.readValue(jsonData, FovusJobConfig)

        def json = new JsonSlurper().parseText(jsonData)
        if (json instanceof Map) {
            def envData = json.environment
            if (envData instanceof Map && envData?.containerized) {
                def containerData = envData.containerized
                if (containerData instanceof Map) {
                    def containerized = new Containerized(
                            container: containerData.container,
                            imagePath: containerData.imagePath,
                            version: containerData.version
                    )

                    def environment = new ContainerizedEnvironment(containerized: containerized)
                    defaultJobConfig.environment = environment
                }

            } else if (envData instanceof Map && envData?.monolithicList) {
                // TODO: Create monolithic environment object and set in config
            }
        }

        return defaultJobConfig

    }

    static FovusJobConfig fromJsonFile(String path) {
        String jsonData = new File(path).getText()
        return fromJsonString(jsonData)
    }
}

class EnvironmentDeserializer extends JsonDeserializer<Environment> {
    @Override
    Environment deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        def mapper = (ObjectMapper) p.codec
        ObjectNode node = p.codec.readTree(p)

        if (node.has("monolithicList")) {
            return mapper.treeToValue(node, MonolithicEnvironment)
        } else if (node.has("containerized")) {
            return mapper.treeToValue(node, ContainerizedEnvironment)
        }

        return null // fallback
    }
}

