package nextflow.fovus.job

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.ObjectNode
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Paths

@CompileStatic
class FovusJobConfigBuilder {
    static FovusJobConfig fromJsonFile(String path) {

        def mapper = new ObjectMapper()
        SimpleModule module = new SimpleModule()
        module.addDeserializer(Environment, new EnvironmentDeserializer())
        mapper.registerModule(module)

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        def defaultJobConfig =  mapper.readValue(Paths.get(path).toFile(), FovusJobConfig)

        def json = new JsonSlurper().parse(new File(path))
        if(json instanceof Map){
            def envData = json.environment
            if (envData instanceof Map && envData?.containerized) {
                def containerData = envData.containerized
                if(containerData instanceof Map){
                    def containerized = new Containerized(
                            container: containerData.container,
                            imagePath: containerData.imagePath,
                            version: containerData.version
                    )

                    def environment = new ContainerizedEnvironment(containerized: containerized);
                    defaultJobConfig.environment = environment;
                }

            } else if (envData instanceof Map && envData?.monolithicList) {
                // TODO: Create monolithic environment object and set in config
            } else {
                throw new Error("Invalid fovus job config. environment data is missing!!")
            }
        }

        return defaultJobConfig;
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


