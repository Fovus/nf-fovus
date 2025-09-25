package nextflow.fovus

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.type.TypeReference
import nextflow.fovus.pipeline.FovusPipelineClient

class FovusPipelineCache {
    public static final String PIPELINE_CACHE_FILE_PATH = "./work/.nextflow/fovus/pipeline_cache.json"

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    static String getOrCreatePipelineId(FovusPipelineClient pipelineClient,
                                        FovusConfig fovusConfig,
                                        String pipelineName) {
        def existingPipelineId = getPipelineId(pipelineName)
        if (existingPipelineId) {
            pipelineClient.setPipeline(pipelineName, existingPipelineId)
            return existingPipelineId
        }

        def newPipelineId = pipelineClient.createPipeline(fovusConfig, pipelineName)
        updatePipelineCache(pipelineName, newPipelineId)
        return newPipelineId
    }

    static void updatePipelineCache(String pipelineName, String pipelineId) {
        File cacheFile = new File(PIPELINE_CACHE_FILE_PATH)
        Map<String, String> pipelineCache = [:]

        if (cacheFile.exists()) {
            try {
                pipelineCache = OBJECT_MAPPER.readValue(cacheFile, new TypeReference<Map<String, String>>() {})
            } catch (IOException e) {
                e.printStackTrace()
            }
        } else {
            // Ensure parent directories exist
            cacheFile.parentFile?.mkdirs()
        }

        pipelineCache[pipelineName] = pipelineId

        try {
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValue(cacheFile, pipelineCache)
        } catch (IOException e) {
            e.printStackTrace()
        }
    }

    static String getPipelineId(String pipelineName) {
        File cacheFile = new File(PIPELINE_CACHE_FILE_PATH)
        Map<String, String> pipelineCache = [:]

        if (cacheFile.exists()) {
            try {
                pipelineCache = OBJECT_MAPPER.readValue(cacheFile, new TypeReference<Map<String, String>>() {})
            } catch (IOException e) {
                e.printStackTrace()
            }
        }

        return pipelineCache[pipelineName]
    }
}
