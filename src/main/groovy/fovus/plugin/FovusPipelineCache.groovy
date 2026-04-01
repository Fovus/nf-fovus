package fovus.plugin

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.type.TypeReference
import fovus.plugin.pipeline.FovusPipelineClient
import groovy.util.logging.Slf4j

@Slf4j
class FovusPipelineCache {
    public static final String PIPELINE_CACHE_FILE_PATH = "./work/.nextflow/fovus/pipeline_cache.json"

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    static String getOrCreatePipelineId(FovusPipelineClient pipelineClient,
                                        FovusConfig fovusConfig,
                                        String pipelineName) {
        final workflowHost = System.getenv("WORKFLOW_HOST")
        final pipelineIdFromEnv = System.getenv("PIPELINE_ID")

        log.info("[FOVUS] WORKFLOW_HOST: ${workflowHost}")
        log.info("[FOVUS] PIPELINE_ID: ${pipelineIdFromEnv}")
        // In REMOTE workflow mode, prefer an explicit pipeline ID from env over local cache/creation.
        if ("REMOTE".equalsIgnoreCase(workflowHost) && pipelineIdFromEnv) {
            log.info("[FOVUS] Using PIPELINE_ID from environment for REMOTE workflow host: ${pipelineIdFromEnv}")
            pipelineClient.setPipeline(pipelineName, pipelineIdFromEnv)
            // Keep cache aligned so later lookups in the same workspace resolve to the same pipeline.
            updatePipelineCache(pipelineName, pipelineIdFromEnv)
            return pipelineIdFromEnv
        }
        if ("REMOTE".equalsIgnoreCase(workflowHost) && !pipelineIdFromEnv) {
            log.info("[FOVUS] WORKFLOW_HOST is REMOTE but PIPELINE_ID is not set; falling back to cache/create flow")
        }

        def existingPipelineId = getPipelineId(pipelineName)
        if (existingPipelineId) {
            final existingPipeline = pipelineClient.getPipeline(fovusConfig, existingPipelineId)
            final existingPipelineStatus = existingPipeline.status.toString()

            if (existingPipelineStatus in ["CREATED", "COMPLETED", "RUNNING", "FAILED"]) {
                pipelineClient.setPipeline(pipelineName, existingPipelineId)
                return existingPipelineId
            }
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
