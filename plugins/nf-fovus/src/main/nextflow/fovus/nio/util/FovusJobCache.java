package nextflow.fovus.nio.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import nextflow.fovus.FovusClient;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.time.Instant;

public class FovusJobCache {

    // File format -> { "<taskName>" : "<jobTimestamp>" }
    public static final String JOB_CACHE_FILE_PATH = "./work/.nextflow/fovus/job_cache.json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Get the job timestamp for a given task.
     * If not present, create a new timestamp using FovusClient,
     * store it using updateJobCache, and return it.
     */
    public static String getOrCreateJobTimestamp(String taskName, FovusClient fovusClient) {
        String existingTimestamp = getGeneratedJobId(taskName);

        if (existingTimestamp != null) {
            return existingTimestamp;
        }

        // Otherwise, create new timestamp and use updateJobCache to save it
        String newJobId = fovusClient.generateJobId();
        updateJobCache(taskName, newJobId);

        return newJobId;
    }

    /**
     * Get the job timestamp for a given task, if it exists in the cache.
     * If not found, returns null (does not create a new one).
     */
    public static String getGeneratedJobId(String taskName) {
        File cacheFile = new File(JOB_CACHE_FILE_PATH);
        Map<String, String> jobCache = new HashMap<>();

        // Load existing cache if present
        if (cacheFile.exists()) {
            try {
                jobCache = OBJECT_MAPPER.readValue(cacheFile, new TypeReference<Map<String, String>>() {});
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Return timestamp if found, otherwise null
        return jobCache.getOrDefault(taskName, null);
    }

    /**
     * Add or update a job cache record for a task.
     * Creates the cache file if it doesn't exist.
     */
    public static void updateJobCache(String taskName, String jobTimestamp) {
        File cacheFile = new File(JOB_CACHE_FILE_PATH);
        Map<String, String> jobCache = new HashMap<>();

        // Load existing cache if present
        if (cacheFile.exists()) {
            try {
                jobCache = OBJECT_MAPPER.readValue(cacheFile, new TypeReference<Map<String, String>>() {});
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // Ensure parent directories exist
            File parentDir = cacheFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }
        }

        // Add or update the task record
        jobCache.put(taskName, jobTimestamp);

        // Write back to the JSON file
        try {
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(cacheFile, jobCache);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
