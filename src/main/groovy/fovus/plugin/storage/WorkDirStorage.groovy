package fovus.plugin.storage

import groovy.transform.CompileStatic

import java.nio.file.Path

/** Where a pipeline's work directory lives: a Fovus storage mount, or Fovus storage itself (direct mode). */
@CompileStatic
interface WorkDirStorage {

    /** Make the work directory usable for this pipeline: mount it, or fetch credentials and attach the S3 client. */
    void prepare(String pipelineId)

    /** Whether Nextflow must stage (copy) this input into the work directory before a task can use it. */
    boolean isForeignFile(Path path)

    /** The path as the compute node sees it, under {@code /fovus-storage}. */
    Path remotePath(Path path)
}
