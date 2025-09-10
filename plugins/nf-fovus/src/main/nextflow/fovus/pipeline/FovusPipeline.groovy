package nextflow.fovus.pipeline

class FovusPipeline {
    private String name
    private String pipelineId

    FovusPipeline(String name, String pipelineId) {
        this.name = name
        this.pipelineId = pipelineId
    }

    String getName() {
        return name
    }

    String getPipelineId() {
        return pipelineId
    }
}
