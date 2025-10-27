package groovy.fovus.pipeline

class FovusPipeline {
    private String name
    private String pipelineId
    private FovusPipelineStatus status

    FovusPipeline(String name, String pipelineId) {
        this.name = name
        this.pipelineId = pipelineId
    }

    FovusPipeline(String name, String pipelineId, FovusPipelineStatus status) {
        this(name, pipelineId)
        this.setStatus(status)
    }

    String getName() {
        return name
    }

    String getPipelineId() {
        return pipelineId
    }

    FovusPipelineStatus getStatus() {
        return status
    }

    void setStatus(FovusPipelineStatus status) {
        this.status = status
    }
}
