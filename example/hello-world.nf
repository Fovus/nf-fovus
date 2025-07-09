process stage1aArray {
    array 2
    input:
        path query_file
    output:
        path 'output*.txt'

    script:
    """
    query_file_name=\$(basename ${query_file})
    echo "Hello from stage1aArray" > output_stage1aArray_\${query_file_name}.txt
    echo "Hello from stage1aArray" > out1.txt
    echo "Running Stage 1 Task \${query_file_name}"
    ./${query_file}
    """
}

process stage1bNormal {
    input:
        path input
    output:
        path 'out2.txt'

    script:
    """
    echo "Hello from stage1bNormal" > out2.txt
    echo "Running Stage 1 Task ${input}"
    ./${input}
    """
}

process stage1cParallel {
    input:
        path input
    output:
        path 'output*.txt'

    script:
    """
    echo "Hello from stage1cParallel" > out3.txt
    echo "Hello from stage1cParallel" > output3_stage1cParallel_${input}.txt
    echo "Running Stage 1 Task ${input}"
    ./${input}
    """
}

process stage2aArray {
    array 2
    input:
        path query_file
    output:
        path 'output*.txt'

    script:
    """
    query_file_name=\$(basename ${query_file})
    ls -la
    echo "Hello from stage2aArray" > output_stage2aArray_\${query_file_name}.txt
    echo "Hello from stage2aArray" > out4.txt
    echo "Running Stage 2 Task \${query_file_name}"
    ./${query_file}
    """
}

process stage2bNormal {
    input:
        path input
        path output
    output:
        path 'out5.txt'

    script:
    """
    ls -la
    echo "Hello from stage2bNormal" > out5.txt
    echo "Running stage2bNormal Task ${input}"
    ./${input}
    """
}

process stage3Normal {
    input:
        path input
        path op1
    output:
        path 'out6.txt'

    script:
    """
    ls -la
    echo "Hello from stage3Normal" > out6.txt
    echo "Running stage3Normal Task ${input}"
    ./${input}
    """
}

workflow testWorkflow {
    // Stage 1 Inputs
//     def stage1MultiInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Hello-Workload/*.sh")
    def stage1SingleInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Hello-Workload/hello.sh")
//     def stage1MultiParallelInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Hello-Workload/*.sh")

    // Stage 1 Processes
//     def stage1aArrayOp = stage1aArray(stage1MultiInput)
    def stage1bNormalOp = stage1bNormal(stage1SingleInput)
//     def stage1cParallelOp = stage1cParallel(stage1MultiParallelInput)

    // Stage 2 Inputs
//     def stage2MultiInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/World-Workload/*.sh")
//     def stage2SingleInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/World-Workload/world.sh")

//     // Stage 2 Processes
// //     def stage2aArrayOp = stage2aArray(stage2MultiInput)
//     def stage2bNormalOp = stage2bNormal(stage2SingleInput, stage1bNormalOp)
// //
// //     // Stage 3 Process
//     def stage3NormalOp = stage3Normal(stage2MultiInput, stage2bNormalOp)
}

workflow {
    testWorkflow()
}