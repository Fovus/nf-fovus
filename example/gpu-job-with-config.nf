process sayHello {
    // No benchmarking profile name is specified. 'Default GPU' profile will be used
    ext timeToCostPriorityRatio: '0.7/0.3'
    container 'nvcr.io/hpc/gromacs:2023.2'

    // No walltime specified, default to 3 hours

    accelerator 1    // Signify that this task requires at least 1 GPU

    // No CPU specified, default to minimum 1 CPU
    // No memory specified, default to minimum 8 GB

    output:
        path 'hello.txt'

    script:
    """
    docker run --rm \
    -v \$PWD:/container_workspace \
    -v /fovus-storage:/fovus-storage \
    -v /fovus/archive:/fovus/archive \
    --workdir /container_workspace \
    --gpus all \
    --ipc=host \
    nvcr.io/hpc/gromacs:2023.2 \
    bash -c "echo 'Hello, World!' > hello.txt"
    """
}

process greeting {
    ext (
        benchmarkingProfileName: 'GROMACS MD Multi-GPU', // Use the 'GROMACS MD' benchmarking profile
        timeToCostPriorityRatio: '0.2/0.8',
        walltimeHours: 6
    )

    // Since GROMACS MD is a GPU benchmarking profile, the accelerator must be specified
    // A limit and a request attributes can be specified:
    // limit: The maximum number of GPUs that can be used for this task
    // request: The minimum number of GPUs that must be available
    accelerator limit: 1, request: 4


    cpus 4          // The number of CPUs required for this task
    memory '2 GB'   // The amount of CPU memory required for this task

    input:
        path "hello.txt"

    output:
        path 'greeting.txt'

    script:
    """
    cat hello.txt > greeting.txt
    echo "Welcome to Fovus!" >> greeting.txt
    """
}

workflow {
    hello_output = sayHello()
    greeting = greeting(hello_output)
}