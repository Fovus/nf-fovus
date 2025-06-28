process sayHello {
    // No benchmarking profile name is specified. 'Default CPU' profile will be used
    ext timeToCostPriorityRatio: '0.2/0.8'
    container 'ubuntu:20.04'

    // No walltime specified, default to 3 hours

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
    ubuntu:20.04 \
    bash -c "echo 'Hello, World!' > hello.txt"
    """
}

process greeting {
    ext (
        benchmarkingProfileName: 'AutoDock Vina', // Use the AutoDock Vina profile
        timeToCostPriorityRatio: '0.2/0.8',
        walltimeHours: 6
    )


    cpus 4          // The number of CPUs required for this taskß
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