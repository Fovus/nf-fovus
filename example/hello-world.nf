process sayHello {

//     publishDir 'results', mode: 'copy'
    input:
        path helloInput
    output:
        path 'output1.txt'

    script:
    """
    echo "Hello" > output1.txt
    ls -la
    cd Hello-Workload
    ls -la
    ./hello.sh
    cat text.out
    """
}


process sayWorld {

//     publishDir 'results', mode: 'copy'
    input:
        path worldInput
        path helloOutput
    output:
        path 'output2.txt'

    script:
    """
    echo "World" > output2.txt
    ls -la
    cat output1.txt
    cd World-Workload
    ls -la
    ./hello.sh
    cat text.out
    ls -la
    echo "World!" > output1.txt
    """
}



workflow {
    def helloInput = file("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Hello-Workload")
    def helloOutput = sayHello(helloInput)
    def worldInput = file("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/World-Workload")
    sayWorld(worldInput, helloOutput)
}