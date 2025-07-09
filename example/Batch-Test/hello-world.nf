process sayHello {
    // publishDir 'results', mode: 'copy'
    input:
        path helloInput
    output:
        path 'out/output.txt'

    script:
    """
    mkdir -p out
    echo "Hello, World!" > out/output.txt
    sleep 120
    cat $helloInput
    """
}

process sayGoodbye {
    // publishDir 'results', mode: 'copy'
    input:
        path helloInput
    output:
        path 'out/output.txt'

    script:
    """
    mkdir -p out
    echo "Hello, World!" > out/output.txt
    cat $helloInput
    """
}
// process sayGoodbye {
//     // publishDir 'results', mode: 'copy'
//     output:
//         path 'output2.txt'
//
//     script:
//     """
//     echo "Goodbye, World!" > output2.txt
//     """
// }


// process combine {
//     // publishDir 'results', mode: 'copy'
//     input:
//         path sayHello
//         path sayGoodbye
//
//     output:
//         path 'output3.txt'
//
//     script:
//     """
//     cat $sayHello $sayGoodbye > output3.txt
//     """
// }

workflow {
//     tempfile = file('/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Batch-Test/temp.txt')
    def helloInput = channel.fromPath("/root/Jashmin/Code/Nextflow-Plugin/nf-fovus/example/Hello-Workload/*.sh")
    hello_output = sayHello(helloInput)
    hello_output = sayGoodbye(helloInput)
//     goodbye_output = sayGoodbye()
//     combine(hello_output, goodbye_output)
}