process sayHello {

//     publishDir 'results', mode: 'copy'
    output:
        path 'output1.txt'

    script:
    """
    echo "Hello, World!" > output1.txt
    """
}

// process sayGoodbye {
//
//     publishDir 'results', mode: 'copy'
//     output:
//         path 'output2.txt'
//
//     script:
//     """
//     echo "Goodbye, World!" > output2.txt
//     """
// }
//
// process greeting {
//     publishDir 'results', mode: 'copy'
//
//     input:
//         path "output1.txt"
//         path "output2.txt"
//
//
//     output:
//         path 'output3.txt'
//
//     script:
//     """
//     cat output1.txt output2.txt > output3.txt
//     """
// }

workflow {
    hello = sayHello()
//     goodbye = sayGoodbye()
//     greeting(hello, goodbye)
}