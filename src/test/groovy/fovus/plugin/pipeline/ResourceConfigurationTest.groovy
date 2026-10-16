package fovus.plugin.pipeline

import spock.lang.Specification
import spock.lang.Unroll

class ResourceConfigurationTest extends Specification {

    @Unroll
    def 'isResolvedValue(#description) is #expected'() {
        expect:
        ResourceConfiguration.isResolvedValue(value) == expected

        where:
        description              | value                                                                  | expected
        'null'                   | null                                                                   | true
        'plain text'             | 'Default CPU'                                                          | true
        'number'                 | 4                                                                      | true
        'boolean'                | false                                                                  | true
        'resolved list'          | ['a', 'b']                                                             | true
        'resolved map'           | [key: 'a', other: 2]                                                   | true
        'closure'                | { 'x' }                                                                | false
        'config object'          | new ConfigObject()                                                     | false
        'closure text'           | '_nf_config_4553341a$_run_closure6$_closure22$_closure38@6fde6f05'     | false
        'closure class text'     | 'Foo$_closure3@1a2b'                                                   | false
        'run closure class text' | 'Foo$_run_closure3@1a2b'                                               | false
        'list with closure'      | ['a', { 'x' }]                                                         | false
        'map with config object' | [key: new ConfigObject()]                                              | false
        'nested unresolved'      | [key: ['a', '_nf_config_1$_closure2@3']]                               | false
    }

    def 'hasOnlyResolvedValues is true for a clean configuration'() {
        expect:
        new ResourceConfiguration(benchmarkingProfileName: 'Default CPU', minvCpu: 2, storageConnectors: ['s3'])
                .hasOnlyResolvedValues()
    }

    def 'hasOnlyResolvedValues is false when a field was built from a closure'() {
        given: 'a closure nested in a closure, named like the ones Nextflow config produces'
        final Closure outerClosure = { Closure innerClosure = { 'x' }; innerClosure }
        final nestedClosure = outerClosure()

        expect:
        !new ResourceConfiguration(benchmarkingProfileName: nestedClosure).hasOnlyResolvedValues()
    }
}
