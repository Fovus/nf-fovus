package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import spock.lang.Specification

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class FovusPipelinesAreaTest extends Specification {

    def 'pipelines paths should parse and print as the compute node sees them'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()

        when:
        def path = (FovusPath) fs.getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')

        then:
        path.fileType == 'pipelines'
        path.key == 'p-1-user/fovus-work/ab/cdef'
        path.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.toUri().toString() == 'fovus:///fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.parent.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab'
        !fs.isReadOnly()
    }

    def 'the area root should be the direct-mode work directory'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()

        expect:
        (fs.getPath('/fovus-storage/pipelines') as FovusPath).isPipelinesAreaRoot()
        (fs.getPath('/fovus-storage/pipelines/') as FovusPath).isPipelinesAreaRoot()
        !(fs.getPath('/fovus-storage/pipelines/p-1-user') as FovusPath).isPipelinesAreaRoot()
    }

    def 'Nextflow can create and check the work directory before any credentials exist'() {
        given:
        def root = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        Files.createDirectories(root)

        then:
        Files.exists(root)
        Files.isDirectory(root)
    }

    def 'any other access before the S3 client is attached should explain direct mode'() {
        given:
        def path = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines/p-1-user/x')

        when:
        Files.readAllBytes(path)

        then:
        def e = thrown(IllegalStateException)
        e.message == FovusFileSystem.NOT_ATTACHED_MESSAGE
    }

    def 'the files area should stay read-only'() {
        given:
        def fs = new FovusFileSystemProvider().newFileSystem(URI.create('fovus:///fovus-storage/files'),
                                                             [pipelineName: 'test-pipeline'])

        expect:
        fs.isReadOnly()
    }

    def 'a delete the write token does not allow should only warn'() {
        given:
        def client = Stub(FovusS3Client) {
            head(_) >> new S3Entry('pipelines/p-1-user/x', 1L, null, false)
            delete(_) >> {
                throw new AccessDeniedException('fovus:///fovus-storage/pipelines/p-1-user/x', null,
                                                "Fovus storage credentials don't allow delete on pipelines/p-1-user/x (write token)")
            }
        }
        def path = PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/x')

        when:
        Files.delete(path)

        then:
        noExceptionThrown()
    }

    // The two cases below stand in for the MinIO integration tests (PipelinesStorageIT), which are not run here.

    def 'an empty file should be a regular file of size 0'() {
        given:
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/.command.err') >> new S3Entry('pipelines/p-1-user/.command.err', 0L, null, false)
        }
        def empty = PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/.command.err')

        expect:
        Files.exists(empty)
        Files.isRegularFile(empty)
        !Files.isDirectory(empty)
        Files.size(empty) == 0
    }

    def 'names that share a prefix should not be confused'() {
        given: 'out.txt.bak exists, but neither an out.txt object nor an out.txt/ folder does'
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/out.txt') >> null
            hasChildren('pipelines/p-1-user/out.txt/') >> false
            list('pipelines/p-1-user/sample/') >> [new S3Entry('pipelines/p-1-user/sample/y.txt', 1L, null, false)]
        }
        def fs = PipelinesTestSupport.fileSystem(client)

        expect:
        !Files.exists(fs.getPath('/fovus-storage/pipelines/p-1-user/out.txt'))
        names(fs.getPath('/fovus-storage/pipelines/p-1-user/sample')) == ['y.txt']
    }

    private static List<String> names(Path folder) {
        return Files.list(folder).withCloseable { stream ->
            stream.map { Path p -> p.fileName.toString() }.sorted().collect(Collectors.toList())
        }
    }
}
