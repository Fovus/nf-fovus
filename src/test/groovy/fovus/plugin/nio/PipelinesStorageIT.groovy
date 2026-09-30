package fovus.plugin.nio

import fovus.plugin.s3.CountingInterceptor
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.MinioSupport
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.stream.Collectors

@Tag('integration')
class PipelinesStorageIT extends Specification {

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared CountingInterceptor requests = new CountingInterceptor()

    @TempDir
    Path tempDir

    FovusPath dir

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio, requests)
    }

    def cleanupSpec() {
        s3?.close()
        minio?.stop()
    }

    def setup() {
        // a page size of 2 makes every listing below span several pages
        final fs = PipelinesTestSupport.fileSystem(MinioSupport.fovusClient(s3, FovusS3Client.MIN_PART_SIZE, 2))
        dir = (FovusPath) fs.getPath("/fovus-storage/pipelines/p-1-user/${UUID.randomUUID()}")
        Files.createDirectories(dir)
        requests.reset()
    }

    private static List<String> names(Path folder) {
        return Files.list(folder).withCloseable { stream ->
            stream.map { Path p -> p.fileName.toString() }.sorted().collect(Collectors.toList())
        }
    }

    def 'a file written the way Nextflow writes .command.run should read back'() {
        given:
        def file = dir.resolve('.command.run')

        when:
        Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                .withCloseable { it.write('#!/bin/bash\necho hi\n') }

        then:
        file.text == '#!/bin/bash\necho hi\n'
        Files.size(file) == 21
        Files.isRegularFile(file)
    }

    def 'a created folder should exist before anything is written into it'() {
        given:
        def folder = dir.resolve('ab/cdef')
        assert !Files.exists(folder)

        when:
        Files.createDirectories(folder)

        then:
        Files.exists(folder)
        Files.isDirectory(folder)
        names(folder) == []
    }

    def 'listing should span pages and report files and sub-folders'() {
        given:
        (1..5).each { Files.writeString(dir.resolve("file${it}.txt"), "content ${it}") }
        Files.writeString(dir.resolve('sub/inner.txt'), 'inner')

        expect:
        names(dir) == ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file5.txt', 'sub']
        Files.isDirectory(dir.resolve('sub'))
    }

    def 'a glob walk like output collection should not look up each file'() {
        given:
        ['a.txt', 'b.txt', 'c.log', 'nested/d.txt', 'nested/deeper/e.txt'].each { Files.writeString(dir.resolve(it), it) }
        requests.reset()
        def found = []

        when:
        FileHelper.visitFiles([type: 'file'], dir, '**.txt') { Path p -> found << dir.relativize(p).toString() }

        then:
        found.sort() == ['a.txt', 'b.txt', 'nested/d.txt', 'nested/deeper/e.txt']
        requests.count('HeadObjectRequest') <= 1
    }

    def 'names with spaces, symbols and non-ASCII letters should round-trip'() {
        given:
        def fileNames = ['my file (1).txt', 'a#b+c%d=e.txt', 'résumé.txt']

        when:
        fileNames.each { Files.writeString(dir.resolve(it), it) }

        then:
        names(dir) == fileNames.sort(false)
        fileNames.every { dir.resolve(it).text == it }
    }

    def 'an empty file should be a regular file of size 0'() {
        given:
        def empty = dir.resolve('.command.err')

        when:
        Files.write(empty, new byte[0])

        then:
        Files.exists(empty)
        Files.isRegularFile(empty)
        !Files.isDirectory(empty)
        Files.size(empty) == 0
        empty.text == ''
    }

    def 'names that share a prefix should not be confused'() {
        given:
        Files.writeString(dir.resolve('out.txt.bak'), 'backup')
        Files.writeString(dir.resolve('sample_2/x.txt'), 'x')
        Files.writeString(dir.resolve('sample/y.txt'), 'y')

        expect:
        !Files.exists(dir.resolve('out.txt'))
        names(dir.resolve('sample')) == ['y.txt']
    }

    def 'writes outside this pipeline should be refused and reads should find nothing'() {
        given:
        def other = dir.fileSystem.getPath('/fovus-storage/pipelines/p-2-user/x.txt')

        when:
        Files.writeString(other, 'nope')

        then:
        thrown(AccessDeniedException)
        !Files.exists(other)
    }

    def 'delete should remove files and deletePath should remove folders'() {
        given:
        Files.writeString(dir.resolve('a.txt'), 'a')
        Files.writeString(dir.resolve('sub/b.txt'), 'b')

        when:
        Files.delete(dir.resolve('a.txt'))
        FileHelper.deletePath(dir.resolve('sub'))

        then:
        !Files.exists(dir.resolve('a.txt'))
        !Files.exists(dir.resolve('sub'))
    }

    def 'copy and move should work inside the pipeline'() {
        given:
        Files.writeString(dir.resolve('a.txt'), 'a')

        when:
        Files.copy(dir.resolve('a.txt'), dir.resolve('b.txt'))
        Files.move(dir.resolve('b.txt'), dir.resolve('c.txt'))

        then:
        dir.resolve('a.txt').text == 'a'
        !Files.exists(dir.resolve('b.txt'))
        dir.resolve('c.txt').text == 'a'
    }

    def 'local files and folders should upload and download the way Nextflow copies them'() {
        given:
        def input = Files.writeString(tempDir.resolve('input.txt'), 'hello')
        def bin = Files.createDirectories(tempDir.resolve('bin/lib'))
        Files.writeString(tempDir.resolve('bin/tool.sh'), '#!/bin/bash')
        Files.writeString(bin.resolve('helper.py'), 'x')
        Files.createDirectories(dir.resolve('tmp'))

        when: 'an input is staged, the bin folder is uploaded and a folder is published locally'
        FileHelper.copyPath(input, dir.resolve('stage/input.txt'))
        FilesEx.copyTo(tempDir.resolve('bin'), dir.resolve('tmp'))
        FileHelper.copyPath(dir.resolve('tmp/bin'), tempDir.resolve('published'))

        then:
        Files.size(dir.resolve('stage/input.txt')) == Files.size(input)
        dir.resolve('tmp/bin/tool.sh').text == '#!/bin/bash'
        tempDir.resolve('published/tool.sh').text == '#!/bin/bash'
        tempDir.resolve('published/lib/helper.py').text == 'x'
    }
}
