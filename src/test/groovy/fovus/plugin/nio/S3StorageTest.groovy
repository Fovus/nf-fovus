package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

/** S3 keys, uploads, downloads and listings of Fovus storage against a stubbed S3 client. */
class S3StorageTest extends Specification {

    private static final String DIR = 'pipelines/p-1-user/dl'

    @TempDir
    Path tempDir

    ListAppender<ILoggingEvent> storageLog
    Level storageLevelBefore

    def cleanup() {
        if (storageLog != null) {
            final logger = LoggerFactory.getLogger(S3Storage) as Logger
            logger.detachAppender(storageLog)
            logger.level = storageLevelBefore
        }
    }

    /** The log of {@link S3Storage}, where a delete that is left in place is reported; undone after the feature. */
    private ListAppender<ILoggingEvent> captureStorageLog() {
        final logger = LoggerFactory.getLogger(S3Storage) as Logger
        storageLevelBefore = logger.level
        logger.level = Level.DEBUG
        storageLog = new ListAppender<ILoggingEvent>()
        storageLog.start()
        logger.addAppender(storageLog)
        return storageLog
    }

    private static List<String> names(Path folder) {
        return Files.list(folder).withCloseable { stream ->
            stream.map { Path p -> p.fileName.toString() }.sorted().collect(Collectors.toList())
        }
    }

    private static S3Entry object(String key) {
        return new S3Entry(key, 1L, null, false)
    }

    private static S3Entry folder(String key) {
        return new S3Entry(key, 0L, null, true)
    }

    def 'the S3 key of #path should be #key'() {
        given:
        def fs = StorageTestSupport.fileSystem()

        expect: 'the object the mount shows at that path; an area root is the area itself, so its folder is <area>/'
        S3Storage.keyOf((FovusPath) fs.getPath(path)) == key

        where:
        path                                         | key
        '/fovus-storage/files'                       | 'files'
        '/fovus-storage/jobs/'                       | 'jobs'
        '/fovus-storage/pipelines'                   | 'pipelines'
        '/fovus-storage/files/data/in.txt'           | 'files/data/in.txt'
        '/fovus-storage/jobs/j-1/out.txt'            | 'jobs/j-1/out.txt'
        '/fovus-storage/pipelines/p-1-user/x/y.txt'  | 'pipelines/p-1-user/x/y.txt'
    }

    def 'a local folder reached through a symlink should upload every real file'() {
        given: 'a folder behind a symlink, holding a symlinked sub-folder'
        def real = Files.createDirectories(tempDir.resolve('real'))
        Files.writeString(real.resolve('a.txt'), 'a')
        Files.writeString(Files.createDirectories(real.resolve('deeper')).resolve('c.txt'), 'c')
        def other = Files.createDirectories(tempDir.resolve('other'))
        Files.writeString(other.resolve('b.txt'), 'b')
        Files.createSymbolicLink(real.resolve('link-to-other'), other)
        def link = Files.createSymbolicLink(tempDir.resolve('link'), real)

        def uploaded = [:]
        def markers = []
        def client = Stub(FovusS3Client) {
            head(_) >> null
            hasChildren(_) >> false
            uploadFile(_, _) >> { Path file, String key -> uploaded[key] = file }
            putDirectoryMarker(_) >> { String key -> markers << key }
        }
        def fs = StorageTestSupport.fileSystem(client)
        def target = fs.getPath('/fovus-storage/pipelines/p-1-user/stage/in')

        when:
        fs.provider().upload(link, target)

        then:
        uploaded.keySet() as Set == ['pipelines/p-1-user/stage/in/a.txt',
                                     'pipelines/p-1-user/stage/in/deeper/c.txt',
                                     'pipelines/p-1-user/stage/in/link-to-other/b.txt'] as Set
        uploaded.values().every { Path file -> Files.isRegularFile(file) }
        markers as Set == ['pipelines/p-1-user/stage/in',
                           'pipelines/p-1-user/stage/in/deeper',
                           'pipelines/p-1-user/stage/in/link-to-other'] as Set
    }

    def 'a folder download should skip keys that would leave the target folder'() {
        given:
        def out = tempDir.resolve('out')
        def entries = [
                folder("${DIR}/"),
                // an absolute local path, and two folders outside the target, if they were honored
                object("${DIR}/" + tempDir.resolve('outside') + '/x.txt'),
                object("${DIR}/../outside2/y.txt"),
                object("${DIR}/a/./b"),
                object("${DIR}//"),
                folder("${DIR}/sub/"),
                object("${DIR}/ok.txt"),
        ]
        def downloaded = []
        def client = Stub(FovusS3Client) {
            head(_) >> null
            hasChildren("${DIR}/".toString()) >> true
            listAll("${DIR}/".toString()) >> entries
            downloadFile(_, _) >> { String key, Path file -> downloaded << [key, file] }
        }
        def fs = StorageTestSupport.fileSystem(client)

        when:
        fs.provider().download(fs.getPath('/fovus-storage/' + DIR), out)

        then: 'only the ordinary entries are fetched'
        downloaded == [["${DIR}/ok.txt".toString(), out.resolve('ok.txt')]]
        Files.isDirectory(out.resolve('sub'))

        and: 'nothing was created outside the target, or for a skipped entry'
        names(tempDir) == ['out']
        names(out) == ['sub']
    }

    def 'a denied delete should warn once in an area, then log at debug level'() {
        given:
        def appender = captureStorageLog()
        def client = Stub(FovusS3Client) {
            head(_) >> { String key -> object(key) }
            delete(_) >> { String key -> throw new AccessDeniedException(FovusS3Client.uri(key), null, "Fovus storage credentials don't allow delete on ${key} (write token)") }
        }
        def fs = StorageTestSupport.fileSystem(client)

        when:
        ['a', 'b', 'c'].each { Files.delete(fs.getPath("/fovus-storage/pipelines/p-1-user/out/${it}.txt")) }

        then:
        def denied = appender.list.findAll { it.formattedMessage.contains('stays in Fovus storage') }
        denied*.level == [Level.WARN, Level.DEBUG, Level.DEBUG]
        denied[0].formattedMessage.contains('out/a.txt')
        denied[2].formattedMessage.contains('out/c.txt')
    }

    def 'a delete the guard refuses should fail, not be left in place with a warning'() {
        given:
        def appender = captureStorageLog()
        def client = Stub(FovusS3Client) {
            checkWritable(_) >> { String key -> throw new AccessDeniedException(FovusS3Client.uri(key), null, 'Fovus storage jobs/ is read-only') }
        }
        def fs = StorageTestSupport.fileSystem(client)

        when:
        Files.delete(fs.getPath('/fovus-storage/jobs/j-1/out.txt'))

        then:
        def e = thrown(AccessDeniedException)
        e.reason == 'Fovus storage jobs/ is read-only'
        appender.list.findAll { it.formattedMessage.contains('stays in Fovus storage') } == []
    }

    def 'a folder move should copy every object under it, with its relative key, before deleting any'() {
        given:
        def client = Mock(FovusS3Client)
        def fs = StorageTestSupport.fileSystem(client)
        def source = fs.getPath('/fovus-storage/pipelines/p-1-user/out')
        def target = fs.getPath('/fovus-storage/pipelines/p-1-user/moved')

        when:
        Files.move(source, target)

        then: 'the source is a folder and the target is free'
        client.hasChildren('pipelines/p-1-user/out/') >> true
        client.hasChildren('pipelines/p-1-user/moved/') >> false
        client.listAll('pipelines/p-1-user/out/') >> [folder('pipelines/p-1-user/out/'), object('pipelines/p-1-user/out/a.txt'),
                                                      folder('pipelines/p-1-user/out/sub/'), object('pipelines/p-1-user/out/sub/b.txt')]

        then:
        1 * client.putDirectoryMarker('pipelines/p-1-user/moved/')
        1 * client.copy('pipelines/p-1-user/out/a.txt', 'pipelines/p-1-user/moved/a.txt', 1L)
        1 * client.putDirectoryMarker('pipelines/p-1-user/moved/sub/')
        1 * client.copy('pipelines/p-1-user/out/sub/b.txt', 'pipelines/p-1-user/moved/sub/b.txt', 1L)

        then:
        1 * client.delete('pipelines/p-1-user/out/')
        1 * client.delete('pipelines/p-1-user/out/a.txt')
        1 * client.delete('pipelines/p-1-user/out/sub/')
        1 * client.delete('pipelines/p-1-user/out/sub/b.txt')
    }

    def 'a folder move that fails while copying should delete nothing'() {
        given:
        def client = Mock(FovusS3Client)
        def fs = StorageTestSupport.fileSystem(client)

        when:
        Files.move(fs.getPath('/fovus-storage/pipelines/p-1-user/out'), fs.getPath('/fovus-storage/pipelines/p-1-user/moved'))

        then:
        client.hasChildren('pipelines/p-1-user/out/') >> true
        client.listAll('pipelines/p-1-user/out/') >> [object('pipelines/p-1-user/out/a.txt'), object('pipelines/p-1-user/out/b.txt')]
        client.copy('pipelines/p-1-user/out/b.txt', _, _) >> { throw new IOException('S3 read failed') }
        def e = thrown(IOException)
        e.message == 'S3 read failed'
        0 * client.delete(_)
    }

    def 'a folder move should warn once and stop deleting when the write token cannot delete'() {
        given:
        def appender = captureStorageLog()
        def deleted = []
        def client = Stub(FovusS3Client) {
            head(_) >> null
            hasChildren('pipelines/p-1-user/out/') >> true
            listAll('pipelines/p-1-user/out/') >> [object('pipelines/p-1-user/out/a.txt'), object('pipelines/p-1-user/out/b.txt')]
            delete(_) >> { String key ->
                deleted << key
                throw new AccessDeniedException(FovusS3Client.uri(key), null, "Fovus storage credentials don't allow delete on ${key} (write token)")
            }
        }
        def fs = StorageTestSupport.fileSystem(client)

        when:
        Files.move(fs.getPath('/fovus-storage/pipelines/p-1-user/out'), fs.getPath('/fovus-storage/pipelines/p-1-user/moved'))

        then:
        noExceptionThrown()
        deleted == ['pipelines/p-1-user/out/a.txt']
        def denied = appender.list.findAll { it.formattedMessage.contains('stays in Fovus storage') }
        denied*.level == [Level.WARN]
        denied[0].formattedMessage.contains('fovus:///fovus-storage/pipelines/p-1-user/out stays in Fovus storage')
    }

    def 'a folder listing should not return the folder itself for an empty name'() {
        given:
        def client = Stub(FovusS3Client) {
            list('pipelines/p-1-user/sample/') >> [folder('pipelines/p-1-user/sample//'),
                                                   object('pipelines/p-1-user/sample/y.txt')]
        }
        def fs = StorageTestSupport.fileSystem(client)

        expect:
        names(fs.getPath('/fovus-storage/pipelines/p-1-user/sample')) == ['y.txt']
    }
}
