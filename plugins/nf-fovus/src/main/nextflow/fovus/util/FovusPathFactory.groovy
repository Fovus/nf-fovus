/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.fovus.util

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.fovus.nio.FovusPath
import nextflow.file.FileHelper
import nextflow.file.FileSystemPathFactory

import java.nio.file.Path

/**
 * Implements the a factory strategy to parse and build S3 path URIs
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class FovusPathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String str) {
        // normalise 's3' path
        log.debug "ParseURI: $str"
        if (str.startsWith('fovus://') && str[8] != '/') {
            final path = "fovus:///${str.substring(8)}"
            return create(path)
        }
        return null
    }

    static private Map config() {
        final result = Global.config?.get('fovus') as Map
        return result != null ? result : Collections.emptyMap()
    }

    @Override
    protected String toUriString(Path path) {
        return path instanceof FovusPath ? "fovus:/$path".toString() : null
    }

    @Override
    protected String getBashLib(Path target) {
        return ""
    }

    @Override
    protected String getUploadCmd(String source, Path target) {
        return null
    }

    /**
     * Creates a {@link FovusPath} from a S3 formatted URI.
     *
     * @param path
     *      A S3 URI path e.g. s3:///BUCKET_NAME/some/data.
     *      NOTE it expect the s3 prefix provided with triple `/` .
     *      This is required by the underlying implementation expecting the host name in the URI to be empty
     *      and the bucket name to be the first path element
     * @return
     *      The corresponding {@link FovusPath}
     */
    static FovusPath create(String path) {
        if (!path) throw new IllegalArgumentException("Missing S3 path argument")
        if (!path.startsWith('fovus:///')) throw new IllegalArgumentException("S3 path must start with fovus:/// prefix -- offending value '$path'")
        // note: this URI constructor parse the path parameter and extract the `scheme` and `authority` components
        final uri = new URI(null, null, path, null, null)

        log.debug "ParseURI create: $path"
        return (FovusPath) FileHelper.getOrCreateFileSystemFor(uri, config()).provider().getPath(uri)
    }
}