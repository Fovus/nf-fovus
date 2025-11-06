/*
 * Copyright 2020-2022, Seqera Labs
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
 *
 */

package fovus.plugin.util;

import java.nio.file.NoSuchFileException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import fovus.plugin.job.FovusJobClient;
import fovus.plugin.nio.FovusPath;
import fovus.plugin.nio.FovusFileMetadata;

public class FovusFileMetadataLookup {

    private static final Logger log = LoggerFactory.getLogger(FovusFileMetadataLookup.class);

    /**
     * Get the {@link FovusFileMetadata} that represent this Path or its first child if the path does not exist
     *
     * @param fovusPath {@link FovusPath}
     * @return {@link FovusFileMetadata}
     * @throws NoSuchFileException if not found the path and any child
     */
    public FovusFileMetadata lookup(FovusPath fovusPath) throws NoSuchFileException {

        /*
         * check is file meta-data that has been cached
         */
        FovusFileMetadata fileMetadata = fovusPath.getFileMetadata();
        if (fileMetadata != null) {
            return fileMetadata;
        }

        final FovusJobClient fovusJobClient = fovusPath.getFileSystem().getJobClient();
        /*
         * when `key` is an empty string retrieve the object meta-data of the fileType/ directory
         */
        if ("".equals(fovusPath.getKey())) {
            FovusFileMetadata meta = fovusJobClient.getFileObject(fovusPath.getFileType(), "");
            if (meta == null)
                throw new NoSuchFileException("fovus:/" + FovusPath.FOVUS_PATH_PREFIX + "/" + fovusPath.getFileType());

            meta.setKey(fovusPath.getKey());
            return meta;
        } else {
            List<FovusFileMetadata> metaDataList = fovusJobClient.listFileObjects(fovusPath.getFileType(), fovusPath.getKey());
            log.trace("metaDataList: {}", metaDataList);
            if (metaDataList == null || metaDataList.isEmpty()) {
                throw new NoSuchFileException("fovus://" + fovusPath.getKey());
            }

            for (FovusFileMetadata fovusFileMetadata : metaDataList) {
                if (matchName(fovusPath, fovusFileMetadata)) {
                    if (fovusFileMetadata.getKey().endsWith("/")) {
                        fovusFileMetadata.setKey(fovusPath.getKey() + "/");
                    } else {
                        fovusFileMetadata.setKey(fovusPath.getKey());
                    }
                    return fovusFileMetadata;
                }
            }
        }

        log.trace("Throwing NoSuchFileException");
        throw new NoSuchFileException("fovus:/" + FovusPath.FOVUS_PATH_PREFIX + "/" + fovusPath.getFileType() + "/" + fovusPath.getKey());

    }

    private boolean matchName(FovusPath fovusPath, FovusFileMetadata fovusFileMetadata) {
        String foundKey = fovusFileMetadata.getKey();
        String remoteFilePath = fovusPath.toRemoteFilePath();
        log.trace("+++ Remote file path {}", remoteFilePath);

        // they are different names return false
        if (!foundKey.startsWith(remoteFilePath)) {
            return false;
        }

        // when they are the same length, they are identical
        if (foundKey.length() == remoteFilePath.length())
            return true;

        return foundKey.charAt(remoteFilePath.length()) == '/';
    }
}
