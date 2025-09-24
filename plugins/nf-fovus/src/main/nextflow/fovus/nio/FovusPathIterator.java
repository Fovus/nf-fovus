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

package nextflow.fovus.nio;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Iterator over folders at first level of a FovusPath.
 * Future versions of this class should be return the elements
 * in an incremental way when the #next() method is called.
 */
public class FovusPathIterator implements Iterator<Path> {

    private static final Logger log = LoggerFactory.getLogger(FovusPathIterator.class);

    private final FovusFileSystem fovusFileSystem;

    private final String key;

    /**
     * The path to the input folder that we want to iterate over
     */
    FovusPath fovusPath;

    private Iterator<FovusPath> it;

    public FovusPathIterator(String key, FovusPath fovusPath) {
        FovusFileSystem fovusFileSystem = fovusPath.getFileSystem();

        Preconditions.checkArgument(key != null && key.endsWith("/"), "key %s should be ended with slash '/'", key);


        // Here, "" is used for listing all objects at /fovus-storage/{fileType}
        this.key = key.length() == 1 ? "" : key;
        this.fovusFileSystem = fovusFileSystem;
        this.fovusPath = fovusPath;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FovusPath next() {
        return getIterator().next();
    }

    @Override
    public boolean hasNext() {
        return getIterator().hasNext();
    }

    private Iterator<FovusPath> getIterator() {
        if (it == null) {
            List<FovusPath> listPath = new ArrayList<>();

            // iterator over this list
            // TODO: Support paginated requests
            List<FovusFileMetadata> fovusFileMetadataList = fovusFileSystem.getClient().listFileObjects(key.substring(0, key.length() - 1), fovusPath.getFileJobId());

            parseObjectListing(listPath, fovusFileMetadataList);

            it = listPath.iterator();
        }

        return it;
    }

    /**
     * add to the listPath the elements at the same level that fovusPath
     *
     * @param listPath           List not null list to add
     * @param fovusFileMetadataList List<ObjectMetadata> to walk
     */
    private void parseObjectListing(List<FovusPath> listPath, List<FovusFileMetadata> fovusFileMetadataList) {
        Set<String> folders = new HashSet<>();
        for (final FovusFileMetadata fovusFileMetadata : fovusFileMetadataList) {
            String metaDataKey = fovusFileMetadata.getKey(); // E.g, files/folder1/text.txt
            FovusPath path = new FovusPath(fovusFileSystem, FovusPath.FOVUS_PATH_PREFIX + "/" + metaDataKey);
            listPath.add(path);


            if (fovusFileMetadata.getKey().endsWith("/")) {
                fovusFileMetadata.setKey(path.getKey() + "/");
            } else {
                fovusFileMetadata.setKey(path.getKey());
            }
            path.setFileMetadata(fovusFileMetadata);

            Path parentPath = path.getParent();
            // Only loop until we reach the root or if the parentPath is fovusPath
            while (parentPath != null && !parentPath.equals(fovusPath)) {
                String parentKey = ((FovusPath) parentPath).getKey();
                if (folders.contains(parentKey)) break;

                folders.add(parentKey);
                listPath.add((FovusPath) parentPath);
                parentPath = parentPath.getParent();
            }
        }
    }
}
