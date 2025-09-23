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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jsoup.helper.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;

/**
 * S3 iterator over folders at first level.
 * Future versions of this class should be return the elements
 * in a incremental way when the #next() method is called.
 */
public class FovusS3Iterator implements Iterator<Path> {

    private static final Logger log = LoggerFactory.getLogger(FovusS3Iterator.class);

    private FovusFileSystem s3FileSystem;

    private String bucket;

    private String key;

    /**
     * The path to the input folder that we want to interate over
     */
    FovusPath fovusPath;

    private Iterator<FovusPath> it;

    public FovusS3Iterator(String key, FovusPath s3Path) {
        String bucket = s3Path.getBucket();
        FovusFileSystem s3FileSystem = s3Path.getFileSystem();

        Preconditions.checkArgument(key != null && key.endsWith("/"), "key %s should be ended with slash '/'", key);

        this.bucket = bucket;
        // the only case i don't need the end slash is to list buckets content
        this.key = key.length() == 1 ? "" : key;
        this.s3FileSystem = s3FileSystem;
        this.fovusPath = s3Path;
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
            // TODO: Need to support paginated requests
            List<ObjectMetaData> objectMetaDataList = s3FileSystem.getClient().listFileObjects(key.substring(0, key.length() - 1), fovusPath.getFileJobId());

//            while (current.isTruncated()) {
//                // parse the elements
//                parseObjectListing(listPath, current);
//                // continue
//                current = s3FileSystem.getClient().listNextBatchOfObjects(current);
//            }

            parseObjectListing(listPath, objectMetaDataList);

            it = listPath.iterator();
        }

        return it;
    }

    private ListObjectsRequest buildRequest() {

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(key);
        request.setMarker(key);
        request.setDelimiter("/");
        return request;
    }

    /**
     * add to the listPath the elements at the same level that s3Path
     *
     * @param listPath           List not null list to add
     * @param objectMetaDataList List<ObjectMetaData> to walk
     */
    private void parseObjectListing(List<FovusPath> listPath, List<ObjectMetaData> objectMetaDataList) {
        log.trace("parseObjectListing {}", bucket);
        Set<String> folders = new HashSet<>();
        for (final ObjectMetaData objectMetaData : objectMetaDataList) {
            String metaDataKey = objectMetaData.getKey();
            List<String> metaDataKeyParts = Arrays.stream(metaDataKey.split("/")).toList();

            // Construct the folders from the last one (ignore the file name) to the first one by looping through the parts index from end to start
            // If folder already exists, break the loop
            for (int i = metaDataKeyParts.size() - 2; i >= 0; i--) {
                String folder = String.join("/", metaDataKeyParts.subList(0, i + 1));
                if (folders.contains(folder)) {
                    break;
                }
                folders.add(folder);
            }

            List<String> fovusS3PathParts = new ArrayList<>();
            fovusS3PathParts.add(fovusPath.getPipelineId());

            if (metaDataKeyParts.get(0).equals("jobs")) {
                if (fovusPath.getFileJobId() == null) {
                    throw new ValidationException("[FOVUS] Expecting job file but parent folder is not a job directory");
                }

                // Add the hashed bucket directory
                String taskHasDir = fovusPath.getParts().get(1);
                fovusS3PathParts.add(taskHasDir);
            }

            // Adding the remaining parts starting with the actual task's working directory or stage-*/
            fovusS3PathParts.addAll(metaDataKeyParts.subList(2, metaDataKeyParts.size()));
            log.trace("+++ fovusfovusS3PathParts {}", fovusS3PathParts);

            FovusPath path = new FovusPath(s3FileSystem, "/" + bucket, String.join("/", fovusS3PathParts));

            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setBucketName(path.getBucket());
            summary.setETag(objectMetaData.getETag());

            if (objectMetaData.getKey().endsWith("/")) {
                summary.setKey(path.getKey() + "/");
            } else {
                summary.setKey(path.getKey());
            }
            summary.setLastModified(objectMetaData.getLastModified());
            summary.setSize(objectMetaData.getSize());
            path.setObjectSummary(summary);

            listPath.add(path);
        }

        for (String folder : folders) {
            String folderPath = "";
            if (folder.equals("jobs") || folder.equals("files")) {
                List<String> parts = Arrays.stream(folder.split("/")).toList();

                if (parts.size() <= 2) {
                    continue;
                }

                if (folder.startsWith("jobs")) {
                    // pipelineId / taskHash / jobId
                    folderPath = fovusPath.getPipelineId() + "/" + fovusPath.getParts().get(1) + "/";
                } else {
                    // Whatever after the files from the folder name
                    folderPath = fovusPath.getPipelineId() + "/";
                }

                folderPath += String.join("/", parts.subList(2, parts.size()));

                listPath.add(new FovusPath(s3FileSystem, "/" + bucket, folderPath));
            }
        }
    }

    /**
     * The current #buildRequest() get all subdirectories and her content.
     * This method filter the keyChild and check if is a immediate
     * descendant of the keyParent parameter
     *
     * @param keyParent String
     * @param keyChild  String
     * @return String parsed
     * or null when the keyChild and keyParent are the same and not have to be returned
     */
    @Deprecated
    private String getInmediateDescendent(String keyParent, String keyChild) {

        keyParent = deleteExtraPath(keyParent);
        keyChild = deleteExtraPath(keyChild);

        final int parentLen = keyParent.length();
        final String childWithoutParent = deleteExtraPath(keyChild
                .substring(parentLen));

        String[] parts = childWithoutParent.split("/");

        if (parts.length > 0 && !parts[0].isEmpty()) {
            return keyParent + "/" + parts[0];
        } else {
            return null;
        }

    }

    @Deprecated
    private String deleteExtraPath(String keyChild) {
        if (keyChild.startsWith("/")) {
            keyChild = keyChild.substring(1);
        }
        if (keyChild.endsWith("/")) {
            keyChild = keyChild.substring(0, keyChild.length() - 1);
        }
        return keyChild;
    }
}
