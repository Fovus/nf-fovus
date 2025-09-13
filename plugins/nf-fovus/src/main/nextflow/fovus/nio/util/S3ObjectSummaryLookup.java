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

package nextflow.fovus.nio.util;

import com.amazonaws.services.s3.model.*;
import nextflow.fovus.FovusClient;
import nextflow.fovus.nio.ObjectMetaData;
import nextflow.fovus.nio.S3Client;
import nextflow.fovus.nio.FovusS3Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

public class S3ObjectSummaryLookup {

    private static final Logger log = LoggerFactory.getLogger(S3ObjectSummary.class);

    /**
     * Get the {@link com.amazonaws.services.s3.model.S3ObjectSummary} that represent this Path or its first child if the path does not exist
     *
     * @param s3Path {@link FovusS3Path}
     * @return {@link com.amazonaws.services.s3.model.S3ObjectSummary}
     * @throws NoSuchFileException if not found the path and any child
     */
    public S3ObjectSummary lookup(FovusS3Path s3Path) throws NoSuchFileException {

        /*
         * check is object summary has been cached
         */
        S3ObjectSummary summary = s3Path.fetchObjectSummary();
        if (summary != null) {
            return summary;
        }

        final FovusClient client = s3Path.getFileSystem().getClient();
        System.out.println("lookup: s3Path " + s3Path.toString());
        System.out.println("===== Lookup Key:" + s3Path.getKey());
        System.out.println("checking");
        /*
         * when `key` is an empty string retrieve the object meta-data of the bucket
         */
        if ("".equals(s3Path.getKey())) {
            System.out.println("lookup - s3Path.getKey is empty");
            ObjectMetaData meta = client.getFileObject(s3Path.getBucket(), "");
            if (meta == null)
                throw new NoSuchFileException("s3://" + s3Path.getBucket());

            summary = new S3ObjectSummary();
            summary.setBucketName(s3Path.getBucket());
            summary.setETag(meta.getETag());
            summary.setKey(s3Path.getKey());
            summary.setLastModified(meta.getLastModified());
            summary.setSize(meta.getSize());
            return summary;
        } else {
            System.out.println("calling getFileObject");
            List<ObjectMetaData> metaDataList = client.listFileObjects(s3Path.getKey(), s3Path.getFileJobId());
            log.trace("metaDataList: {}", metaDataList);
            System.out.println("metaDataList: " + metaDataList);
            if (metaDataList == null || metaDataList.isEmpty()) {
                throw new NoSuchFileException("s3://" + s3Path.getKey());
            }

            for (ObjectMetaData objectMetaData : metaDataList) {
                if (matchName(s3Path, objectMetaData)) {
                    summary = new S3ObjectSummary();
                    summary.setBucketName(s3Path.getBucket());
                    summary.setETag(objectMetaData.getETag());

                    if (objectMetaData.getKey().endsWith("/")) {
                        summary.setKey(s3Path.getKey() + "/");
                    } else {
                        summary.setKey(s3Path.getKey());
                    }
                    summary.setLastModified(objectMetaData.getLastModified());
                    summary.setSize(objectMetaData.getSize());
                    return summary;
                }
            }
        }

        System.out.println("Throwing NoSuchFileException");
        log.trace("Throwing NoSuchFileException");
        throw new NoSuchFileException("s3://" + s3Path.getBucket() + "/" + s3Path.getKey());

    }

    private boolean matchName(FovusS3Path s3Path, ObjectMetaData objectMetaData) {
        String foundKey = objectMetaData.getKey();
        String remoteFilePath = s3Path.toRemoteFilePath();
        log.trace("+++ Remote file path {}", remoteFilePath);
        System.out.println("+++ Remote file path " + remoteFilePath);

        // they are different names return false
        if (!foundKey.startsWith(remoteFilePath)) {
            return false;
        }

        // when they are the same length, they are identical
        if (foundKey.length() == remoteFilePath.length())
            return true;

        return foundKey.charAt(remoteFilePath.length()) == '/';
    }

//    public ObjectMetadata getS3ObjectMetadata(FovusS3Path s3Path) {
//        FovusClient client = s3Path.getFileSystem().getClient();
//        try {
//            return client.getObjectMetadata(s3Path.getBucket(), s3Path.getKey());
//        }
//        catch (AmazonS3Exception e){
//            if (e.getStatusCode() != 404){
//                throw e;
//            }
//            return null;
//        }
//    }

//    /**
//     * get S3Object represented by this S3Path try to access with or without end slash '/'
//     * @param s3Path S3Path
//     * @return S3Object or null if it does not exist
//     */
//    @Deprecated
//    private S3Object getS3Object(FovusS3Path s3Path){
//
//        S3Client client = s3Path.getFileSystem()
//                .getClient();
//
//        S3Object object = getS3Object(s3Path.getBucket(), s3Path.getKey(), client);
//
//        if (object != null) {
//            return object;
//        }
//        else{
//            return getS3Object(s3Path.getBucket(), s3Path.getKey() + "/", client);
//        }
//    }

//    /**
//     * get s3Object with S3Object#getObjectContent closed
//     * @param bucket String bucket
//     * @param key String key
//     * @param client S3Client client
//     * @return S3Object
//     */
//    private S3Object getS3Object(String bucket, String key, S3Client client){
//        try {
//            S3Object object = client .getObject(bucket, key);
//            if (object.getObjectContent() != null){
//                try {
//                    object.getObjectContent().close();
//                }
//                catch (IOException e ) {
//                    log.debug("Error while closing S3Object for bucket: `{}` and key: `{}` -- Cause: {}",bucket, key, e.getMessage());
//                }
//            }
//            return object;
//        }
//        catch (AmazonS3Exception e){
//            if (e.getStatusCode() != 404){
//                throw e;
//            }
//            return null;
//        }
//    }
}
