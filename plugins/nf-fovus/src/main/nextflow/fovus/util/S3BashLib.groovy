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

import com.amazonaws.services.s3.model.CannedAccessControlList
import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.executor.BashFunLib

/**
 * AWS S3 helper class
 */
@CompileStatic
class S3BashLib extends BashFunLib<S3BashLib> {

    private String storageClass = ''
    private String storageEncryption = ''
    private String storageKmsKeyId = ''
    private String retryMode
    private String s5cmdPath
    private String acl = ''
    private String requesterPays = ''


    protected String retryEnv() {
        if( !retryMode )
            return ''
        """
        # aws cli retry config
        export AWS_RETRY_MODE=${retryMode} 
        export AWS_MAX_ATTEMPTS=${maxTransferAttempts}
        """.stripIndent().rightTrim()
    }

    /**
     * Implement S3 upload/download helper using `aws s3` CLI tool
     *
     * @return The Bash script implementing the S3 helper functions
     */
    protected String s3Lib() {
        """
        # fovus helper
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              cp /dev/null "\$s3path"
            elif [[ -d "\$name" ]]; then
              cp -r "\$name" "\$s3path/"
            else
              cp "\$name" "\$s3path/"
            fi
        }
        
        nxf_s3_download() {
            local source="\$1"
            local target="\$2"
        
            if [[ -d "\$source" ]]; then
                # source is a directory
                cp -r "\$source" "\$target"
            else
                # source is a file
                cp "\$source" "\$target"
            fi
        }
        """.stripIndent(true)
    }

    /**
     * Implement S3 upload/download helper using s3cmd CLI tool
     * https://github.com/peak/s5cmd
     *
     * @return The Bash script implementing the S3 helper functions
     */
    protected String s5cmdLib() {
        final cli = s5cmdPath
        """
        # aws helper for s5cmd
        nxf_s3_upload() {
            local name=\$1
            local s3path=\$2
            if [[ "\$name" == - ]]; then
              local tmp=\$(nxf_mktemp)
              cp /dev/stdin \$tmp/\$name
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass \$tmp/\$name "\$s3path"
            elif [[ -d "\$name" ]]; then
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name/" "\$s3path/\$name/"
            else
              $cli cp ${acl}${storageEncryption}${storageKmsKeyId}${requesterPays}--storage-class $storageClass "\$name" "\$s3path/\$name"
            fi
        }
        
        nxf_s3_download() {
            local source=\$1
            local target=\$2
            local file_name=\$(basename \$1)
            local is_dir=\$($cli ls \$source | grep -F "DIR  \${file_name}/" -c)
            if [[ \$is_dir == 1 ]]; then
                $cli cp "\$source/*" "\$target"
            else 
                $cli cp "\$source" "\$target"
            fi
        }
        """.stripIndent()
    }

    @Override
    String render() {
        return s5cmdPath
                ? super.render() + s5cmdLib()
                : super.render() + retryEnv() + s3Lib()
    }

    static private S3BashLib lib0(boolean includeCore) {
        new S3BashLib()
                .includeCoreFun(includeCore)
//                .withMaxParallelTransfers( opts.maxParallelTransfers )
//                .withDelayBetweenAttempts(opts.delayBetweenAttempts )
//                .withMaxTransferAttempts( opts.maxTransferAttempts )
//                .withCliPath( opts.awsCli )
//                .withStorageClass(opts.storageClass )
//                .withStorageEncryption( opts.storageEncryption )
//                .withStorageKmsKeyId( opts.storageKmsKeyId )
//                .withRetryMode( opts.retryMode )
//                .withDebug( opts.debug )
//                .withS5cmdPath( opts.s5cmdPath )
//                .withAcl( opts.s3Acl )
//                .withRequesterPays( opts.requesterPays )
    }


    static String script() {
        lib0(true).render()
    }
}
