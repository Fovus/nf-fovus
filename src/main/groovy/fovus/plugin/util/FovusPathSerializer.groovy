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

package fovus.plugin.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import fovus.plugin.nio.FovusPath
import nextflow.util.SerializerRegistrant
import org.pf4j.Extension

/**
 * Register the FovusPath serializer.
 *
 * Adapted from S3PathSerializer
 */
@Slf4j
@Extension
@CompileStatic
class FovusPathSerializer extends Serializer<FovusPath> implements SerializerRegistrant {

    @Override
    void register(Map<Class, Object> serializers) {
        serializers.put(FovusPath, FovusPathSerializer)
    }

    @Override
    void write(Kryo kryo, Output output, FovusPath target) {
        final scheme = target.getFileSystem().provider().getScheme()
        final path = target.toString()
        log.trace "FovusPath serialization > scheme: $scheme; path: $path"
        output.writeString(scheme)
        output.writeString(path)
    }

    @Override
    FovusPath read(Kryo kryo, Input input, Class<FovusPath> type) {
        final scheme = input.readString()
        final path = input.readString()
        if (scheme != 'fovus') throw new IllegalStateException("Unexpected scheme for Fovus path -- offending value '$scheme'")
        log.trace "FovusPath de-serialization > scheme: $scheme; path: $path"
        return (FovusPath) FovusPathFactory.create("fovus://${path}")
    }

}