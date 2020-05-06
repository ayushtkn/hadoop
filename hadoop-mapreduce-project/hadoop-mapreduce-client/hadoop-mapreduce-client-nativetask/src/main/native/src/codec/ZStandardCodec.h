/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ZSTANDARDCODEC_H_
#define ZSTANDARDCODEC_H_

#include "lib/Compressions.h"
#include "BlockCodec.h"
#include "zstd.h"

namespace NativeTask {

    class ZStandardCompressStream : public CompressStream {
    protected:
        ZSTD_outBuffer *_zstdOut;
//        char * _buffer; // OutputBuffer
        uint32_t _capacity; // OutputBuffer Capacity
        ZSTD_CStream *_ZSTDCStream;
        bool _finished;
    public:
        ZStandardCompressStream(OutputStream *stream, uint32_t bufferSizeHint);

        virtual ~ZStandardCompressStream();

        virtual void write(const void *buff, uint32_t length);

        virtual void flush();

        virtual void close();

        virtual void finish();

        virtual void resetState();

        virtual void writeDirect(const void *buff, uint32_t length);

        virtual uint64_t compressedBytesWritten() {
//            return _compressedBytesWritten;
        }
    };

    class ZStandardDecompressStream : public DecompressStream {
    protected:
        ZSTD_DStream *_zstdDStream;
        char *_buffer;
        uint32_t _capacity;
        ZSTD_inBuffer *_zstdIn;
        bool _eof;
    public:
        ZStandardDecompressStream(InputStream *stream, uint32_t bufferSizeHint);

        virtual ~ZStandardDecompressStream();

        virtual int32_t read(void *buff, uint32_t length);

        virtual void close();

        virtual int32_t readDirect(void *buff, uint32_t length);

        virtual uint64_t compressedBytesRead() {
//            return _compressedBytesRead;
        }
    };

} // namespace NativeTask

#endif /* ZSTANDARDCODEC_H_ */
