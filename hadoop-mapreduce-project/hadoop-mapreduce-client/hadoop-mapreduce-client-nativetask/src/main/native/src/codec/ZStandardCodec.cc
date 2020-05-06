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

#include <zconf.h>
#include "lib/commons.h"
#include "NativeTask.h"
#include "zstd.h"
#include "ZStandardCodec.h"


namespace NativeTask {

    ZStandardCompressStream::ZStandardCompressStream(OutputStream *stream, uint32_t bufferSizeHint)
        : CompressStream(stream), _ZSTDCStream(NULL), _finished(false) {
        _capacity = bufferSizeHint;
        _zstdOut = new ZSTD_outBuffer;
        _zstdOut->size = _capacity;
        _zstdOut->pos = 0;
        _zstdOut->dst = new char[_capacity];
        _ZSTDCStream = ZSTD_createCStream();

        if (ZSTD_isError(ZSTD_initCStream(_ZSTDCStream, 3))) {
            ZSTD_freeCStream(_ZSTDCStream);
            _ZSTDCStream = NULL;
            THROW_EXCEPTION(IOException, "init failed");
        }
    }

    ZStandardCompressStream::~ZStandardCompressStream() {
        if (_ZSTDCStream != NULL) {
            ZSTD_freeCStream(_ZSTDCStream);
            _ZSTDCStream = NULL;
        }
    }

    void ZStandardCompressStream::write(const void *buff, uint32_t length) {
        ZSTD_inBuffer input = {buff, length, 0};
        while (true) {
            ZSTD_outBuffer output = {_zstdOut->dst, _zstdOut->size, 0};
            int remaining = ZSTD_compressStream(_ZSTDCStream, &output, &input);
            if (!ZSTD_isError(remaining)) {
                if (output.pos > 0) {
                    _stream->write(output.dst, output.pos);
                    output.pos = 0;
                }
                if (input.pos == length) { // TODO
                    break;
                }
            } else {
                THROW_EXCEPTION(IOException, "compress stream return error");
            }
        }
        _finished = false;
    }

    void ZStandardCompressStream::flush() {
        ZSTD_outBuffer output = {_zstdOut->dst, _zstdOut->size, 0};
        while (true) {
            int ret = ZSTD_flushStream(_ZSTDCStream, &output);
            if (!ZSTD_isError(ret)) {
                if (output.pos > 0) {
                    _stream->write(output.dst, output.pos);
                    output.pos = 0;
                }
            } else {
                THROW_EXCEPTION(IOException, "Nahi Chal Rha!!!");
            }

            if (ret == 0) {
                break;
            }
        }
        _stream->flush();
        _finished = false;
    }

    void ZStandardCompressStream::resetState() {
        ZSTD_freeCStream(_ZSTDCStream);
        _ZSTDCStream = ZSTD_createCStream();
        ZSTD_initCStream(_ZSTDCStream, 3);
    }

    void ZStandardCompressStream::finish() {
        if (_finished) {
            return;
        }
        flush();
        ZSTD_outBuffer output = {_zstdOut->dst, _zstdOut->size, 0};
        while (true) {
            size_t ret = ZSTD_endStream(_ZSTDCStream, &output);
            if (ret == 0 || !ZSTD_isError(ret)) {
                _stream->write(output.dst, output.pos);
                if (output.pos == output.size) {
                    //Some more data to be flushed.
                    output.pos = 0;
                    continue;
                }
            }
            if (ret == 0) {
                break;
            }
        }
        _finished = true;
    }

    void ZStandardCompressStream::close() {
        if (!_finished) {
            finish();
        }
    }

    void ZStandardCompressStream::writeDirect(const void *buff, uint32_t length) {
        if (!_finished) {
            finish();
        }
        _stream->write(buff, length);
    }

//////////////////////////////////////////////////////////////

    ZStandardDecompressStream::ZStandardDecompressStream(InputStream *stream, uint32_t bufferSizeHint)
        : DecompressStream(stream), _zstdDStream(NULL), _capacity(bufferSizeHint), _zstdIn(new ZSTD_inBuffer()) {
        _buffer = new char[bufferSizeHint];
        _zstdIn->src = _buffer;
        _capacity = bufferSizeHint;
        _zstdIn->pos = 0;
        _zstdIn->size = _capacity;

//        memset(_zstdIn, 0, sizeof(_zstdIn));
        _zstdDStream = ZSTD_createDStream();

        if (ZSTD_isError(ZSTD_initDStream(_zstdDStream))) {
            ZSTD_freeDStream(_zstdDStream);
            _zstdDStream = NULL;
            THROW_EXCEPTION(IOException, "Decompress init failed");
        }
        _eof = false;
    }

    ZStandardDecompressStream::~ZStandardDecompressStream() {
        if (_zstdDStream != NULL) {
//            inflateEnd((z_stream*)_zstream);

            ZSTD_freeDStream(_zstdDStream);
            _zstdDStream = NULL;
        }
        delete _zstdIn;
        _zstdIn = NULL;
    }

    int32_t ZStandardDecompressStream::read(void *buff, uint32_t length) {
        ZSTD_outBuffer output = {buff, length, 0};
        while (output.pos < output.size) {
            if (_zstdIn->pos == _zstdIn->size || _zstdIn->pos == 0) {
                _zstdIn->pos = 0;
                int32_t rd = _stream->read(_buffer, _capacity);
                if (rd <= 0) {
                    _eof = true;
                    return -1;
                } else {
                    _zstdIn->pos = rd;
                }
            }

            //Pass-on read bytes from underlying stream for decompression
            ZSTD_inBuffer input = {_zstdIn->src, _zstdIn->pos, 0};
            while (input.pos < input.size) {
                int ret = ZSTD_decompressStream(_zstdDStream, &output, &input);
                if (!ZSTD_isError(ret)) {
                    if (input.pos == input.size) {
                        _zstdIn->pos = 0;
                        return output.pos;
                    }
                } else {
                    THROW_EXCEPTION(IOException, "Decompress init failed " + ZSTD_getErrorName(ret));
                }
            }
        }
        return -1;
    }

    void ZStandardDecompressStream::close() {
    }

    int32_t ZStandardDecompressStream::readDirect(void *buff, uint32_t length) {
        int32_t ret = _stream->readFully(buff, length);
        if (ret > 0) {
//            _compressedBytesRead += ret;
        }
        return ret;
    }

} // namespace NativeTask