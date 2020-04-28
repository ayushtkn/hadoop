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

#include "lib/commons.h"
#include "NativeTask.h"
#include "zstd.h"
#include "ZStandardCodec.h"

namespace NativeTask {

uint64_t ZStandardCompressStream::maxCompressedLength(uint64_t origLength) {
   return ZSTD_compressBound(origLength);
}

ZStandardCompressStream::ZStandardCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : BlockCompressStream(stream, bufferSizeHint) {
  init();
}

void ZStandardCompressStream::compressOneBlock(const void * buff, uint32_t length) {
  size_t compressedLength = _tempBufferSize - 8;
  size_t ret =   ZSTD_compress(_tempBuffer + 8,compressedLength, (const char*)buff, length, ZSTD_CLEVEL_DEFAULT);
  if (ret > 0) {
    compressedLength = ret;
    ((uint32_t*)_tempBuffer)[0] = bswap(length);
    ((uint32_t*)_tempBuffer)[1] = bswap((uint32_t)compressedLength);
    _stream->write(_tempBuffer, compressedLength + 8);
    _compressedBytesWritten += (compressedLength + 8);
  } else {
    THROW_EXCEPTION(IOException, "compress ZStandard failed");
  }
}

//////////////////////////////////////////////////////////////

ZStandardDecompressStream::ZStandardDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : BlockDecompressStream(stream, bufferSizeHint) {
  init();
}

uint32_t ZStandardDecompressStream::decompressOneBlock(uint32_t compressedSize, void * buff,
    uint32_t length) {
  if (compressedSize > _tempBufferSize) {
    char * newBuffer = (char *)realloc(_tempBuffer, compressedSize);
    if (newBuffer == NULL) {
      THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
    }
    _tempBuffer = newBuffer;
    _tempBufferSize = compressedSize;
  }
  uint32_t rd = _stream->readFully(_tempBuffer, compressedSize);
  if (rd != compressedSize) {
    THROW_EXCEPTION(IOException, "readFully reach EOF");
  }
  _compressedBytesRead += rd;
  uint32_t ret =  ZSTD_decompress((void *)buff,length, (void *)_tempBuffer, compressedSize);
  if (!ZSTD_isError(ret)) {
    return length;
  } else {
    THROW_EXCEPTION(IOException, "decompress ZStandard failed");
  }
}

uint64_t ZStandardDecompressStream::maxCompressedLength(uint64_t origLength) {
return ZSTD_compressBound(origLength);
}

} // namespace NativeTask