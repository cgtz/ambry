/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.messageformat;

import com.github.ambry.router.ByteRange;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * A class to serialize and deserialize MetadataContent which forms the content of a Metadata Blob.
 */
public class MetadataContentSerDe {
  /**
   * Serialize the input list of keys that form the metadata content.
   * @param compositeBlobInfo the {@link CompositeBlobInfo} to serialize.
   * @return a ByteBuffer containing the serialized output.
   */
  public static ByteBuffer serializeMetadataContent(CompositeBlobInfo compositeBlobInfo) {
    int keySize = compositeBlobInfo.getChunks().get(0).getStoreKey().sizeInBytes();
    int numKeys = compositeBlobInfo.getChunks().size();
    ByteBuffer outputBuf;
    if (compositeBlobInfo.containsNonUniformChunks()) {
      outputBuf = ByteBuffer.allocate(
          MessageFormatRecord.Metadata_Content_Format_V3.getMetadataContentSize(keySize, numKeys));
      MessageFormatRecord.Metadata_Content_Format_V3.serializeMetadataContentRecord(outputBuf, compositeBlobInfo);
    } else {
      outputBuf = ByteBuffer.allocate(
          MessageFormatRecord.Metadata_Content_Format_V2.getMetadataContentSize(keySize, numKeys));
      MessageFormatRecord.Metadata_Content_Format_V2.serializeMetadataContentRecord(outputBuf, compositeBlobInfo);
    }
    return outputBuf;
  }

  /**
   * Deserialize the serialized metadata content in the input ByteBuffer using the given {@link StoreKeyFactory} as a
   * reference.
   * @param buf ByteBuffer containing the serialized metadata content.
   * @param storeKeyFactory the {@link StoreKeyFactory} to use to deserialize the content.
   * @param range an optional {@link ByteRange} to filter the chunks returned by. If this argument is supplied, the
   *              returned {@link CompositeBlobInfo} will only include chunks that include bytes in this range.
   * @return a list of {@link StoreKey} containing the deserialized output.
   * @throws IOException if an IOException is encountered during deserialization.
   * @throws MessageFormatException if an unknown version is encountered in the header of the serialized input.
   */
  public static CompositeBlobInfo deserializeMetadataContentRecord(ByteBuffer buf, StoreKeyFactory storeKeyFactory,
      ByteRange range) throws IOException, MessageFormatException {
    int version = buf.getShort();
    switch (version) {
      case MessageFormatRecord.Metadata_Content_Version_V2:
        return MessageFormatRecord.Metadata_Content_Format_V2.deserializeMetadataContentRecord(
            new DataInputStream(new ByteBufferInputStream(buf)), storeKeyFactory, range);
      case MessageFormatRecord.Metadata_Content_Version_V3:
        return MessageFormatRecord.Metadata_Content_Format_V3.deserializeMetadataContentRecord(
            new DataInputStream(new ByteBufferInputStream(buf)), storeKeyFactory, range);
      default:
        throw new MessageFormatException("Unknown version encountered for MetadataContent: " + version,
            MessageFormatErrorCodes.Unknown_Format_Version);
    }
  }
}
