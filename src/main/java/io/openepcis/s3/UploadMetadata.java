/*
 * Copyright 2022-2024 benelog GmbH & Co. KG
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package io.openepcis.s3;

import java.util.*;
import lombok.Getter;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.utils.CollectionUtils;

@Getter
public class UploadMetadata {

  private Optional<String> contentType;

  private Optional<Long> contentLength;

  private Optional<Map<String, String>> tags;

  private UploadMetadata(UploadMetadataBuilder builder) {
    this.contentType = Optional.ofNullable(builder.contentType);
    this.contentLength = Optional.ofNullable(builder.contentLength);
    this.tags = Optional.ofNullable(builder.tags);
  }

  public static final UploadMetadataBuilder builder() {
    return new UploadMetadataBuilder();
  }

  public static final UploadMetadataBuilder builder(UploadMetadata metadata) {
    return new UploadMetadataBuilder(metadata);
  }

  public PutObjectRequest.Builder request(String bucket, String key) {
    final PutObjectRequest.Builder requestBuilder =
        PutObjectRequest.builder().bucket(bucket).key(key);
    contentType.ifPresent(requestBuilder::contentType);
    contentLength.ifPresent(requestBuilder::contentLength);
    if (tags.isPresent() && CollectionUtils.isNotEmpty(tags.get())) {
      Set<Tag> tagSet = new HashSet<>();
      tags.get().forEach((k, v) -> tagSet.add(Tag.builder().key(k).value(v).build()));
      requestBuilder.tagging(Tagging.builder().tagSet(tagSet).build());
    }
    return requestBuilder;
  }

  public static class UploadMetadataBuilder {
    private String contentType;

    private Long contentLength;

    private Map<String, String> tags;

    private UploadMetadataBuilder() {
      // private empty constructor
    }

    private UploadMetadataBuilder(UploadMetadata metadata) {
      contentType = metadata.getContentType().isPresent() ? metadata.getContentType().get() : null;
      contentLength =
          metadata.getContentLength().isPresent() ? metadata.getContentLength().get() : null;
      tags =
          metadata.tags.isPresent() && CollectionUtils.isNotEmpty(metadata.tags.get())
              ? metadata.tags.get()
              : null;
    }

    public UploadMetadataBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public UploadMetadataBuilder contentLength(Long contentLength) {
      this.contentLength = contentLength;
      return this;
    }

    public UploadMetadataBuilder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public UploadMetadata build() {
      return new UploadMetadata(this);
    }
  }
}
