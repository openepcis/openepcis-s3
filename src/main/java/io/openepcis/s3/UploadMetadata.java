/*
 * Copyright 2022 benelog GmbH & Co. KG
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

import java.util.Optional;
import lombok.Getter;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Getter
public class UploadMetadata {

  private Optional<String> contentType;

  private Optional<Long> contentLength;

  private UploadMetadata(UploadMetadataBuilder builder) {
    this.contentType = Optional.ofNullable(builder.contentType);
    this.contentLength = Optional.ofNullable(builder.contentLength);
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
    if (contentType.isPresent()) {
      requestBuilder.contentType(contentType.get());
    }
    if (contentLength.isPresent()) {
      requestBuilder.contentLength(contentLength.get());
    }
    return requestBuilder;
  }

  public static class UploadMetadataBuilder {
    private String contentType;

    private Long contentLength;

    private UploadMetadataBuilder() {
      // private empty constructor
    }

    private UploadMetadataBuilder(UploadMetadata metadata) {
      contentType = metadata.getContentType().isPresent() ? metadata.getContentType().get() : null;
      contentLength =
          metadata.getContentLength().isPresent() ? metadata.getContentLength().get() : null;
    }

    public UploadMetadataBuilder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public UploadMetadataBuilder contentLength(Long contentLength) {
      this.contentLength = contentLength;
      return this;
    }

    public UploadMetadata build() {
      return new UploadMetadata(this);
    }
  }
}
