/*
 * Copyright 2022-2023 benelog GmbH & Co. KG
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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AmazonS3Service {

  @Deprecated
  String put(final String key, final InputStream in, long contentLength);

  String put(final String key, final InputStream in, UploadMetadata metadata);

  @Deprecated
  Uni<UploadResult> putAsync(final String key, final InputStream in);

  @Deprecated
  Uni<UploadResult> putAsync(String key, InputStream in, Optional<Long> contentLength);

  Uni<UploadResult> putAsync(String key, InputStream in, UploadMetadata metadata);

  InputStream get(final String key);

  List<ObjectVersion> getAllVersions(String key);

  void delete(final String key);

  boolean exists(String key);

  boolean addTags(String key, Map<String, String> tags);
}
