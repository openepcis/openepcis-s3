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

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

public interface AmazonS3Service {

  public static final String REGEX_VALID_S3_TAG_KEY = "^([\\p{L}\\p{Z}\\p{N}_.:/=+\\-@]*)$";
  public static final String REGEX_VALID_S3_TAG_VALUE = "^([\\p{L}\\p{Z}\\p{N}_.:/=+\\-]*)$";

  public static final Pattern MATCH_VALID_S3_TAG_KEY = Pattern.compile(REGEX_VALID_S3_TAG_KEY);
  public static final Pattern MATCH_VALID_S3_TAG_VALUE = Pattern.compile(REGEX_VALID_S3_TAG_VALUE);

  static Stream<Map.Entry<String, String>> cleanupTagSet(Map<String, String> tags) {
    return tags.entrySet().stream()
        .filter(
            entry -> {
              if (AmazonS3Service.MATCH_VALID_S3_TAG_KEY.matcher(entry.getKey()).matches()
                  && AmazonS3Service.MATCH_VALID_S3_TAG_VALUE.matcher(entry.getValue()).matches()) {
                return true;
              } else {
                Log.warn(
                    "invalid tag content: unable to use "
                        + entry.getKey()
                        + "="
                        + entry.getValue()
                        + " for S3 Tag");
                return false;
              }
            });
  }

  @Deprecated
  String put(final String key, final InputStream in, long contentLength);

  String put(final String key, final InputStream in, UploadMetadata metadata);

  @Deprecated
  Uni<UploadResult> putAsync(final String key, final InputStream in);

  @Deprecated
  Uni<UploadResult> putAsync(String key, InputStream in, Optional<Long> contentLength);

  Uni<UploadResult> putAsync(String key, InputStream in, UploadMetadata metadata);

  InputStream get(final String key);

  InputStream get(final String key, Optional<String> version);

  List<ObjectVersion> getAllVersions(String key);

  boolean hasVersionId(String objectKey);

  // String getLatestVersionId(String objectKey);

  void delete(final String key);

  boolean exists(String key);

  boolean addTags(String key, Map<String, String> tags);
}
