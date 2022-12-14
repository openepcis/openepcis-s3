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
package io.openepcis.s3.provider;

import io.openepcis.s3.AmazonS3Service;
import io.openepcis.s3.S3AsyncUpload;
import io.openepcis.s3.UploadMetadata;
import io.openepcis.s3.UploadResult;
import io.smallrye.mutiny.Uni;
import java.io.InputStream;
import java.util.Optional;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@ApplicationScoped
@RequiredArgsConstructor
public class AmazonS3ServiceImpl implements AmazonS3Service {

  private final S3Client client;

  private final S3Config config;

  private final S3AsyncUpload asyncUpload;

  @PostConstruct
  public void verifyBucket() {
    try {
      client.headBucket(HeadBucketRequest.builder().bucket(config.bucket()).build());
      return;
    } catch (NoSuchBucketException e) {
      client.createBucket(CreateBucketRequest.builder().bucket(config.bucket()).build());
    }
  }

  @Override
  public String put(String key, final InputStream in, long contentLength) {
    final PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(config.bucket()).key(key).build();
    client.putObject(putObjectRequest, RequestBody.fromInputStream(in, contentLength));
    return key;
  }

  @Override
  public String put(String key, InputStream in, UploadMetadata metadata) {
    client.putObject(
        metadata.request(config.bucket(), key).build(),
        RequestBody.fromInputStream(in, metadata.getContentLength().get()));
    return key;
  }

  @Override
  public Uni<UploadResult> putAsync(String key, InputStream in) {
    return putAsync(key, in, Optional.empty());
  }

  @Override
  public Uni<UploadResult> putAsync(String key, InputStream in, Optional<Long> contentLength) {
    return putAsync(
        key,
        in,
        UploadMetadata.builder()
            .contentLength(contentLength.isPresent() ? contentLength.get() : null)
            .build());
  }

  @Override
  public Uni<UploadResult> putAsync(String key, InputStream in, UploadMetadata metadata) {
    return Uni.createFrom()
        .completionStage(asyncUpload.upload(config.bucket(), key, in, Optional.of(metadata)));
  }

  @Override
  public InputStream get(String key) {
    final GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(config.bucket()).key(key).build();
    return client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
  }

  @Override
  public void delete(String key) {
    final DeleteObjectRequest deleteObjectRequest =
        DeleteObjectRequest.builder().bucket(config.bucket()).key(key).build();
    client.deleteObject(deleteObjectRequest);
  }

  @Override
  public boolean exists(String key) {
    try {
      client.headObject(HeadObjectRequest.builder().bucket(config.bucket()).key(key).build());
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    }
  }
}
