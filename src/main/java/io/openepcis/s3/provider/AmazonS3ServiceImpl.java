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
package io.openepcis.s3.provider;

import io.openepcis.s3.AmazonS3Service;
import io.openepcis.s3.S3AsyncUpload;
import io.openepcis.s3.UploadMetadata;
import io.openepcis.s3.UploadResult;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.InputStream;
import java.util.*;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

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
        key, in, UploadMetadata.builder().contentLength(contentLength.orElse(null)).build());
  }

  @Override
  public Uni<UploadResult> putAsync(String key, InputStream in, UploadMetadata metadata) {
    return Uni.createFrom()
        .completionStage(asyncUpload.upload(config.bucket(), key, in, Optional.of(metadata)));
  }

  @Override
  public InputStream get(String key) {
    return get(key, Optional.empty());
  }

  public InputStream get(final String key, Optional<String> versionId) {
    final GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder().bucket(config.bucket()).key(key);
    if (versionId.isPresent()) {
      getObjectRequestBuilder.versionId(versionId.get());
    }
    return client.getObject(getObjectRequestBuilder.build(), ResponseTransformer.toInputStream());
  }

//check for object version list
  @Override
  public List<ObjectVersion> getAllVersions(String key) {
    final ListObjectVersionsRequest listObjectVersionsRequest =
            ListObjectVersionsRequest.builder().bucket(config.bucket()).prefix(key).build();
    final ListObjectVersionsResponse listObjectVersionsResponse =
            client.listObjectVersions(listObjectVersionsRequest);
    return listObjectVersionsResponse.versions();
  }

  /*public String getLatestVersionId(String objectKey) {
    ListObjectVersionsRequest listVersionsRequest = ListObjectVersionsRequest.builder()
            .bucket(config.bucket())
            .prefix(objectKey)
            .build();
    ListObjectVersionsResponse listVersionsResponse = client.listObjectVersions(listVersionsRequest);
   List<ObjectVersion> objectVersions = listVersionsResponse.versions();
    objectVersions.sort(Comparator.comparing(ObjectVersion::lastModified).reversed());
    return objectVersions.get(0).versionId();
  }*/
@Override
  public boolean hasVersionId( String objectKey) {

  HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
          .bucket(config.bucket())
          .key(objectKey)
          .build();
  HeadObjectResponse headObjectResponse = client.headObject(headObjectRequest);
  return headObjectResponse.versionId() != null;

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

  public boolean addTags(String key, Map<String, String> tags) {
    try {
      List<Tag> tagSet =
          new ArrayList<>(
              client
                  .getObjectTagging(
                      GetObjectTaggingRequest.builder().bucket(config.bucket()).key(key).build())
                  .tagSet());
      tags.forEach((k, v) -> tagSet.add(Tag.builder().key(k).value(v).build()));
      PutObjectTaggingResponse response =
          client.putObjectTagging(
              PutObjectTaggingRequest.builder()
                  .bucket(config.bucket())
                  .key(key)
                  .tagging(Tagging.builder().tagSet(tagSet).build())
                  .build());
      return response.sdkHttpResponse().isSuccessful();
    } catch (Exception e) {
      return false;
    }
  }
}
