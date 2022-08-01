package io.openepcis.s3;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface S3AsyncUpload {
  CompletableFuture<UploadResult> upload(
      String bucketName, String key, InputStream in, Optional<UploadMetadata> objectMetadata);
}
