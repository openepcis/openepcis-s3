package io.openepcis.s3;

import io.smallrye.mutiny.Uni;
import java.io.InputStream;
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

  void delete(final String key);

  boolean exists(String key);
}
