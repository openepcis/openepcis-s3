package io.openepcis.s3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public final class UploadResult {

  private String bucketName;
  private String eTag;
  private String key;
  private String versionId;
}
