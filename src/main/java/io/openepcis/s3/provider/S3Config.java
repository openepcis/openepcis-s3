package io.openepcis.s3.provider;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;

@StaticInitSafe
@ConfigMapping(prefix = "s3")
public interface S3Config {
  String bucket();

  String jsonSchemaKeyPrefix();

  String xsdSchemaKeyPrefix();

  String documentKeyPrefixDateFormat();
}
