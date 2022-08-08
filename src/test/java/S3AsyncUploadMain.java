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
import io.openepcis.s3.UploadResult;
import io.openepcis.s3.provider.S3AsyncUploadImpl;
import java.io.BufferedInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/** main class for manual S3AsyncUpload Tests */
public class S3AsyncUploadMain {

  public static void main(String args[]) throws Exception {
    final Path file = Paths.get(args[0]);
    var credentialsProvider =
        new AwsCredentialsProvider() {
          @Override
          public AwsCredentials resolveCredentials() {
            return new AwsCredentials() {
              @Override
              public String accessKeyId() {
                return "root";
              }

              @Override
              public String secretAccessKey() {
                return "openepcis";
              }
            };
          }
        };
    final var clientBuilder = S3Client.builder();
    clientBuilder.endpointOverride(new URI("http://localhost:9100"));
    clientBuilder.region(Region.US_EAST_1);
    clientBuilder.credentialsProvider(credentialsProvider);
    clientBuilder.httpClient(ApacheHttpClient.create());
    var asyncClientBuilder = S3AsyncClient.builder();
    asyncClientBuilder.endpointOverride(new URI("http://localhost:9100"));
    asyncClientBuilder.region(Region.US_EAST_1);
    asyncClientBuilder.credentialsProvider(credentialsProvider);
    asyncClientBuilder.httpClient(NettyNioAsyncHttpClient.create());
    final S3AsyncUploadImpl s3AsyncUpload =
        new S3AsyncUploadImpl(clientBuilder.build(), asyncClientBuilder.build());
    final String fileName = args[0].substring(args[0].lastIndexOf("/") + 1);
    final CompletableFuture<UploadResult> future =
        s3AsyncUpload.upload(
            "test",
            fileName,
            new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 1024),
            Optional.empty());
    System.out.println(future.join().getETag());
    System.exit(0);
  }
}
