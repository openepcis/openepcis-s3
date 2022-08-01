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
