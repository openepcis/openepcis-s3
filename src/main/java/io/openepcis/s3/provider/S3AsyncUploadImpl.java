package io.openepcis.s3.provider;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.openepcis.s3.S3AsyncUpload;
import io.openepcis.s3.UploadMetadata;
import io.openepcis.s3.UploadResult;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Wrapper managing async streamed upload for streams with unknown content-length This Wrapper will
 * handle the data to be uploaded in 5 MegaByte chunks, silly but required for effective streaming
 * backpressure it will do the following: 1. Check if content-length is known: if yes use standard
 * S3 put 2. Check if content is below 5 MegaBytes: if yes, use content-length detected for standard
 * S3 put 3. Initiate Multipart Upload for content with more than 5 MegaBytes which will allow us to
 * stream up to 50 GigaBytes
 *
 * <p>see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
 */
@ApplicationScoped
@RequiredArgsConstructor
public class S3AsyncUploadImpl implements S3AsyncUpload {

  // it' silly but we need to have at least 5 MB for S3 to allow multipart upload
  private static final int FIVE_MEGABYTES = 5242880;
  private final S3Client client;
  private final S3AsyncClient asyncClient;
  private ByteBufAllocator byteBufAllocator = PooledByteBufAllocator.DEFAULT;

  private ExecutorService executorService = Executors.newCachedThreadPool();

  @Override
  public CompletableFuture<UploadResult> upload(
      final String bucketName,
      final String key,
      final InputStream in,
      Optional<UploadMetadata> uploadMetadata) {
    return CompletableFuture.supplyAsync(
        () -> {
          final UploadMetadata metadata = uploadMetadata.orElse(UploadMetadata.builder().build());

          // if content-length is defined use direct upload with TransferManager
          if (metadata.getContentLength().isPresent()) {
            return getUploadResult(bucketName, key, in, metadata);
          }

          // optimize S3 Upload Behaviour using 5 MegaByte ByteBuf
          final ByteBuf buffer = byteBufAllocator.directBuffer(FIVE_MEGABYTES);
          try {
            int bytesWritten = buffer.writeBytes(in, FIVE_MEGABYTES);
            int totalBytesWritten = bytesWritten;
            while (bytesWritten != -1 && totalBytesWritten < FIVE_MEGABYTES) {
              bytesWritten = buffer.writeBytes(in, FIVE_MEGABYTES);
              totalBytesWritten += bytesWritten == -1 ? 0 : bytesWritten;
            }

            // InputStream has less than 5 MegaBytes
            if (totalBytesWritten < FIVE_MEGABYTES) {
              return getUploadResult(
                  bucketName,
                  key,
                  buffer.nioBuffer(),
                  UploadMetadata.builder(metadata).contentLength((long) totalBytesWritten).build());
            }

            // Upload files larger than 5 MegaByte using multipart feature
            return getAsyncMultipartUploadResult(
                bucketName, key, in, metadata, buffer, totalBytesWritten);
          } catch (CompletionException e) {
            throw e;
          } catch (Exception e) {
            throw new CompletionException(e);
          } finally {
            buffer.release();
          }
        },
        executorService);
  }

  private UploadResult getAsyncMultipartUploadResult(
      String bucketName,
      String key,
      InputStream in,
      UploadMetadata metadata,
      ByteBuf buffer,
      int bytesWritten)
      throws IOException {
    final CreateMultipartUploadRequest.Builder requestBuilder =
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key);
    if (metadata.getContentType().isPresent()) {
      requestBuilder.contentType(metadata.getContentType().get());
    }
    final CreateMultipartUploadResponse createMultipartUploadResponse =
        client.createMultipartUpload(requestBuilder.build());
    final List<CompletedPart> completedParts = new ArrayList<>();
    if (createMultipartUploadResponse.sdkHttpResponse().isSuccessful()) {
      int partNumber = 1;
      final ByteBuf outputBuffer = byteBufAllocator.directBuffer(FIVE_MEGABYTES);
      try {
        while (bytesWritten != -1) {
          final UploadPartRequest uploadPartRequest =
              UploadPartRequest.builder() //
                  .partNumber(partNumber) //
                  .uploadId(createMultipartUploadResponse.uploadId()) //
                  .bucket(bucketName) //
                  .key(key) //
                  .contentLength((long) buffer.readableBytes()) //
                  .build();
          outputBuffer.resetReaderIndex();
          outputBuffer.resetWriterIndex();
          outputBuffer.writeBytes(buffer);
          final CompletableFuture<UploadPartResponse> uploadPartFuture =
              asyncClient.uploadPart(
                  uploadPartRequest, AsyncRequestBody.fromByteBuffer(outputBuffer.nioBuffer()));
          buffer.resetReaderIndex();
          buffer.resetWriterIndex();
          bytesWritten = buffer.writeBytes(in, FIVE_MEGABYTES);
          final UploadPartResponse uploadPartResponse = uploadPartFuture.join();
          if (uploadPartResponse.sdkHttpResponse().isSuccessful()) {
            completedParts.add(
                CompletedPart.builder()
                    .partNumber(partNumber++)
                    .eTag(uploadPartResponse.eTag())
                    .build());
          } else {
            asyncClient.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .uploadId(createMultipartUploadResponse.uploadId())
                    .build());
            throw new IOException(
                uploadPartResponse
                    .sdkHttpResponse()
                    .statusText()
                    .orElse("HttpStatus: " + uploadPartResponse.sdkHttpResponse().statusCode()));
          }
        }
        final CompleteMultipartUploadResponse completeMultipartUploadResponse =
            client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder() //
                    .multipartUpload(
                        CompletedMultipartUpload.builder().parts(completedParts).build()) //
                    .uploadId(createMultipartUploadResponse.uploadId()) //
                    .bucket(bucketName) //
                    .key(key) //
                    .build());
        if (completeMultipartUploadResponse.sdkHttpResponse().isSuccessful()) {
          return UploadResult.builder() //
              .versionId(completeMultipartUploadResponse.versionId()) //
              .eTag(completeMultipartUploadResponse.eTag()) //
              .bucketName(completeMultipartUploadResponse.bucket()) //
              .key(completeMultipartUploadResponse.key()) //
              .build();
        } else {
          throw new IOException(
              completeMultipartUploadResponse
                  .sdkHttpResponse()
                  .statusText()
                  .orElse(
                      "HttpStatus: "
                          + completeMultipartUploadResponse.sdkHttpResponse().statusCode()));
        }
      } finally {
        outputBuffer.release();
      }
    } else {
      throw new IOException(
          createMultipartUploadResponse
              .sdkHttpResponse()
              .statusText()
              .orElse(
                  "HttpStatus: " + createMultipartUploadResponse.sdkHttpResponse().statusCode()));
    }
  }

  private UploadResult getUploadResult(
      String bucketName, String key, ByteBuffer buffer, UploadMetadata metadata)
      throws CompletionException {
    final PutObjectRequest.Builder requestBuilder = metadata.request(bucketName, key);
    final PutObjectResponse res =
        client.putObject(requestBuilder.build(), RequestBody.fromByteBuffer(buffer));
    return createUploadResult(bucketName, key, res);
  }

  private UploadResult getUploadResult(
      String bucketName, String key, InputStream in, UploadMetadata metadata)
      throws CompletionException {
    if (metadata.getContentLength().isEmpty()) {
      throw new CompletionException(new IOException("content-length missing in metadata"));
    }
    final PutObjectRequest.Builder requestBuilder = metadata.request(bucketName, key);
    final PutObjectResponse res =
        client.putObject(
            requestBuilder.build(),
            RequestBody.fromInputStream(in, metadata.getContentLength().get()));
    return createUploadResult(bucketName, key, res);
  }

  private UploadResult createUploadResult(
      final String bucketName, final String key, PutObjectResponse res) throws CompletionException {
    if (res.sdkHttpResponse().isSuccessful()) {
      return UploadResult.builder() //
          .bucketName(bucketName) //
          .key(key) //
          .eTag(res.eTag()) //
          .versionId(res.versionId()) //
          .build();
    }
    throw new CompletionException(
        new IOException(
            res.sdkHttpResponse()
                .statusText()
                .orElse("HttpStatus: " + res.sdkHttpResponse().statusCode())));
  }
}
