/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.catalog.files.s3;

import static java.lang.String.format;

import java.io.InputStream;
import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.S3Request;

public class S3Clients {

  /**
   * Builds the base S3 client with the shared HTTP client, configured with the minimum amount of
   * options, an empty "profile file".
   */
  public static S3Client createS3BaseClient(S3Config s3Config) {
    ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder();
    s3Config.maxHttpConnections().ifPresent(httpClient::maxConnections);
    s3Config.readTimeout().ifPresent(httpClient::socketTimeout);
    s3Config.connectTimeout().ifPresent(httpClient::connectionTimeout);
    s3Config.connectionAcquisitionTimeout().ifPresent(httpClient::connectionAcquisitionTimeout);
    s3Config.connectionMaxIdleTime().ifPresent(httpClient::connectionMaxIdleTime);
    s3Config.connectionTimeToLive().ifPresent(httpClient::connectionTimeToLive);
    s3Config.expectContinueEnabled().ifPresent(httpClient::expectContinueEnabled);

    // Supply an empty profile file
    ProfileFile profileFile =
        ProfileFile.builder()
            .content(InputStream.nullInputStream())
            .type(ProfileFile.Type.CONFIGURATION)
            .build();

    return S3Client.builder()
        .httpClientBuilder(httpClient)
        .credentialsProvider(new NoCredentialsProvider())
        .region(Region.EU_CENTRAL_1)
        .overrideConfiguration(override -> override.defaultProfileFileSupplier(() -> profileFile))
        .serviceConfiguration(serviceConfig -> serviceConfig.profileFile(() -> profileFile))
        .build();
  }

  /**
   * Produces an S3 client for the given set of S3 options and secrets. S3 options are retrieved
   * from the per-bucket config, which derives from the global config. References to the secrets
   * that contain the actual S3 access-key-ID and secret-access-key are present in the S3 options as
   * well.
   */
  public static S3Client configuredClient(
      S3Client s3client, S3Options<?> s3options, SecretsProvider secretsProvider) {
    return new DelegatingS3Client(s3client) {

      @Override
      protected <T extends S3Request, ReturnT> ReturnT invokeOperation(
          T request, Function<T, ReturnT> operation) {

        Optional<String> bucketName = request.getValueForField("Bucket", String.class);

        S3BucketOptions bucketOptions = s3options.effectiveOptionsForBucket(bucketName);

        AwsRequestOverrideConfiguration.Builder override =
            request
                .overrideConfiguration()
                .map(AwsRequestOverrideConfiguration::toBuilder)
                .orElseGet(AwsRequestOverrideConfiguration::builder)
                //
                .addPlugin(
                    config -> {
                      S3ServiceClientConfiguration.Builder s3configBuilder =
                          (S3ServiceClientConfiguration.Builder) config;

                      s3configBuilder.endpointOverride(resolveS3Endpoint(bucketOptions));

                      bucketOptions
                          .region()
                          .ifPresent(region -> s3configBuilder.region(Region.of(region)));
                    })
                .credentialsProvider(awsCredentialsProvider(bucketOptions, secretsProvider));

        // https://cloud.google.com/storage/docs/aws-simple-migration#project-header
        bucketOptions.projectId().ifPresent(prj -> override.putHeader("x-amz-project-id", prj));

        @SuppressWarnings("unchecked")
        T overridden = (T) request.toBuilder().overrideConfiguration(override.build()).build();

        return super.invokeOperation(overridden, operation);
      }
    };
  }

  public static AwsCredentialsProvider awsCredentialsProvider(
      S3BucketOptions bucketOptions, SecretsProvider secretsProvider) {
    return () -> {
      String accessKeyId =
          secretsProvider.getSecret(
              bucketOptions
                  .accessKeyIdRef()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Secret reference to S3 access key ID is not defined")));
      String secretAccessKey =
          secretsProvider.getSecret(
              bucketOptions
                  .secretAccessKeyRef()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Secret reference to S3 secret access key is not defined")));
      return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    };
  }

  private static class NoCredentialsProvider implements AwsCredentialsProvider {
    @Override
    public AwsCredentials resolveCredentials() {
      throw new IllegalStateException(
          "Invalid access path to S3Client - must wrap with credentials-providing delegate");
    }
  }

  private static URI resolveS3Endpoint(S3BucketOptions options) {
    Cloud cloud =
        options
            .cloud()
            .orElseThrow(() -> new IllegalStateException("Must configure the cloud type"));
    switch (cloud) {
      case AMAZON:
        return options
            .endpoint()
            .orElseGet(
                () ->
                    URI.create(
                        format(
                            "https://s3.%s.amazonaws.com/",
                            options
                                .region()
                                .orElseThrow(
                                    () ->
                                        new IllegalArgumentException(
                                            "Must configure the AWS region")))));
      case GOOGLE:
        return options.endpoint().orElseGet(() -> URI.create("https://storage.googleapis.com/"));
      case MICROSOFT:
        throw new IllegalArgumentException("Cloud " + cloud + " not supported for S3");
      case PRIVATE:
        return options
            .endpoint()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No endpoint provided, but cloud "
                            + cloud
                            + " requires an explicit endpoint."));
      default:
        throw new IllegalArgumentException("Unknown cloud " + cloud);
    }
  }
}
