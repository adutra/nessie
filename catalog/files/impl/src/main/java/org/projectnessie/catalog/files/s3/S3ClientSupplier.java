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

import static org.projectnessie.catalog.files.s3.S3Clients.awsCredentialsProvider;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.S3Request;

public class S3ClientSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ClientSupplier.class);

  private final S3Client baseS3Client;
  private final S3Config s3config;
  private final S3Options<?> s3options;
  private final SecretsProvider secretsProvider;

  public S3ClientSupplier(
      S3Client baseS3Client,
      S3Config s3config,
      S3Options<?> s3options,
      SecretsProvider secretsProvider) {
    this.baseS3Client = baseS3Client;
    this.s3config = s3config;
    this.s3options = s3options;
    this.secretsProvider = secretsProvider;
  }

  public S3Config s3config() {
    return s3config;
  }

  public S3Options<?> s3options() {
    return s3options;
  }

  public S3Uri parseUri(URI uri) {
    return baseS3Client.utilities().parseUri(uri);
  }

  /**
   * Produces an S3 client for the set of S3 options and secrets. S3 options are retrieved from the
   * per-bucket config, which derives from the global config. References to the secrets that contain
   * the actual S3 access-key-ID and secret-access-key are present in the S3 options as well.
   */
  public S3Client getClient() {
    return new DelegatingS3Client(baseS3Client) {

      @Override
      protected <T extends S3Request, ReturnT> ReturnT invokeOperation(
          T request, Function<T, ReturnT> operation) {

        Optional<String> bucketName = request.getValueForField("Bucket", String.class);

        S3BucketOptions bucketOptions = s3options.effectiveOptionsForBucket(bucketName);

        URI endpoint = bucketOptions.resolveS3Endpoint();

        // TODO reduce log level to TRACE
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Building S3-client for bucket {} using endpoint {} with {}",
              bucketName,
              endpoint,
              toLogString(bucketOptions));
        }

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

                      s3configBuilder.endpointOverride(endpoint);

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

  private static String toLogString(S3BucketOptions options) {
    return "S3BucketOptions{"
        + "cloud="
        + options.cloud().map(Cloud::name).orElse("<undefined>")
        + ", endpoint="
        + options.endpoint().map(URI::toString).orElse("<undefined>")
        + ", region="
        + options.region().orElse("<undefined>")
        + ", projectId="
        + options.projectId().orElse("<undefined>")
        + ", accessKeyIdRef="
        + options.accessKeyIdRef().orElse("<undefined>")
        + ", secretAccessKeyRef="
        + options.secretAccessKeyRef().orElse("<undefined>")
        + "}";
  }
}
