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

import java.io.InputStream;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

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
}
