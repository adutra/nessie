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

import java.util.Optional;
import org.projectnessie.catalog.api.sign.ImmutableSigningResponse;
import org.projectnessie.catalog.api.sign.SigningRequest;
import org.projectnessie.catalog.api.sign.SigningResponse;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

public class S3Signer implements RequestSigner {

  private final AwsS3V4Signer signer = AwsS3V4Signer.create();
  private final S3Options<? extends S3BucketOptions> s3Options;
  private final SecretsProvider secretsProvider;

  public S3Signer(S3Options<? extends S3BucketOptions> s3Options, SecretsProvider secretsProvider) {
    this.s3Options = s3Options;
    this.secretsProvider = secretsProvider;
  }

  @Override
  public SigningResponse sign(String ref, String key, SigningRequest clientRequest) {

    if ("post".equalsIgnoreCase(clientRequest.method())
        && clientRequest.uri().getQuery().contains("delete")) {
      String body = clientRequest.body();
      if (body == null || body.isEmpty()) {
        throw new IllegalArgumentException("DELETE requests must have a non-empty body");
      }
    }

    S3BucketOptions bucketOptions =
        s3Options.effectiveOptionsForBucket(Optional.ofNullable(clientRequest.bucket()));
    AwsCredentialsProvider credentialsProvider =
        S3Clients.awsCredentialsProvider(bucketOptions, secretsProvider);

    SdkHttpFullRequest request =
        SdkHttpFullRequest.builder()
            .uri(clientRequest.uri())
            .method(SdkHttpMethod.fromValue(clientRequest.method()))
            .headers(clientRequest.headers())
            .build();

    AwsS3V4SignerParams params =
        AwsS3V4SignerParams.builder()
            .signingRegion(Region.of(clientRequest.region()))
            .signingName("s3")
            .awsCredentials(credentialsProvider.resolveCredentials())
            .build();

    SdkHttpFullRequest signed = signer.sign(request, params);

    return ImmutableSigningResponse.of(signed.getUri(), signed.headers());
  }
}
