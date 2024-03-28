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

import org.projectnessie.catalog.files.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3Sessions {

  private final String repositoryId;
  private final CachingS3SessionsManager cache;

  public S3Sessions(String repositoryId, CachingS3SessionsManager cache) {
    this.repositoryId = repositoryId;
    this.cache = cache;
  }

  AwsCredentialsProvider assumeRole(S3BucketOptions options, SecretsProvider secretsProvider) {
    if (options.cloud().orElse(null) != Cloud.AMAZON) {
      if (options.stsEndpoint().isEmpty()) {
        throw new IllegalArgumentException(
            "STS endpoint must be provided for cloud: "
                + options.cloud().map(Cloud::name).orElse("<unset>"));
      }
    }

    return () -> {
      Credentials credentials = cache.sessionCredentials(repositoryId, options);
      return AwsSessionCredentials.create(
          credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
    };
  }
}
