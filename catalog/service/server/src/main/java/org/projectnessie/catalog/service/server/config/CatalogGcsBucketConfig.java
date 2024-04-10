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
package org.projectnessie.catalog.service.server.config;

import com.google.api.gax.retrying.RetrySettings;
import io.smallrye.config.WithName;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;

public interface CatalogGcsBucketConfig extends GcsBucketOptions {

  @WithName("host")
  @Override
  Optional<URI> host();

  @WithName("external-host")
  @Override
  Optional<URI> externalHost();

  @WithName("project-id")
  @Override
  Optional<String> projectId();

  @WithName("quota-project-id")
  @Override
  Optional<String> quotaProjectId();

  @WithName("client-lib-token")
  @Override
  Optional<String> clientLibToken();

  @WithName("auth-type")
  @Override
  Optional<GcsAuthType> authType();

  @WithName("auth-credentials-json-ref")
  @Override
  Optional<String> authCredentialsJsonRef();

  @WithName("oauth2-token-ref")
  @Override
  Optional<String> oauth2TokenRef();

  @WithName("oauth2-token-expires-at")
  @Override
  Optional<Instant> oauth2TokenExpiresAt();

  @Override
  OptionalInt maxAttempts();

  @Override
  Optional<Duration> logicalTimeout();

  @Override
  Optional<Duration> totalTimeout();

  @Override
  Optional<Duration> initialRetryDelay();

  @Override
  Optional<Duration> maxRetryDelay();

  @Override
  OptionalDouble retryDelayMultiplier();

  @Override
  Optional<Duration> initialRpcTimeout();

  @Override
  Optional<Duration> maxRpcTimeout();

  @Override
  OptionalDouble rpcTimeoutMultiplier();

  @Override
  OptionalInt readChunkSize();

  @Override
  OptionalInt writeChunkSize();

  @Override
  OptionalInt deleteBatchSize();

  @Override
  Optional<String> encryptionKeyRef();

  @Override
  Optional<String> decryptionKeyRef();

  @Override
  Optional<String> userProject();

  @Override
  Optional<Duration> readTimeout();

  @Override
  Optional<Duration> connectTimeout();

  @Override
  default Optional<RetrySettings> buildRetrySettings() {
    return GcsBucketOptions.super.buildRetrySettings();
  }
}
