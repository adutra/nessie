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
package org.projectnessie.catalog.files.gcs;

import com.google.api.gax.retrying.RetrySettings;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.Function;

public interface GcsBucketOptions {

  Optional<URI> host();

  Optional<String> projectId();

  Optional<String> quotaProjectId();

  Optional<String> clientLibToken();

  Optional<GcsAuthType> authType();

  Optional<String> authCredentialsJsonRef();

  Optional<String> oauth2tokenRef();

  Optional<Instant> oauth2TokenExpiresAt();

  OptionalInt maxAttempts();

  Optional<Duration> logicalTimeout();

  Optional<Duration> totalTimeout();

  Optional<Duration> initialRetryDelay();

  Optional<Duration> maxRetryDelay();

  OptionalDouble retryDelayMultiplier();

  Optional<Duration> initialRpcTimeout();

  Optional<Duration> maxRpcTimeout();

  OptionalDouble rpcTimeoutMultiplier();

  OptionalInt readChunkSize();

  OptionalInt writeChunkSize();

  Optional<String> encryptionKeyRef();

  Optional<String> userProject();

  Optional<Duration> readTimeout();

  Optional<Duration> connectTimeout();

  enum GcsAuthType {
    NONE,
    USER,
    SERVICE_ACCOUNT,
    ACCESS_TOKEN,
  }

  default Optional<RetrySettings> buildRetrySettings() {
    Function<Duration, org.threeten.bp.Duration> duration =
        d -> org.threeten.bp.Duration.ofMillis(d.toMillis());

    RetrySettings.Builder retry = RetrySettings.newBuilder();
    maxAttempts().ifPresent(retry::setMaxAttempts);
    logicalTimeout().map(duration).ifPresent(retry::setLogicalTimeout);
    totalTimeout().map(duration).ifPresent(retry::setTotalTimeout);

    initialRetryDelay().map(duration).ifPresent(retry::setInitialRetryDelay);
    maxRetryDelay().map(duration).ifPresent(retry::setMaxRetryDelay);
    retryDelayMultiplier().ifPresent(retry::setRetryDelayMultiplier);

    initialRpcTimeout().map(duration).ifPresent(retry::setInitialRpcTimeout);
    maxRpcTimeout().map(duration).ifPresent(retry::setMaxRpcTimeout);
    rpcTimeoutMultiplier().ifPresent(retry::setRpcTimeoutMultiplier);

    return Optional.of(retry.build());
  }
}
