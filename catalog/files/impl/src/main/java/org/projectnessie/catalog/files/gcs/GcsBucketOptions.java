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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public interface GcsBucketOptions {

  Optional<URI> host();

  Optional<URI> externalHost();

  Optional<String> projectId();

  Optional<String> quotaProjectId();

  Optional<String> clientLibToken();

  Optional<GcsAuthType> authType();

  Optional<String> authCredentialsJsonRef();

  Optional<String> oauth2TokenRef();

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

  /** The read chunk size in bytes. */
  OptionalInt readChunkSize();

  /** The write chunk size in bytes. */
  OptionalInt writeChunkSize();

  OptionalInt deleteBatchSize();

  /**
   * Customer-supplied AES256 key for blob encryption when writing.
   *
   * @implNote This is currently unsupported.
   */
  Optional<String> encryptionKeyRef();

  /**
   * Customer-supplied AES256 key for blob decryption when reading.
   *
   * @implNote This is currently unsupported.
   */
  Optional<String> decryptionKeyRef();

  Optional<String> userProject();

  Optional<Duration> readTimeout();

  Optional<Duration> connectTimeout();

  enum GcsAuthType {
    NONE,
    USER,
    SERVICE_ACCOUNT,
    ACCESS_TOKEN,
  }
}
