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

import java.net.URI;
import java.util.Optional;

public interface S3BucketOptions {
  /**
   * The type of cloud running the S3 service. The cloud type must be configured, either per bucket
   * or in {@link S3Options S3Options}.
   */
  Optional<Cloud> cloud();

  /**
   * Endpoint URI, required for {@linkplain Cloud#PRIVATE private clouds}. The endpoint must be
   * specified for {@linkplain Cloud#PRIVATE private clouds}, either per bucket or in {@link
   * S3Options S3Options}.
   */
  Optional<URI> endpoint();

  /**
   * DNS name of the region, required for {@linkplain Cloud#AMAZON AWS}. The region must be
   * specified for {@linkplain Cloud#AMAZON AWS}, either per bucket or in {@link S3Options
   * S3Options}.
   */
  Optional<String> region();

  /** Project ID, if required, for example for {@linkplain Cloud#GOOGLE Google cloud}. */
  Optional<String> projectId();

  /**
   * Key used to look up the access-key-ID via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. An access-key-id must
   * be configured, either per bucket or in {@link S3Options S3Options}.
   */
  Optional<String> accessKeyIdRef();

  /**
   * Key used to look up the secret-access-key via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. A secret-access-key
   * must be configured, either per bucket or in {@link S3Options S3Options}.
   */
  Optional<String> secretAccessKeyRef();
}
