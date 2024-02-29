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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.s3.Cloud;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3Options;

@ConfigMapping(prefix = "nessie.catalog.service.s3")
public interface CatalogS3Config extends S3Config, S3Options<CatalogS3BucketConfig> {
  @WithName("throttled-retry-after")
  @WithDefault("PT10S")
  @Override
  Optional<Duration> retryAfter();

  @WithName("http.expect-continue-enabled")
  @Override
  Optional<Boolean> expectContinueEnabled();

  @WithName("http.connection-time-to-live")
  @Override
  Optional<Duration> connectionTimeToLive();

  @WithName("http.connection-max-idle-time")
  @Override
  Optional<Duration> connectionMaxIdleTime();

  @WithName("http.connection-acquisition-timeout")
  @Override
  Optional<Duration> connectionAcquisitionTimeout();

  @WithName("http.connect-timeout")
  @Override
  Optional<Duration> connectTimeout();

  @WithName("http.read-timeout")
  @Override
  Optional<Duration> readTimeout();

  @WithName("http.max-http-connections")
  @Override
  OptionalInt maxHttpConnections();

  @WithName("endpoint")
  @Override
  Optional<URI> endpoint();

  @WithName("cloud")
  @Override
  Optional<Cloud> cloud();

  @WithName("region")
  @Override
  Optional<String> region();

  @WithName("buckets")
  @Override
  Map<String, CatalogS3BucketConfig> buckets();
}
