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
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.projectnessie.catalog.files.adls.AdlsConfig;
import org.projectnessie.catalog.files.adls.AdlsOptions;

@ConfigMapping(prefix = "nessie.catalog.service.adls")
public interface CatalogAdlsConfig extends AdlsConfig, AdlsOptions<CatalogAdlsFileSystemOptions> {
  @WithName("max-http-connections")
  @Override
  OptionalInt maxHttpConnections();

  @WithName("connect-timeout")
  @Override
  Optional<Duration> connectTimeout();

  @WithName("connection-idle-timeout")
  @Override
  Optional<Duration> connectionIdleTimeout();

  @WithName("write-timeout")
  @Override
  Optional<Duration> writeTimeout();

  @WithName("read-timeout")
  @Override
  Optional<Duration> readTimeout();

  @WithName("response-timeout")
  @Override
  Optional<Duration> responseTimeout();

  @WithName("configuration")
  @Override
  Map<String, String> configurationOptions();

  @WithName("write-block-size")
  @Override
  OptionalLong writeBlockSize();

  // file-system options

  @WithName("account-name-ref")
  @Override
  Optional<String> accountNameRef();

  @WithName("account-key-ref")
  @Override
  Optional<String> accountKeyRef();

  @WithName("sas-token-ref")
  @Override
  Optional<String> sasTokenRef();

  @WithName("endpoint")
  @Override
  Optional<String> endpoint();

  @WithName("external-endpoint")
  @Override
  Optional<String> externalEndpoint();

  @WithName("retry-policy")
  @Override
  Optional<AdlsRetryStrategy> retryPolicy();

  @WithName("max-retries")
  @Override
  Optional<Integer> maxRetries();

  @WithName("try-timeout")
  @Override
  Optional<Duration> tryTimeout();

  @WithName("retry-delay")
  @Override
  Optional<Duration> retryDelay();

  @WithName("max-retry-delyy")
  @Override
  Optional<Duration> maxRetryDelay();

  @WithName("file-systems")
  @Override
  Map<String, CatalogAdlsFileSystemOptions> fileSystems();
}
