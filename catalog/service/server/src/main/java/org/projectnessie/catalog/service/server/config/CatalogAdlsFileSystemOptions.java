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

import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.catalog.files.adls.AdlsFileSystemOptions;

public interface CatalogAdlsFileSystemOptions extends AdlsFileSystemOptions {
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

  @Override
  default Optional<RetryOptions> buildRetryOptions() {
    return AdlsFileSystemOptions.super.buildRetryOptions();
  }

  @Override
  default Optional<RequestRetryOptions> buildRequestRetryOptions() {
    return AdlsFileSystemOptions.super.buildRequestRetryOptions();
  }
}
