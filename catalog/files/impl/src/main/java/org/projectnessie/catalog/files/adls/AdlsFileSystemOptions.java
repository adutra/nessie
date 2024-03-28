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
package org.projectnessie.catalog.files.adls;

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.FixedDelayOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.util.Optional;

public interface AdlsFileSystemOptions {

  Optional<String> accountNameRef();

  Optional<String> accountKeyRef();

  Optional<String> sasTokenRef();

  Optional<String> endpoint();

  Optional<AdlsRetryStrategy> retryPolicy();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Integer> maxRetries();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Duration> tryTimeout();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Duration> retryDelay();

  /** Mandatory, if {@link AdlsRetryStrategy#EXPONENTIAL_BACKOFF} is configured. */
  Optional<Duration> maxRetryDelay();

  enum AdlsRetryStrategy {
    /** Same as not configuring a retry strategy. */
    NONE,
    EXPONENTIAL_BACKOFF,
    FIXED_DELAY,
  }

  // Both RetryOptions + RequestRetryOptions look redundant, but neither type inherits the other -
  // so :shrug:

  default Optional<RetryOptions> buildRetryOptions() {
    return retryPolicy()
        .flatMap(
            strategy -> {
              switch (strategy) {
                case NONE:
                  return Optional.empty();
                case EXPONENTIAL_BACKOFF:
                  ExponentialBackoffOptions exponentialBackoffOptions =
                      new ExponentialBackoffOptions();
                  retryDelay().ifPresent(exponentialBackoffOptions::setBaseDelay);
                  maxRetryDelay().ifPresent(exponentialBackoffOptions::setMaxDelay);
                  maxRetries().ifPresent(exponentialBackoffOptions::setMaxRetries);
                  return Optional.of(new RetryOptions(exponentialBackoffOptions));
                case FIXED_DELAY:
                  FixedDelayOptions fixedDelayOptions =
                      new FixedDelayOptions(maxRetries().orElseThrow(), retryDelay().orElseThrow());
                  return Optional.of(new RetryOptions(fixedDelayOptions));
                default:
                  throw new IllegalArgumentException("Invalid retry strategy: " + strategy);
              }
            });
  }

  default Optional<RequestRetryOptions> buildRequestRetryOptions() {
    return retryPolicy()
        .flatMap(
            strategy -> {
              switch (strategy) {
                case NONE:
                  return Optional.empty();
                case EXPONENTIAL_BACKOFF:
                  return Optional.of(
                      new RequestRetryOptions(
                          RetryPolicyType.EXPONENTIAL,
                          maxRetries().orElse(null),
                          tryTimeout().orElse(null),
                          retryDelay().orElse(null),
                          maxRetryDelay().orElse(null),
                          null));
                case FIXED_DELAY:
                  return Optional.of(
                      new RequestRetryOptions(
                          RetryPolicyType.FIXED,
                          maxRetries().orElse(null),
                          tryTimeout().orElse(null),
                          retryDelay().orElse(null),
                          maxRetryDelay().orElse(null),
                          null));
                default:
                  throw new IllegalArgumentException("Invalid retry strategy: " + strategy);
              }
            });
  }
}
