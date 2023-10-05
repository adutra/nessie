/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import java.util.List;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.threeten.bp.Duration;

public class BigTableBackendFactory implements BackendFactory<BigTableBackendConfig> {

  public static final String NAME = "BigTable";

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public BigTableBackendConfig newConfigInstance() {
    return BigTableBackendConfig.builder().build();
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Backend buildBackend(@Nonnull @jakarta.annotation.Nonnull BigTableBackendConfig config) {
    return new BigTableBackend(config, false);
  }

  public static void configureDataClient(Builder settings, boolean enableTelemetry) {
    Duration maxRetryDelay = Duration.ofSeconds(1);

    EnhancedBigtableStubSettings.Builder stubSettings = settings.stubSettings();
    for (RetrySettings.Builder retrySettings :
        List.of(
            stubSettings.readRowSettings().retrySettings(),
            stubSettings.readRowsSettings().retrySettings(),
            stubSettings.bulkReadRowsSettings().retrySettings(),
            stubSettings.mutateRowSettings().retrySettings(),
            stubSettings.bulkMutateRowsSettings().retrySettings(),
            stubSettings.readChangeStreamSettings().retrySettings())) {
      // The max-retry-delay is 1 minute, which is pretty high for us.
      retrySettings.setMaxRetryDelay(maxRetryDelay);
    }

    stubSettings
        .bulkMutateRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigTableConstants.MAX_BULK_MUTATIONS)
                .build());

    stubSettings
        .bulkReadRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkReadRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigTableConstants.MAX_BULK_READS)
                .build());

    // Enable tracing & metrics
    if (enableTelemetry) {
      BigtableDataSettings.enableOpenCensusStats();
      BigtableDataSettings.enableGfeOpenCensusStats();
    }
  }
}
