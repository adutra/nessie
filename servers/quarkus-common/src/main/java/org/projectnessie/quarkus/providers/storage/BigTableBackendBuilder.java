/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.BIGTABLE;
import static org.projectnessie.versioned.storage.bigtable.BigTableBackendFactory.configureDataClient;
import static org.projectnessie.versioned.storage.bigtable.BigTableBackendFactory.configureDuration;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.retrying.RetrySettings.Builder;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.quarkiverse.googlecloudservices.common.GcpBootstrapConfiguration;
import io.quarkiverse.googlecloudservices.common.GcpConfigHolder;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.quarkus.config.QuarkusBigTableConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendConfig;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendFactory;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StoreType(BIGTABLE)
@Dependent
public class BigTableBackendBuilder implements BackendBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigTableBackendBuilder.class);

  @Inject QuarkusBigTableConfig bigTableConfig;

  @Inject Instance<CredentialsProvider> credentialsProviderInstance;

  @Inject Instance<GcpConfigHolder> gcpConfigHolderInstance;

  @Override
  public Backend buildBackend() {
    CredentialsProvider credentialsProvider = null;
    Exception googleCredentialsException = null;
    try {
      credentialsProvider = credentialsProviderInstance.get();
    } catch (Exception e) {
      googleCredentialsException = e;
    }
    GcpConfigHolder gcpConfigHolder = gcpConfigHolderInstance.get();

    GcpBootstrapConfiguration gcpConfiguration = gcpConfigHolder.getBootstrapConfig();

    String projectId =
        gcpConfiguration
            .projectId()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Required Google gRPC configuration quarkus.google.cloud.project-id is missing"));

    if (bigTableConfig.emulatorHost().isEmpty()) {
      if (credentialsProvider == null) {
        if (googleCredentialsException != null) {
          throw new RuntimeException(
              "No Google CredentialsProvider available", googleCredentialsException);
        } else {
          throw new RuntimeException("No Google CredentialsProvider available");
        }
      }
      LOGGER.info(
          "Connecting to Google BigTable using project ID {}, instance ID {}, via endpoint {}",
          projectId,
          bigTableConfig.instanceId(),
          bigTableConfig.endpoint().orElse("(default)"));
    } else {
      LOGGER.warn(
          "Connecting to Google BigTable emulator {}:{} using project ID {}, instance ID {}",
          bigTableConfig.emulatorHost().get(),
          bigTableConfig.emulatorPort(),
          projectId,
          bigTableConfig.instanceId());
    }

    try {
      BigtableDataSettings.Builder dataSettings =
          (bigTableConfig.emulatorHost().isPresent()
              ? BigtableDataSettings.newBuilderForEmulator(
                      bigTableConfig.emulatorHost().get(), bigTableConfig.emulatorPort())
                  .setCredentialsProvider(NoCredentialsProvider.create())
              : BigtableDataSettings.newBuilder().setCredentialsProvider(credentialsProvider));
      dataSettings.setProjectId(projectId).setInstanceId(bigTableConfig.instanceId());
      bigTableConfig.mtlsEndpoint().ifPresent(dataSettings.stubSettings()::setMtlsEndpoint);
      bigTableConfig.quotaProjectId().ifPresent(dataSettings.stubSettings()::setQuotaProjectId);
      bigTableConfig.endpoint().ifPresent(dataSettings.stubSettings()::setEndpoint);
      if (!bigTableConfig.jwtAudienceMapping().isEmpty()) {
        dataSettings.stubSettings().setJwtAudienceMapping(bigTableConfig.jwtAudienceMapping());
      }

      ChannelPoolSettings defaultPoolSettings = ChannelPoolSettings.builder().build();

      ChannelPoolSettings poolSettings =
          ChannelPoolSettings.builder()
              .setMinChannelCount(
                  bigTableConfig.minChannelCount().orElse(defaultPoolSettings.getMinChannelCount()))
              .setMaxChannelCount(
                  bigTableConfig.maxChannelCount().orElse(defaultPoolSettings.getMaxChannelCount()))
              .setInitialChannelCount(
                  bigTableConfig
                      .initialChannelCount()
                      .orElse(defaultPoolSettings.getInitialChannelCount()))
              .setMinRpcsPerChannel(
                  bigTableConfig
                      .minRpcsPerChannel()
                      .orElse(defaultPoolSettings.getMinRpcsPerChannel()))
              .setMaxRpcsPerChannel(
                  bigTableConfig
                      .maxRpcsPerChannel()
                      .orElse(defaultPoolSettings.getMaxRpcsPerChannel()))
              .setPreemptiveRefreshEnabled(true)
              .build();

      configureDataClient(
          dataSettings,
          Optional.of(poolSettings),
          bigTableConfig.maxRetryDelay(),
          bigTableConfig.initialRpcTimeout(),
          bigTableConfig.initialRetryDelay());

      String mainProfile = bigTableConfig.appProfileId().orElse("(default)");
      String singleClusterProfile = bigTableConfig.singleClusterAppProfileId().orElse(mainProfile);

      LOGGER.info(
          "Creating Google BigTable data clients with main profile: {} and single-cluster profile: {}...",
          mainProfile,
          singleClusterProfile);

      bigTableConfig.appProfileId().ifPresent(dataSettings::setAppProfileId);
      BigtableDataClient dataClient = BigtableDataClient.create(dataSettings.build());

      BigtableDataClient singleClusterDataClient;
      if (bigTableConfig.singleClusterAppProfileId().isPresent()) {
        dataSettings.setAppProfileId(bigTableConfig.singleClusterAppProfileId().get());
        singleClusterDataClient = BigtableDataClient.create(dataSettings.build());
      } else {
        singleClusterDataClient = dataClient;
      }

      BigtableTableAdminClient tableAdminClient = null;
      if (bigTableConfig.noTableAdminClient()) {
        LOGGER.info("Google BigTable table admin client creation disabled.");
      } else {

        BigtableTableAdminSettings.Builder adminSettings =
            bigTableConfig.emulatorHost().isPresent()
                ? BigtableTableAdminSettings.newBuilderForEmulator(
                        bigTableConfig.emulatorHost().get(), bigTableConfig.emulatorPort())
                    .setCredentialsProvider(NoCredentialsProvider.create())
                : BigtableTableAdminSettings.newBuilder()
                    .setCredentialsProvider(credentialsProvider);
        adminSettings.setProjectId(projectId).setInstanceId(bigTableConfig.instanceId());
        bigTableConfig.mtlsEndpoint().ifPresent(adminSettings.stubSettings()::setMtlsEndpoint);
        bigTableConfig.quotaProjectId().ifPresent(adminSettings.stubSettings()::setQuotaProjectId);
        bigTableConfig.endpoint().ifPresent(adminSettings.stubSettings()::setEndpoint);

        adminSettings
          .stubSettings()
          .checkConsistencySettings()
          .setSimpleTimeoutNoRetries(org.threeten.bp.Duration.ofMinutes(5));

        LOGGER.info("Creating Google BigTable table admin client...");
        tableAdminClient = BigtableTableAdminClient.create(adminSettings.build());

        // Check whether the admin client actually works (Google cloud API access could be
        // disabled). If not, we cannot even check whether tables need to be created, if necessary.
        try {
          tableAdminClient.listTables();
        } catch (PermissionDeniedException e) {
          LOGGER.warn(
              "Google BigTable table admin client cannot list tables due to {}.", e.toString());
          try {
            tableAdminClient.close();
          } finally {
            tableAdminClient = null;
          }
        }
      }

      BigTableBackendFactory factory = new BigTableBackendFactory();
      BigTableBackendConfig c =
          BigTableBackendConfig.builder()
              .dataClient(dataClient)
              .singleClusterDataClient(singleClusterDataClient)
              .tableAdminClient(tableAdminClient)
              .tablePrefix(bigTableConfig.tablePrefix())
              .build();

      return factory.buildBackend(c);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
