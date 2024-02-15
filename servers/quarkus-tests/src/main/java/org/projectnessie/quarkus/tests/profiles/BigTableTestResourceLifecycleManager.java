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
package org.projectnessie.quarkus.tests.profiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.bigtabletests.BigTableBackendContainerTestFactory;

public class BigTableTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private BigTableBackendContainerTestFactory bigTable;

  private Optional<String> containerNetworkId;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    bigTable = new BigTableBackendContainerTestFactory();
    bigTable.startBigtable(containerNetworkId);

    return ImmutableMap.of(
        "nessie.version.store.persist.bigtable.emulator-host",
        bigTable.getEmulatorHost(),
        "nessie.version.store.persist.bigtable.emulator-port",
        Integer.toString(bigTable.getEmulatorPort()));
  }

  @Override
  public void stop() {
    if (bigTable != null) {
      try {
        bigTable.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        bigTable = null;
      }
    }
  }
}
