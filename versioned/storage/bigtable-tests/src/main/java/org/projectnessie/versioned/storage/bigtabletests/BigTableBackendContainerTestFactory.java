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
package org.projectnessie.versioned.storage.bigtabletests;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** Bigtable emulator via testcontainers. */
public class BigTableBackendContainerTestFactory extends AbstractBigTableBackendTestFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BigTableBackendContainerTestFactory.class);
  public static final int BIGTABLE_PORT = 8086;

  private GenericContainer<?> container;
  private String emulatorHost;
  private int emulatorPort;

  @Override
  public String getName() {
    return BigTableBackendFactory.NAME + "Container";
  }

  protected static String dockerImage(String dbName) {
    URL resource =
        BigTableBackendContainerTestFactory.class.getResource("Dockerfile-" + dbName + "-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String image = imageTag[0];
      String version = System.getProperty("it.nessie.container." + dbName + ".tag", imageTag[1]);
      return image + ':' + version;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  @SuppressWarnings("resource")
  public void startBigtable(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String imageName = dockerImage("google-cloud-sdk");

    for (int retry = 0; ; retry++) {
      GenericContainer<?> c =
          new GenericContainer<>(imageName)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withExposedPorts(BIGTABLE_PORT)
              .withCommand(
                  "gcloud",
                  "beta",
                  "emulators",
                  "bigtable",
                  "start",
                  "--verbosity=info", // debug, info, warning, error, critical, none
                  "--host-port=0.0.0.0:" + BIGTABLE_PORT);
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    emulatorPort = containerNetworkId.isPresent() ? BIGTABLE_PORT : container.getFirstMappedPort();
    emulatorHost =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    projectId = "test-project";
    instanceId = "test-instance";
  }

  public String getEmulatorHost() {
    return emulatorHost;
  }

  public int getEmulatorPort() {
    return emulatorPort;
  }

  @Override
  public void start() {
    startBigtable(Optional.empty());
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
    }
  }
}
