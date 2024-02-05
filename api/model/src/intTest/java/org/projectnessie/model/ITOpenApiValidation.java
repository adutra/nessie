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
package org.projectnessie.model;

import static java.lang.Boolean.FALSE;
import static java.lang.System.getProperty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.copy;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PullResponseItem;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;

public class ITOpenApiValidation {
  @Test
  public void validateOpenApi(@TempDir Path workDir) throws IOException, InterruptedException {
    copy(
        Paths.get(getProperty("openapiSchemaDir") + "/openapi.yaml"),
        workDir.resolve("openapi.yaml"));

    // The ignore-file can be re-generated using these commands:
    //  ./gradlew :nessie-model:generateOpenApiSpec
    //  docker run --rm \
    //    -v $(realpath api/model/build/generated/openapi/META-INF/openapi):/spec \
    //    docker.io/redocly/cli \
    //    lint \
    //    --generate-ignore-file \
    //    openapi.yaml

    copy(
        Paths.get(getProperty("redoclyConfDir") + "/.redocly.lint-ignore.yaml"),
        workDir.resolve(".redocly.lint-ignore.yaml"));

    // Global client instance, not closable (throws an ISE)
    @SuppressWarnings("resource")
    DockerClient dockerClient = DockerClientFactory.instance().client();

    // Intentionally a StringBuffer, because it's synchronized
    StringBuffer buffer = new StringBuffer();

    String image = dockerImage("redocly");

    if (dockerClient.listImagesCmd().withImageNameFilter(image).exec().isEmpty()) {
      dockerClient
          .pullImageCmd(image)
          .exec(
              new ResultCallback.Adapter<>() {
                @Override
                public void onNext(PullResponseItem pullResponse) {
                  // Just print something
                  System.out.println(pullResponse);
                }
              })
          .awaitCompletion(5, MINUTES);
    }

    CreateContainerResponse exec =
        dockerClient
            .createContainerCmd(image)
            .withCmd(asList("lint", "openapi.yaml"))
            .withHostConfig(
                HostConfig.newHostConfig()
                    .withBinds(Bind.parse(workDir.toAbsolutePath() + ":/spec")))
            .exec();
    dockerClient
        .logContainerCmd(exec.getId())
        .withStdOut(true)
        .withStdErr(true)
        .withFollowStream(true)
        .exec(
            new ResultCallback.Adapter<>() {
              @Override
              public void onNext(Frame frame) {
                buffer.append(new String(frame.getPayload()).trim()).append('\n');
              }
            });

    try {
      dockerClient.startContainerCmd(exec.getId()).exec();

      await()
          .timeout(30, SECONDS)
          .pollInterval(100, MICROSECONDS)
          .until(
              () -> {
                InspectContainerResponse.ContainerState state =
                    dockerClient.inspectContainerCmd(exec.getId()).exec().getState();
                return FALSE.equals(state.getRunning());
              });

      InspectContainerResponse.ContainerState state =
          dockerClient.inspectContainerCmd(exec.getId()).exec().getState();
      assertThat(state.getExitCodeLong()).describedAs("%s", buffer).isEqualTo(0L);
    } finally {
      try {
        dockerClient.stopContainerCmd(exec.getId()).exec();
      } catch (NotModifiedException ignore) {
        // already stopped
      }
      dockerClient.removeContainerCmd(exec.getId()).exec();
    }
  }

  protected static String dockerImage(String name) {
    URL resource = ITOpenApiValidation.class.getResource("Dockerfile-" + name + "-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String image = imageTag[0];
      String version = System.getProperty("it.nessie.container." + name + ".tag", imageTag[1]);
      return image + ':' + version;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }
}
