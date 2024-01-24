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
package org.projectnessie.catalog.iceberg.httpfileio;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.encoding.EncodingHandler;
import io.undertow.server.handlers.encoding.RequestEncodingHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.InstanceHandle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.iceberg.io.SeekableInputStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public class TestHttpInputFile {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void notFound(@TempDir Path tempDir) {
    try (HttpTestServer server = new HttpTestServer((req, resp) -> resp.sendError(404))) {
      HttpInputFile inputFile =
          new HttpInputFile(server.uri.resolve("foo").toString(), tempDir.toString());
      soft.assertThat(inputFile.exists()).isFalse();
    }
  }

  @Test
  public void linear(@TempDir Path tempDir) throws Exception {
    byte[] data = new byte[20000];
    ThreadLocalRandom.current().nextBytes(data);

    AtomicInteger requests = new AtomicInteger();

    try (HttpTestServer server =
        new HttpTestServer(
            (req, resp) -> {
              requests.incrementAndGet();
              resp.setStatus(200);
              resp.setContentType("avro/binary+linear");
              try (ServletOutputStream out = resp.getOutputStream()) {
                out.write(data);
              }
            })) {
      HttpInputFile inputFile =
          new HttpInputFile(server.uri.resolve("foo").toString(), tempDir.toString());

      soft.assertThat(requests.get()).isEqualTo(1);

      soft.assertThat(inputFile.exists()).isTrue();
      // Returns a fake, huge length
      soft.assertThat(inputFile.getLength()).isEqualTo(Long.MAX_VALUE);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);

        soft.assertThatThrownBy(() -> input.seek(0L))
            .isInstanceOf(UnsupportedOperationException.class);
      }
      soft.assertThat(requests.get()).isEqualTo(1);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);

        soft.assertThatThrownBy(() -> input.seek(0L))
            .isInstanceOf(UnsupportedOperationException.class);
      }
      soft.assertThat(requests.get()).isEqualTo(2);
    }
  }

  @Test
  public void spillingSeekingWithoutContentLength(@TempDir Path tempDir) throws Exception {
    byte[] data = new byte[20000];
    ThreadLocalRandom.current().nextBytes(data);

    AtomicInteger requests = new AtomicInteger();

    try (HttpTestServer server =
        new HttpTestServer(
            (req, resp) -> {
              requests.incrementAndGet();
              resp.setStatus(200);
              try (ServletOutputStream out = resp.getOutputStream()) {
                out.write(data);
              }
            })) {
      HttpInputFile inputFile =
          new HttpInputFile(server.uri.resolve("foo").toString(), tempDir.toString());

      soft.assertThat(requests.get()).isEqualTo(1);

      soft.assertThat(inputFile.exists()).isTrue();
      soft.assertThat(inputFile.getLength()).isEqualTo(data.length);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);

        input.seek(0L);
        soft.assertThat(input.getPos()).isEqualTo(0);
        readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(10000);
        soft.assertThat(input.getPos()).isEqualTo(10000);
        readData = readAll(input);
        soft.assertThat(data).endsWith(readData);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(0L);
        soft.assertThat(input.getPos()).isEqualTo(0);
        readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);
      }
      soft.assertThat(requests.get()).isEqualTo(1);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
      }
      soft.assertThat(requests.get()).isEqualTo(1);
    }
  }

  @Test
  public void spillingSeekingWithContentLength(@TempDir Path tempDir) throws Exception {
    byte[] data = new byte[20000];
    ThreadLocalRandom.current().nextBytes(data);

    AtomicInteger requests = new AtomicInteger();

    try (HttpTestServer server =
        new HttpTestServer(
            (req, resp) -> {
              requests.incrementAndGet();
              resp.setStatus(200);
              resp.addHeader("Content-Length", Integer.toString(data.length));
              try (ServletOutputStream out = resp.getOutputStream()) {
                out.write(data);
              }
            })) {
      HttpInputFile inputFile =
          new HttpInputFile(server.uri.resolve("foo").toString(), tempDir.toString());

      soft.assertThat(requests.get()).isEqualTo(1);

      soft.assertThat(inputFile.exists()).isTrue();
      soft.assertThat(inputFile.getLength()).isEqualTo(data.length);

      try (SeekableInputStream input = inputFile.newStream()) {
        soft.assertThat(input.getPos()).isEqualTo(0L);
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(0L);
        soft.assertThat(input.getPos()).isEqualTo(0L);
        readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(10000);
        soft.assertThat(input.getPos()).isEqualTo(10000L);
        readData = readAll(input);
        soft.assertThat(data).endsWith(readData);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(0L);
        readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);
      }
      soft.assertThat(requests.get()).isEqualTo(1);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
      }
      soft.assertThat(requests.get()).isEqualTo(1);
    }
  }

  @Test
  public void range(@TempDir Path tempDir) throws Exception {
    byte[] data = new byte[20000];
    ThreadLocalRandom.current().nextBytes(data);

    AtomicInteger rangeFrom = new AtomicInteger();

    try (HttpTestServer server =
        new HttpTestServer(
            (req, resp) -> {
              resp.setStatus(200);
              resp.addHeader("Accept-Ranges", "bytes");
              String range = req.getHeader("Range");
              if (range == null) {
                rangeFrom.set(-1);
                resp.addHeader("Content-Length", Integer.toString(data.length));
                try (ServletOutputStream out = resp.getOutputStream()) {
                  out.write(data);
                }
              } else {
                Pattern pattern = Pattern.compile("bytes=([0-9]+)-");
                Matcher matcher = pattern.matcher(range);
                soft.assertThat(matcher.matches()).isTrue();
                int startAt = Integer.parseInt(matcher.group(1));
                rangeFrom.set(startAt);
                resp.addHeader("Content-Length", Integer.toString(data.length - startAt));
                try (ServletOutputStream out = resp.getOutputStream()) {
                  out.write(data, startAt, data.length - startAt);
                }
              }
            })) {
      HttpInputFile inputFile =
          new HttpInputFile(server.uri.resolve("foo").toString(), tempDir.toString());

      soft.assertThat(rangeFrom.get()).isEqualTo(-1);

      soft.assertThat(inputFile.exists()).isTrue();
      soft.assertThat(inputFile.getLength()).isEqualTo(data.length);

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);

        input.seek(0L);
        soft.assertThat(input.getPos()).isEqualTo(0);
        readData = readAll(input);
        soft.assertThat(rangeFrom.get()).isEqualTo(0);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(10000);
        soft.assertThat(input.getPos()).isEqualTo(10000);
        readData = readAll(input);
        soft.assertThat(rangeFrom.get()).isEqualTo(10000);
        soft.assertThat(data).endsWith(readData);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(5000);
        soft.assertThat(input.getPos()).isEqualTo(5000);
        readData = readAll(input);
        soft.assertThat(rangeFrom.get()).isEqualTo(5000);
        soft.assertThat(data).endsWith(readData);
        soft.assertThat(input.getPos()).isEqualTo(data.length);

        input.seek(0L);
        readData = readAll(input);
        soft.assertThat(rangeFrom.get()).isEqualTo(0);
        soft.assertThat(readData).containsExactly(data);
        soft.assertThat(input.getPos()).isEqualTo(data.length);
      }

      try (SeekableInputStream input = inputFile.newStream()) {
        byte[] readData = readAll(input);
        soft.assertThat(readData).containsExactly(data);
      }
    }
  }

  private static byte[] readAll(InputStream input) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[256];
    while (true) {
      int rd = input.read(buf);
      if (rd < 0) {
        break;
      }
      out.write(buf, 0, rd);
    }
    return out.toByteArray();
  }

  @FunctionalInterface
  public interface RequestHandler {
    void handle(HttpServletRequest request, HttpServletResponse response) throws IOException;
  }

  static class HttpTestServer implements AutoCloseable {
    private final Undertow server;
    private final URI uri;

    HttpTestServer(RequestHandler handler) {

      DeploymentInfo servletBuilder =
          Servlets.deployment()
              .setClassLoader(HttpTestServer.class.getClassLoader())
              .setContextPath("/")
              .setDeploymentName("nessie-http-file-io-tests")
              .addServlets(
                  Servlets.servlet(
                          "nessie-http-file-io",
                          HttpServlet.class,
                          () ->
                              new InstanceHandle<>() {
                                final HttpServlet servlet =
                                    new HttpServlet() {
                                      @Override
                                      public void service(
                                          HttpServletRequest request, HttpServletResponse response)
                                          throws IOException {
                                        handler.handle(request, response);
                                      }
                                    };

                                @Override
                                public Servlet getInstance() {
                                  return servlet;
                                }

                                @Override
                                public void release() {}
                              })
                      .addInitParam("message", "Hello World")
                      .addMapping("/*"));

      DeploymentManager manager = Servlets.defaultContainer().addDeployment(servletBuilder);
      manager.deploy();

      HttpHandler httpHandler;
      try {
        httpHandler = manager.start();
      } catch (ServletException e) {
        throw new RuntimeException(e);
      }

      httpHandler = new EncodingHandler.Builder().build(Collections.emptyMap()).wrap(httpHandler);
      httpHandler =
          new RequestEncodingHandler.Builder().build(Collections.emptyMap()).wrap(httpHandler);

      Undertow.Builder server = Undertow.builder().setHandler(httpHandler);

      Undertow.ListenerBuilder listener =
          new Undertow.ListenerBuilder()
              .setPort(0)
              .setHost("localhost")
              .setType(Undertow.ListenerType.HTTP);
      server.setServerOption(UndertowOptions.ENABLE_HTTP2, true);
      server.addListener(listener);

      this.server = server.build();
      this.server.start();

      Undertow.ListenerInfo li = this.server.getListenerInfo().get(0);
      InetSocketAddress sa = (InetSocketAddress) li.getAddress();
      int port = sa.getPort();

      this.uri = URI.create("http://localhost:" + port + "/");
    }

    @Override
    public void close() {
      server.stop();
    }
  }
}
