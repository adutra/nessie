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
package org.projectnessie.catalog.service.server;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class UploadRoute {

  // TODO THIS IS A SAMPLE COMMAND
  //   curl -v -o /dev/stdout \
  //     --upload-file '/home/snazy/VirtualBox VMs/Win10/Win10.vdi' \
  //     -X POST http://127.0.0.1:19110/upload

  /**
   * Installs the VertX-Web route to implement non-buffering file uploads.
   *
   * <p>This seems to be the <em>only</em> way to bypass Quarkus/Vert.X-Web's body-handler, which
   * would materialize the whole upload in memory.
   */
  void installRoute(@Observes StartupEvent startupEvent, Router router) {
    // Setting `order(-3)` is necessary to get our route placed _before_ Quarkus' body-size-limit
    // enforcement of `quarkus.http.limits.max-body-size` (defaults to 10M).
    router.post("/upload").order(-3).handler(UploadHandler::newRequest);
  }

  static class UploadHandler {
    final HttpServerRequest req;
    final HttpServerResponse resp;

    final AtomicLong total = new AtomicLong();

    UploadHandler(HttpServerRequest req, HttpServerResponse resp) {
      this.req = req;
      this.resp = resp;
    }

    void end(Void x) {
      resp.setStatusCode(200)
          .setStatusMessage("OK")
          .putHeader("Content-Type", "text/plain")
          .end("Got " + total + " bytes");
    }

    void onData(Buffer buffer) {
      total.addAndGet(buffer.length());
    }

    void onException(Throwable exception) {
      exception.printStackTrace();
      resp.setStatusCode(500)
          .setStatusMessage("Internal Server Error")
          .end("Failed to process request.");
    }

    static void newRequest(RoutingContext routingContext) {
      HttpServerRequest req = routingContext.request();
      HttpServerResponse resp = routingContext.response();

      String expectValue = req.getHeader(HttpHeaders.EXPECT);
      if (expectValue != null) {
        if (!"100-continue".equalsIgnoreCase(expectValue)) {
          routingContext.fail(417);
        }
        if (req.version() != HttpVersion.HTTP_1_0) {
          resp.writeContinue();
        }
      }

      UploadHandler uploadHandler = new UploadHandler(req, resp);
      req.handler(uploadHandler::onData)
          .endHandler(uploadHandler::end)
          .exceptionHandler(uploadHandler::onException);
    }
  }
}
