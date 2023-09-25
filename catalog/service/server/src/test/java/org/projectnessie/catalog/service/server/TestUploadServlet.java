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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class TestUploadServlet {
  @TestHTTPResource(value = "/upload")
  URI uploadUrl;

  @TestHTTPResource(value = "/")
  URI rootUrl;

  @Test
  public void uploadHugeAmountOfData() throws Exception {
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();

    // Send some request - just to let the HTTP upgrade happen first. Otherwise, Java's HTTP client
    // fails with an expectation-failed-error, because it expects an HTTP/100, but gets an HTTP/101
    // for the HTTP 1.1->2 upgrade.
    client.send(
        HttpRequest.newBuilder(rootUrl).GET().build(), HttpResponse.BodyHandlers.ofString());

    // Equates to 8G of data, which would cause an OOM if anything would buffer.
    byte[] justData = new byte[65536];
    int chunks = 128 * 1024;
    long total = ((long) chunks) * justData.length;

    HttpRequest request =
        HttpRequest.newBuilder(uploadUrl)
            .header("Content-Type", "multipart/form-data")
            .POST(
                HttpRequest.BodyPublishers.ofByteArrays(
                    () ->
                        new Iterator<>() {
                          int chunk;

                          @Override
                          public boolean hasNext() {
                            return chunk < chunks;
                          }

                          @Override
                          public byte[] next() {
                            chunk++;
                            return justData;
                          }
                        }))
            .expectContinue(true)
            .build();

    HttpResponse.BodyHandler<String> responseHandler = HttpResponse.BodyHandlers.ofString();
    HttpResponse<String> response = client.send(request, responseHandler);

    assertThat(response)
        .extracting(HttpResponse::statusCode, HttpResponse::body)
        .containsExactly(200, "Got " + total + " bytes");
  }
}
