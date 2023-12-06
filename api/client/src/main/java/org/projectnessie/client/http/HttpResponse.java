/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.projectnessie.client.rest.NessieBadResponseException;
import org.projectnessie.error.ImmutableNessieError;

/** Simple holder for http response object. */
public class HttpResponse {

  private final ResponseContext responseContext;
  private final ObjectMapper mapper;

  public HttpResponse(ResponseContext context, ObjectMapper mapper) {
    this.responseContext = context;
    this.mapper = mapper;
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  private <V> V readEntity(ObjectReader reader) {
    try {
      if (responseContext.getResponseCode().getCode() == Status.NO_CONTENT.getCode()) {
        // In case the Nessie server returns no content (because it's on an older version)
        // but the client expects a response, just return null here and cross fingers that
        // the code doesn't expect a non-null value.
        // This situation happened when `TreeApi.createReference()` started to return a value
        // but the tests in :nessie-versioned-gc-iceberg were running Nessie 0.4 server, which
        // doesn't return a value, so `org.projectnessie.versioned.gc.TestUtils#resetData` failed.
        return null;
      }
      try (InputStream is = responseContext.getInputStream()) {
        if (!responseContext.isJsonCompatibleResponse()) {
          nonJsonResponse();
        }

        return reader.readValue(is);
      }
    } catch (IOException e) {
      throw new HttpClientException("Cannot parse response.", e);
    }
  }

  private void nonJsonResponse() throws IOException {
    Status status = responseContext.getResponseCode();
    throw new NessieBadResponseException(
        ImmutableNessieError.builder()
            .status(status.getCode())
            .message(status.getReason())
            .reason(
                String.format(
                    "Expected the server to return a JSON compatible response, "
                        + "but the server returned with Content-Type '%s' from '%s'. "
                        + "Check the Nessie REST API base URI. "
                        + "Nessie REST API base URI usually end in '/api/v1', but your service "
                        + "provider may have a different URL pattern.",
                    responseContext.getContentType(), responseContext.getRequestedUri()))
            .build());
  }

  public <V> V readEntity(Class<V> clazz) {
    return readEntity(mapper.readerFor(clazz));
  }

  public URI getRequestUri() {
    return responseContext.getRequestedUri();
  }
}
