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
package org.projectnessie.catalog.service.rest.nessieproxy;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Request;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.client.api.NessieApiV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxies HTTP (REST) requests down to the remote Nessie service. Having a proxy resource allows us
 * to re-use the already available Nessie base URI in clients like Apache Iceberg.
 */
@RequestScoped
@Consumes
@Produces
@Path("api")
@Tag(name = "Nessie REST API proxy")
public class NessieProxyResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieProxyResource.class);

  protected static final List<String> REQUEST_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Encoding",
          "Authorization",
          "Accept",
          "Accept-Encoding",
          "Accept-Language",
          "Nessie-Client-Spec");
  protected static final List<String> RESPONSE_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Length",
          "Content-Encoding",
          "Accept",
          "Expires",
          "Cache-Control",
          "Last-Modified",
          "ETag",
          "Vary");

  @Inject protected NessieApiV2 nessieApi;
  @Inject protected HttpClientHolder httpClientHolder;

  @Inject protected UriInfo uriInfo;
  @Inject protected Request request;
  @Inject protected HttpHeaders httpHeaders;

  @HEAD
  @Path("{path:.*}")
  public Response doHead(@PathParam("path") String path) {
    return doProxy(null);
  }

  @GET
  @Path("{path:.*}")
  public Response doGet(@PathParam("path") String path) {
    return doProxy(null);
  }

  @POST
  @Path("{path:.*}")
  public Response doPost(@PathParam("path") String path, InputStream data) {
    return doProxy(data);
  }

  @PUT
  @Path("{path:.*}")
  public Response doPut(@PathParam("path") String path, InputStream data) {
    return doProxy(data);
  }

  @DELETE
  @Path("{path:.*}")
  public Response doDelete(@PathParam("path") String path) {
    return doProxy(null);
  }

  protected Response doProxy(InputStream data) {
    try {
      HttpResponse<InputStream> httpResponse = proxyRequest(data);

      ResponseBuilder responseBuilder = Response.status(httpResponse.statusCode());
      responseHeaders(httpResponse.headers()::allValues, responseBuilder::header);

      return responseBuilder.entity(httpResponse.body()).build();
    } catch (WebApplicationException e) {
      return e.getResponse();
    } catch (Exception e) {
      LOGGER.error(
          "Internal error during proxy of {} {}", request.getMethod(), uriInfo.getRequestUri(), e);
      return new ServerErrorException("Internal error", Response.Status.INTERNAL_SERVER_ERROR)
          .getResponse();
    }
  }

  protected HttpResponse<InputStream> proxyRequest(InputStream data)
      throws IOException, InterruptedException {
    URI target = proxyTo();

    @SuppressWarnings("resource")
    HttpClient httpClient = httpClientHolder.httpClient();

    HttpRequest.BodyPublisher bodyPublisher =
        data != null
            ? HttpRequest.BodyPublishers.ofInputStream(() -> data)
            : HttpRequest.BodyPublishers.noBody();

    HttpRequest.Builder httpRequest =
        HttpRequest.newBuilder(target)
            .timeout(httpClientHolder.requestTimeout())
            .method(request.getMethod(), bodyPublisher);
    requestHeaders(httpRequest::header);

    HttpResponse.BodyHandler<InputStream> bodyHandler = HttpResponse.BodyHandlers.ofInputStream();

    return httpClient.send(httpRequest.build(), bodyHandler);
  }

  protected URI proxyTo() {
    // Need the request URI, which has everything we need - the path and query parameters
    URI reqUri = uriInfo.getRequestUri();
    // "Our" base-URI is the one for this resource
    URI baseUri = uriInfo.getBaseUri().resolve("api/");

    // Relativize the request URI
    URI relativeUri = baseUri.relativize(reqUri);

    @SuppressWarnings("resource")
    URI nessieApiBaseUri =
        nessieApi
            .unwrapClient(org.projectnessie.client.http.HttpClient.class)
            .orElseThrow(() -> new IllegalStateException("No Nessie HTTP client"))
            .getBaseUri();

    // Ensure that the resolved URI is not used to abuse this proxy for anything else than Nessie
    // Core REST API calls. Otherwise, it could be used to issue generic requests to other resources
    // on the same network.
    validateRelativeUri(relativeUri);

    // Resolve the relative request URI against the Nessie Core server base URI. This gives us the
    // properly encoded path and query parameters.
    return nessieApiBaseUri.resolve(relativeUri);
  }

  protected void validateRelativeUri(URI relativeUri) {
    if (relativeUri.isAbsolute()) {
      // Status message isn't always propagated into the result
      throw new ClientErrorException(Response.Status.BAD_REQUEST);
    }
  }

  protected void requestHeaders(BiConsumer<String, String> headerConsumer) {
    for (String hdr : REQUEST_HEADERS) {
      String value = httpHeaders.getHeaderString(hdr);
      if (value != null) {
        headerConsumer.accept(hdr, value);
      }
    }
  }

  protected void responseHeaders(
      Function<String, List<String>> headerProvider, BiConsumer<String, String> headerConsumer) {
    for (String hdr : RESPONSE_HEADERS) {
      List<String> value = headerProvider.apply(hdr);
      if (value != null) {
        value.forEach(v -> headerConsumer.accept(hdr, v));
      }
    }
  }
}
