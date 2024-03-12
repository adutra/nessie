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
package org.projectnessie.catalog.service.rest;

import static java.util.Collections.emptyList;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergError.icebergError;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorResponse.icebergErrorResponse;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergOAuthTokenEndpointException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergRestBaseException;
import org.projectnessie.services.config.ExceptionConfig;

@Provider
@ApplicationScoped
public class IcebergExceptionMapper implements ExceptionMapper<IcebergRestBaseException> {
  @Inject ExceptionConfig exceptionConfig;

  // TODO unify exception handling with Nessie exception handling, map and throw the _right_
  //  exception depending on the endpoint, so Iceberg exceptions for Iceberg-REST endpoints and
  //  Nessie exceptions for Nessie endpoints. For example, IcebergConflictExceptions thrown by
  //  CatalogTransportResource.commit() need to become NessieConflictExceptions.
  //  Hint: can `@Inject UriInfo;` here.

  @Override
  public Response toResponse(IcebergRestBaseException ex) {
    if (ex instanceof IcebergOAuthTokenEndpointException) {
      IcebergOAuthTokenEndpointException e = (IcebergOAuthTokenEndpointException) ex;
      return authEndpointErrorResponse(e);
    }

    List<String> stack;
    if (exceptionConfig.sendStacktraceToClient()) {
      stack =
          Arrays.stream(ex.getStackTrace())
              .map(StackTraceElement::toString)
              .collect(Collectors.toList());
    } else {
      stack = emptyList();
    }

    return genericErrorResponse(ex.getResponseCode(), ex.getType(), ex.getMessage(), stack);
  }

  private static Response genericErrorResponse(
      int code, String type, String message, List<String> stack) {
    return Response.status(code)
        .entity(icebergErrorResponse(icebergError(code, type, message, stack)))
        .build();
  }

  private static Response authEndpointErrorResponse(IcebergOAuthTokenEndpointException e) {
    return Response.status(e.getResponseCode()).entity(e.getDetails()).build();
  }
}
