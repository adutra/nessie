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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergError;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergErrorResponse;

@Provider
@ApplicationScoped
public class IcebergErrorExceptionMapper implements ExceptionMapper<IcebergErrorException> {
  @Override
  public Response toResponse(IcebergErrorException ex) {
    IcebergErrorResponse errorResponse = ex.errorResponse();
    IcebergError error = errorResponse.error();
    return Response.status(error.code()).entity(errorResponse).build();
  }
}
