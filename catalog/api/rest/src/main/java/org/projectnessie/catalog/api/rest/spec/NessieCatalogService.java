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
package org.projectnessie.catalog.api.rest.spec;

import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

@Consumes(MediaType.APPLICATION_JSON)
@jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
@Path("catalog/v1")
@jakarta.ws.rs.Path("catalog/v1")
@Tag(name = "catalog-v1")
public interface NessieCatalogService {
  @GET
  @jakarta.ws.rs.GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshot/{key}")
  @jakarta.ws.rs.Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshot/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  Object tableSnapshot(
      @PathParam("ref") @jakarta.ws.rs.PathParam("ref") String ref,
      @PathParam("key") @jakarta.ws.rs.PathParam("key") ContentKey key,
      @QueryParam("format") @jakarta.ws.rs.QueryParam("format") String format,
      @QueryParam("specVersion") @jakarta.ws.rs.QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException;
}
