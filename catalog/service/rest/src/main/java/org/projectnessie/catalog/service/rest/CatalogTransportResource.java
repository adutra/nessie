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
package org.projectnessie.catalog.service.rest;

import static org.projectnessie.catalog.service.api.SnapshotReqParams.forSnapshotHttpReq;
import static org.projectnessie.catalog.service.rest.ExternalBaseUri.parseRefPathString;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.jboss.resteasy.reactive.RestMulti;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.sign.SigningRequest;
import org.projectnessie.catalog.api.sign.SigningResponse;
import org.projectnessie.catalog.api.types.CatalogCommit;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Path("catalog/v1")
public class CatalogTransportResource extends AbstractCatalogResource {

  @Inject RequestSigner signer;

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshots")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Multi<Object> tableSnapshots(
      @PathParam("ref") String ref,
      @QueryParam("key") List<ContentKey> keys,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotReqParams reqParams = forSnapshotHttpReq(parseRefPathString(ref), format, specVersion);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

    AtomicReference<Reference> effectiveReference = new AtomicReference<>();

    // The order of the returned items does not necessarily match the order of the requested items,
    // Nessie's getContents() does neither.

    // This operation can block --> @Blocking
    Stream<Supplier<CompletionStage<SnapshotResponse>>> snapshots =
        catalogService.retrieveSnapshots(
            reqParams, keys, catalogUriResolver, effectiveReference::set);

    Multi<Object> multi =
        Multi.createFrom()
            .items(snapshots)
            .capDemandsTo(2)
            .map(Multi.createFrom()::completionStage)
            .flatMap(m -> m)
            .map(SnapshotResponse::entityObject)
            .flatMap(Multi.createFrom()::optional);

    // TODO This implementation just returns a "bare" array built from the `Multi`. It would be much
    //  nicer to return a wrapping object, or at least a trailing object with additional information
    //  like the effective reference for the Nessie Catalog response format.
    //  See https://github.com/orgs/resteasy/discussions/4032

    RestMulti.SyncRestMulti.Builder<Object> restMulti = RestMulti.fromMultiData(multi);
    nessieResponseHeaders(effectiveReference.get(), restMulti::header);
    return restMulti.build();
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshot/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> tableSnapshot(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    return snapshotBased(
        key, forSnapshotHttpReq(parseRefPathString(ref), format, specVersion), ICEBERG_TABLE);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-list/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> manifestList(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    return snapshotBased(
        key,
        SnapshotReqParams.forManifestListHttpReq(parseRefPathString(ref), format, specVersion),
        ICEBERG_TABLE);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> manifestFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion,
      @QueryParam("manifest-file") String manifestFile)
      throws NessieNotFoundException {
    return snapshotBased(
        key,
        SnapshotReqParams.forManifestFileHttpReq(
            parseRefPathString(ref), format, specVersion, manifestFile),
        ICEBERG_TABLE);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/data-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> dataFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("type") String fileType,
      @QueryParam("token") String fileToken,
      @QueryParam("file") String dataFile)
      throws NessieNotFoundException {
    return snapshotResponse(
            key,
            SnapshotReqParams.forDataFile(parseRefPathString(ref), SnapshotFormat.NESSIE_SNAPSHOT),
            ICEBERG_TABLE)
        .map(
            snapshotResponse -> {
              NessieTableSnapshot tableSnapshot =
                  snapshotResponse
                      .entityObject()
                      .map(NessieTableSnapshot.class::cast)
                      .orElseThrow();
              // TODO will need table-snapshot later for AuthZ

              // TODO need the effective Nessie reference incl commit-ID here, add as a HTTP
              //  response header?

              String dataFileName = Paths.get(dataFile).getFileName().toString();

              NessieDataFileFormat fileFormat;
              if (fileType != null) {
                fileFormat = NessieDataFileFormat.valueOf(fileType.toUpperCase(Locale.ROOT));
              } else {
                fileFormat = NessieDataFileFormat.forFileName(dataFile);
              }
              String contentType =
                  fileFormat != null ? fileFormat.contentType() : "application/octet-stream";

              // TODO need to validate that 'dataFile' belongs to the table -> AuthZ !!
              //  Need some token, that's signed using the Nessie reference, content-key,
              //  signed using a private key, with it's public key published "somewhere".
              //  These key-pairs need IDs, so we can have multiple key-pairs.
              //  In other words: need a way to prove that 'dataFile' belongs to the table,
              //  then we can rely on table AuthZ.
              //  Since the dataFile-token itself only verifies that 'dataFile' belongs
              //  to some table, it is not a "private thing" and can be shared w/ others.
              //  AuthN+table-level-AuthZ is still required to access the file.
              //  Technically:
              //     var hash = hash ( snapshot-id , file-path )
              //     var signature = sign ( hash , key-pair )
              //     var token = signature + key-pair-name
              StreamingOutput producer =
                  outputStream -> {
                    try (InputStream in = objectIO.readObject(URI.create(dataFile))) {
                      in.transferTo(outputStream);
                    }
                  };

              return Response.ok(producer)
                  .header("Content-Disposition", "attachment; filename=\"" + dataFileName + "\"")
                  .header("Content-Type", contentType)
                  .build();
            });
  }

  @POST
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/commit")
  @Blocking
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> commit(
      @PathParam("ref") String ref,
      @RequestBody CatalogCommit commit,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException, NessieConflictException {

    ParsedReference reference = parseRefPathString(ref);

    SnapshotReqParams reqParams = forSnapshotHttpReq(reference, format, specVersion);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo, reqParams.snapshotFormat());

    return Uni.createFrom()
        .completionStage(catalogService.commit(reference, commit, reqParams, catalogUriResolver))
        .map(v -> Response.ok().build());
  }

  @POST
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/sign/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public SigningResponse signRequest(
      @PathParam("ref") String ref, @PathParam("key") ContentKey key, SigningRequest request)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref);
    // TODO access check
    return signer.sign(reference.name(), key.toPathString(), request);
  }
}
