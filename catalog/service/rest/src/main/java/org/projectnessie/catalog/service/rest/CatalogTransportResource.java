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

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.jboss.resteasy.reactive.RestMulti;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.base.transport.CatalogCommit;
import org.projectnessie.catalog.api.rest.spec.NessieCatalogServiceBase;
import org.projectnessie.catalog.api.sign.SigningRequest;
import org.projectnessie.catalog.api.sign.SigningResponse;
import org.projectnessie.catalog.files.api.ObjectIO;
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
public class CatalogTransportResource
    implements NessieCatalogServiceBase<CompletionStage<Response>, Multi<Object>> {

  private final CatalogService catalogService;
  private final ObjectIO objectIO;
  private final RequestSigner signer;

  @Context UriInfo uriInfo;

  @SuppressWarnings("unused")
  public CatalogTransportResource() {
    this(null, null, null);
  }

  @Inject
  public CatalogTransportResource(
      CatalogService catalogService, ObjectIO objectIO, RequestSigner signer) {
    this.catalogService = catalogService;
    this.objectIO = objectIO;
    this.signer = signer;
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshots")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  @Override
  public Multi<Object> tableSnapshots(
      @PathParam("ref") String ref,
      @QueryParam("key") List<ContentKey> keys,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotReqParams reqParams = SnapshotReqParams.forSnapshotHttpReq(ref, format, specVersion);

    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo.getRequestUri(), reqParams.snapshotFormat());

    AtomicReference<Reference> effectiveReference = new AtomicReference<>();

    // The order of the returned items does not necessarily match the order of the requested items,
    // Nessie's getContents() does neither.

    // This operation can block --> @Blocking
    Stream<Supplier<CompletionStage<SnapshotResponse>>> snapshots =
        catalogService.retrieveTableSnapshots(
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
  @Override
  public CompletionStage<Response> tableSnapshot(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    return snapshotBased(key, SnapshotReqParams.forSnapshotHttpReq(ref, format, specVersion));
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-list/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  @Override
  public CompletionStage<Response> manifestList(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    return snapshotBased(key, SnapshotReqParams.forManifestListHttpReq(ref, format, specVersion));
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  @Override
  public CompletionStage<Response> manifestFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion,
      @QueryParam("manifest-file") String manifestFile)
      throws NessieNotFoundException {
    return snapshotBased(
        key, SnapshotReqParams.forManifestFileHttpReq(ref, format, specVersion, manifestFile));
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/data-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  @Override
  public CompletionStage<Response> dataFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("type") String fileType,
      @QueryParam("token") String fileToken,
      @QueryParam("file") String dataFile)
      throws NessieNotFoundException {
    return snapshotResponse(key, SnapshotReqParams.forDataFile(ref, SnapshotFormat.NESSIE_SNAPSHOT))
        .thenApply(
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

  private CompletionStage<Response> snapshotBased(
      ContentKey key, SnapshotReqParams snapshotReqParams) throws NessieNotFoundException {
    return snapshotResponse(key, snapshotReqParams)
        .thenApply(CatalogTransportResource::snapshotToResponse);
  }

  private CompletionStage<SnapshotResponse> snapshotResponse(
      ContentKey key, SnapshotReqParams snapshotReqParams) throws NessieNotFoundException {
    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogUriResolverImpl(uriInfo.getRequestUri(), snapshotReqParams.snapshotFormat());

    return catalogService.retrieveTableSnapshot(snapshotReqParams, key, catalogUriResolver);
  }

  private static Response snapshotToResponse(SnapshotResponse snapshot) {
    // TODO For REST return an ETag header + cache-relevant fields (consider Nessie commit
    //  ID and state of the manifest-list/files to reflect "in-place" changes, like
    //  compaction/optimization)

    // TODO need the effective Nessie reference incl commit-ID here, add as a HTTP response header?

    Optional<Object> entity = snapshot.entityObject();
    if (entity.isPresent()) {
      return finalizeResponse(Response.ok(entity.get()), snapshot);
    }

    // TODO do we need a BufferedOutputStream via StreamingOutput.write ?
    return finalizeResponse(Response.ok((StreamingOutput) snapshot::produce), snapshot);
  }

  private static Response finalizeResponse(
      Response.ResponseBuilder response, SnapshotResponse snapshot) {
    response
        .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
        .header("Content-Type", snapshot.contentType());
    nessieResponseHeaders(snapshot.effectiveReference(), response::header);
    return response.build();
  }

  private static void nessieResponseHeaders(
      Reference reference, BiConsumer<String, String> header) {
    header.accept("Nessie-Reference", URLEncoder.encode(reference.toPathString()));
  }

  @POST
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/commit")
  @Blocking
  @Override
  @Produces(MediaType.APPLICATION_JSON)
  public CompletionStage<Response> commit(
      @PathParam("ref") String ref, @RequestBody CatalogCommit commit)
      throws NessieNotFoundException, NessieConflictException {

    return catalogService.commit(ref, commit).thenApply(v -> Response.ok().build());
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

  private ParsedReference parseRefPathString(String refPathString) {
    return resolveReferencePathElement(
        refPathString,
        Reference.ReferenceType.BRANCH,
        () -> {
          throw new IllegalArgumentException("ref path must specify a branch");
        });
  }
}
