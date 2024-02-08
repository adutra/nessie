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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
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
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import org.projectnessie.catalog.api.rest.spec.NessieCatalogServiceBase;
import org.projectnessie.catalog.api.rest.spec.SnapshotResultFormat;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

@RequestScoped
@Consumes(MediaType.APPLICATION_JSON)
@Path("catalog/v1")
public class CatalogTransportResource implements NessieCatalogServiceBase<Uni<Response>> {

  private final CatalogService catalogService;
  private final ObjectIO objectIO;

  @Context UriInfo uriInfo;

  @SuppressWarnings("unused")
  public CatalogTransportResource() {
    this(null, null);
  }

  @Inject
  public CatalogTransportResource(CatalogService catalogService, ObjectIO objectIO) {
    this.catalogService = catalogService;
    this.objectIO = objectIO;
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/snapshot/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  @Override
  public Uni<Response> tableSnapshot(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    OptionalInt reqVersion = OptionalInt.empty();

    if (format == null) {
      // No table format specified, return the NessieTableSnapshot as JSON
      snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT;
    } else {
      format = format.toUpperCase(Locale.ROOT);
      SnapshotResultFormat tableFormat = SnapshotResultFormat.valueOf(format);
      switch (tableFormat) {
        case ICEBERG:
          // Return the snapshot as an Iceberg table-metadata using either the spec-version given
          // in
          // the request or the one used when the table-metadata was written.
          // TODO Does requesting a table-metadata using another spec-version make any sense?
          // TODO Response should respect the JsonView / spec-version
          // TODO Add a check that the original table format was Iceberg (not Delta)
          snapshotFormat = SnapshotFormat.ICEBERG_TABLE_METADATA;
          if (specVersion != null) {
            reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
          }
          break;
        case ICEBERG_IMPORTED:
          // Return the snapshot as an Iceberg table-metadata using either the spec-version given
          // in
          // the request or the one used when the table-metadata was written.
          // TODO Does requesting a table-metadata using another spec-version make any sense?
          // TODO Response should respect the JsonView / spec-version
          // TODO Add a check that the original table format was Iceberg (not Delta)
          snapshotFormat = SnapshotFormat.ICEBERG_TABLE_METADATA_IMPORTED;
          if (specVersion != null) {
            reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
          }
          break;
        case NESSIE:
          snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT;
          break;
        case NESSIE_NO_MANIFEST:
          snapshotFormat = SnapshotFormat.NESSIE_SNAPSHOT_NO_MANIFESTS;
          break;
        case DELTA_LAKE:
        default:
          throw new UnsupportedOperationException();
      }
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.empty(), reqVersion);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-list/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Override
  public Uni<Response> manifestList(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    OptionalInt reqVersion = OptionalInt.empty();

    TableFormat tableFormat =
        format != null ? TableFormat.valueOf(format.toUpperCase(Locale.ROOT)) : TableFormat.ICEBERG;

    switch (tableFormat) {
      case ICEBERG:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        snapshotFormat = SnapshotFormat.ICEBERG_MANIFEST_LIST;
        if (specVersion != null) {
          reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
        }
        break;
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.empty(), reqVersion);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/manifest-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Override
  public Uni<Response> manifestFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("manifest-file") String manifestFile,
      @QueryParam("format") String format,
      @QueryParam("specVersion") String specVersion)
      throws NessieNotFoundException {
    SnapshotFormat snapshotFormat;
    NessieId manifestFileId;
    OptionalInt reqVersion = OptionalInt.empty();

    TableFormat tableFormat =
        format != null ? TableFormat.valueOf(format.toUpperCase(Locale.ROOT)) : TableFormat.ICEBERG;

    switch (tableFormat) {
      case ICEBERG:
        // Return the snapshot as an Iceberg table-metadata using either the spec-version given in
        // the request or the one used when the table-metadata was written.
        // TODO Does requesting a table-metadata using another spec-version make any sense?
        // TODO Response should respect the JsonView / spec-version
        // TODO Add a check that the original table format was Iceberg (not Delta)
        manifestFileId = NessieId.nessieIdFromStringBase64(manifestFile);
        snapshotFormat = SnapshotFormat.ICEBERG_MANIFEST_FILE;
        if (specVersion != null) {
          reqVersion = OptionalInt.of(Integer.parseInt(specVersion));
        }
        break;
      case DELTA_LAKE:
      default:
        throw new UnsupportedOperationException();
    }

    return snapshotBased(ref, key, snapshotFormat, Optional.of(manifestFileId), reqVersion);
  }

  @GET
  @Path("trees/{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/data-file/{key}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Override
  public Uni<Response> dataFile(
      @PathParam("ref") String ref,
      @PathParam("key") ContentKey key,
      @QueryParam("file") String dataFile,
      @QueryParam("type") String fileType,
      @QueryParam("token") String fileToken)
      throws NessieNotFoundException {
    CompletionStage<Response> cs =
        snapshotResponse(
                ref, key, SnapshotFormat.NESSIE_SNAPSHOT, Optional.empty(), OptionalInt.empty())
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
                      .header(
                          "Content-Disposition", "attachment; filename=\"" + dataFileName + "\"")
                      .header("Content-Type", contentType)
                      .build();
                });

    return Uni.createFrom().completionStage(cs);
  }

  private Uni<Response> snapshotBased(
      String ref,
      ContentKey key,
      SnapshotFormat snapshotFormat,
      Optional<NessieId> manifestFileId,
      OptionalInt reqVersion)
      throws NessieNotFoundException {
    CompletionStage<SnapshotResponse> snapshotResponse =
        snapshotResponse(ref, key, snapshotFormat, manifestFileId, reqVersion);
    return Uni.createFrom()
        .completionStage(snapshotResponse)
        .map(CatalogTransportResource::snapshotToResponse);
  }

  private CompletionStage<SnapshotResponse> snapshotResponse(
      String ref,
      ContentKey key,
      SnapshotFormat snapshotFormat,
      Optional<NessieId> manifestFileId,
      OptionalInt reqVersion)
      throws NessieNotFoundException {
    // Remove content key and query parameters from the URI. For example, the request URI
    //   http://127.0.0.1:19110/catalog/v1/trees/main/snapshot/testing.city?format=iceberg
    // becomes
    //   http://127.0.0.1:19110/catalog/v1/trees/main/
    // which is then resolved to the URIs for manifest lists and manifest files.
    CatalogService.CatalogUriResolver catalogUriResolver =
        new CatalogService.CatalogUriResolver() {
          // TODO this baseUri must include the commit-ID
          final URI baseUri = uriInfo.getRequestUri().resolve("..");
          final String keyPathString = key.toPathString();

          @Override
          public URI icebergManifestList(NessieTableSnapshot snapshot) {
            return snapshotFormat.asImported()
                ? URI.create(snapshot.icebergManifestListLocation())
                : baseUri.resolve("manifest-list/" + keyPathString);
          }

          @Override
          public URI icebergManifestFile(NessieId manifestFileId) {
            return baseUri.resolve(
                "manifest-file/"
                    + keyPathString
                    + "?manifest-file="
                    + URLEncoder.encode(manifestFileId.idAsBase64(), UTF_8));
          }

          @Override
          public URI dataFile(NessieDataFileFormat fileFormat, String dataFile) {
            // TODO generate dataFile token
            String fileToken = "tokenToVerifyThatTheDataFileBelongsToTheTable";
            String fileFormatParam =
                dataFile.endsWith(fileFormat.fileExtension())
                    ? ""
                    : "&format=" + URLEncoder.encode(fileFormat.name(), UTF_8);
            return baseUri.resolve(
                "data-file/"
                    + keyPathString
                    + "?file="
                    + URLEncoder.encode(dataFile, UTF_8)
                    + fileFormatParam
                    + "&token="
                    + URLEncoder.encode(fileToken, UTF_8));
          }
        };

    return catalogService.retrieveTableSnapshot(
        ref, key, manifestFileId, snapshotFormat, reqVersion, catalogUriResolver);
  }

  private static Response snapshotToResponse(SnapshotResponse snapshot) {
    // TODO For REST return an ETag header + cache-relevant fields (consider Nessie commit
    //  ID and state of the manifest-list/files to reflect "in-place" changes, like
    //  compaction/optimization)

    // TODO need the effective Nessie reference incl commit-ID here, add as a HTTP response header?

    Optional<Object> entity = snapshot.entityObject();
    if (entity.isPresent()) {
      return Response.ok(entity.get())
          .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
          .header("Content-Type", snapshot.contentType())
          .build();
    }

    // TODO do we need a BufferedOutputStream via StreamingOutput.write ?
    return Response.ok((StreamingOutput) snapshot::produce)
        .header("Content-Disposition", "attachment; filename=\"" + snapshot.fileName() + "\"")
        .header("Content-Type", snapshot.contentType())
        .build();
  }
}
