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
package org.projectnessie.catalog.service.impl;

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent.fromNessieFileContentType;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessiePartitionDefinitionToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieSchemaToIcebergSchema;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.file.SeekableInput;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroupEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class CatalogServiceImpl implements CatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

  private final ObjectIO objectIO;
  private final NessieApiV2 nessieApi;
  private final Persist persist;
  private final TasksService tasksService;
  private final Executor executor;

  @SuppressWarnings("unused")
  public CatalogServiceImpl() {
    this(null, null, null, null, null);
  }

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public CatalogServiceImpl(
      ObjectIO objectIO,
      NessieApiV2 nessieApi,
      Persist persist,
      TasksService tasksService,
      @Named("import-jobs") Executor executor) {
    this.objectIO = objectIO;
    this.nessieApi = nessieApi;
    this.persist = persist;
    this.tasksService = tasksService;
    this.executor = executor;
  }

  @Override
  public CompletionStage<SnapshotResponse> retrieveTableSnapshot(
      String ref,
      ContentKey key,
      Optional<NessieId> manifestFileId,
      SnapshotFormat format,
      OptionalInt specVersion,
      CatalogUriResolver catalogUriResolver)
      throws NessieNotFoundException {

    ParsedReference reference = parseRefPathString(ref);

    // TODO remove this log information / move to "trace" / remove sensitive information
    LOGGER.info(
        "retrieveTableSnapshot ref-name:{} ref-hash:{} key:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        key);

    ContentResponse contentResponse =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
            .getSingle(key);
    Content content = contentResponse.getContent();

    ObjId snapshotId = snapshotIdFromContent(content);

    // TODO only retrieve objects that are required. For example:
    //  Manifest-list-entries are not required when returning an Iceberg table-metadata JSON.
    //  Other parts are not required when returning only an Iceberg manifest-list.
    CompletionStage<NessieTableSnapshot> snapshotStage;
    try {
      snapshotStage =
          new IcebergStuff(objectIO, persist, tasksService, executor)
              .retrieveIcebergSnapshot(snapshotId, content, format);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return snapshotStage.thenApply(
        snapshot -> {
          Object result;
          String fileName;

          switch (format) {
            case NESSIE_SNAPSHOT:
              fileName =
                  String.join("/", key.getElements())
                      + '_'
                      + snapshot.snapshotId().idAsString()
                      + ".nessie-metadata.json";
              result = snapshot;
              break;
            case NESSIE_SNAPSHOT_NO_MANIFESTS:
              fileName =
                  String.join("/", key.getElements())
                      + '_'
                      + snapshot.snapshotId().idAsString()
                      + ".nessie-metadata-no-manifests.json";
              result = snapshot;
              break;
            case ICEBERG_TABLE_METADATA:
            case ICEBERG_TABLE_METADATA_IMPORTED:
              // Return the snapshot as an Iceberg table-metadata using either the spec-version
              // given in
              // the request or the one used when the table-metadata was written.
              // TODO Does requesting a table-metadata using another spec-version make any sense?
              // TODO Response should respect the JsonView / spec-version
              // TODO Add a check that the original table format was Iceberg (not Delta)
              result =
                  nessieTableSnapshotToIceberg(
                      snapshot,
                      optionalIcebergSpec(specVersion),
                      new NessieModelIceberg.IcebergSnapshotTweak() {
                        @Override
                        public String resolveManifestListLocation(String original) {
                          return format.asImported()
                              ? snapshot.icebergManifestListLocation()
                              : catalogUriResolver.icebergManifestList(snapshot).toString();
                        }

                        @Override
                        public List<String> resolveManifestFilesLocations(List<String> original) {
                          Stream<NessieFileManifestGroupEntry> stream =
                              snapshot.fileManifestGroup().manifests().stream();
                          Stream<String> next = stream.map(e -> e.icebergManifestPath());
                          // TODO the following would "tweak" the manifest-list entries (locations
                          //  to manifest-files) to point to Nessie Catalog.
                          //                        format.useOriginalPaths()
                          //                            ? stream.map(e -> e.icebergManifestPath())
                          //                            : stream.map(
                          //                                e ->
                          //   catalogUriResolver.icebergManifestFile(e.id()).toString());
                          return next.collect(Collectors.toList());
                        }
                      });
              fileName = "00000-" + snapshotId + ".metadata.json";
              break;
            case ICEBERG_MANIFEST_LIST:
            case ICEBERG_MANIFEST_LIST_IMPORTED:
              return produceIcebergManifestList(
                  snapshot,
                  optionalIcebergSpec(specVersion),
                  manifestListFileName(snapshot),
                  (manifest) ->
                      format.asImported()
                          ? manifest.icebergManifestPath()
                          : catalogUriResolver
                              .icebergManifestFile(
                                  NessieFileManifestEntry.id(manifest.icebergManifestPath()))
                              .toString());
            case ICEBERG_MANIFEST_FILE:
              return produceIcebergManifestFile(
                  snapshot,
                  manifestFileId.orElseThrow(),
                  optionalIcebergSpec(specVersion),
                  catalogUriResolver);
            default:
              throw new IllegalArgumentException("Unknown format " + format);
          }

          return SnapshotResponse.forEntity(result, fileName, "application/json");
        });
  }

  // TODO copied from RestV2TreeResource
  private ParsedReference parseRefPathString(String refPathString) {
    return resolveReferencePathElement(refPathString, Reference.ReferenceType.BRANCH, () -> "-");
  }

  private static Optional<IcebergSpec> optionalIcebergSpec(OptionalInt specVersion) {
    return specVersion.isPresent()
        ? Optional.of(IcebergSpec.forVersion(specVersion.getAsInt()))
        : Optional.empty();
  }

  private static String manifestListFileName(NessieTableSnapshot snapshot) {
    return "snap-"
        + snapshot.icebergSnapshotId()
        + "-1-"
        + snapshot.snapshotId().idAsString()
        + ".avro";
  }

  private static String manifestFileName(NessieId manifestFileId) {
    return manifestFileId.idAsString() + "-m0.avro";
  }

  private SnapshotResponse produceIcebergManifestFile(
      NessieTableSnapshot snapshot,
      NessieId manifestFileId,
      Optional<IcebergSpec> icebergSpec,
      CatalogUriResolver catalogUriResolver) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public String contentType() {
        return "avro/binary+linear";
      }

      @Override
      public String fileName() {
        return manifestFileName(manifestFileId);
      }

      @Override
      public void produce(OutputStream outputStream) throws IOException {
        NessieSchema schema =
            snapshot.schemas().stream()
                .filter(s -> s.id().equals(snapshot.currentSchema()))
                .findFirst()
                .orElseThrow();
        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitions().stream()
                .filter(p -> p.id().equals(snapshot.currentPartitionDefinition()))
                .findFirst()
                .orElseThrow();

        NessieFileManifestGroupEntry nessieFileManifestGroupEntry =
            snapshot.fileManifestGroup().manifests().stream()
                .filter(g -> g.manifestId().equals(manifestFileId))
                .findFirst()
                .orElseThrow(
                    () ->
                        new RuntimeException(
                            "No NessieFileManifestGroupEntry with ID " + manifestFileId));

        LOGGER.info(
            "Producing manifest file for manifest-list-entry {} for snapshot {}/{}, schema {}/{}, partition-definition {}/{}",
            nessieFileManifestGroupEntry.manifestId().idAsString(),
            snapshot.icebergSnapshotId(),
            snapshot.snapshotId().idAsString(),
            schema.icebergId(),
            schema.id().idAsString(),
            partitionDefinition.icebergId(),
            partitionDefinition.id().idAsString());

        IcebergSchema icebergSchema = nessieSchemaToIcebergSchema(schema);
        IcebergPartitionSpec partitionSpec =
            nessiePartitionDefinitionToIceberg(partitionDefinition);

        try (IcebergManifestFileWriter.IcebergManifestFileEntryWriter writer =
            IcebergManifestFileWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                .schema(icebergSchema)
                .partitionSpec(partitionSpec)
                .addedSnapshotId(nessieFileManifestGroupEntry.addedSnapshotId())
                .sequenceNumber(nessieFileManifestGroupEntry.sequenceNumber())
                .minSequenceNumber(nessieFileManifestGroupEntry.minSequenceNumber())
                .tableProperties(snapshot.properties())
                .content(fromNessieFileContentType(nessieFileManifestGroupEntry.content()))
                .output(outputStream)
                .manifestPath(nessieFileManifestGroupEntry.icebergManifestPath())
                .build()
                .entryWriter()) {

          URI manifestUri = URI.create(nessieFileManifestGroupEntry.icebergManifestPath());
          try (SeekableInput avroFileInput =
                  new SeekableStreamInput(manifestUri, objectIO::readObject);
              IcebergManifestFileReader.IcebergManifestFileEntryReader reader =
                  IcebergManifestFileReader.builder().build().entryReader(avroFileInput)) {
            while (reader.hasNext()) {
              IcebergManifestEntry entry = reader.next();
              NessieDataFileFormat fileFormat =
                  entry.dataFile().fileFormat().nessieDataFileFormat();
              String updatedDataFileLocation =
                  catalogUriResolver.dataFile(fileFormat, entry.dataFile().filePath()).toString();
              IcebergManifestEntry updatedEntry =
                  IcebergManifestEntry.builder()
                      .from(entry)
                      .dataFile(
                          IcebergDataFile.builder()
                              .from(entry.dataFile())
                              .filePath(updatedDataFileLocation)
                              .build())
                      .build();
              writer.append(updatedEntry);
            }
          }

          writer.finish();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private SnapshotResponse produceIcebergManifestList(
      NessieTableSnapshot snapshot,
      Optional<IcebergSpec> icebergSpec,
      String fileName,
      Function<NessieFileManifestGroupEntry, String> tweakManifestPath) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public String fileName() {
        return fileName;
      }

      @Override
      public String contentType() {
        return "avro/binary+linear";
      }

      @Override
      public void produce(OutputStream outputStream) throws IOException {
        NessieSchema schema =
            snapshot.schemas().stream()
                .filter(s -> s.id().equals(snapshot.currentSchema()))
                .findFirst()
                .orElseThrow();
        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitions().stream()
                .filter(p -> p.id().equals(snapshot.currentPartitionDefinition()))
                .findFirst()
                .orElseThrow();

        NessieFileManifestGroup nessieFileManifestGroup = snapshot.fileManifestGroup();

        LOGGER.info(
            "Producing manifest list with {} entries for snapshot {}/{}, schema {}/{}, partition-definition {}/{}",
            nessieFileManifestGroup.manifests().size(),
            snapshot.icebergSnapshotId(),
            snapshot.snapshotId().idAsString(),
            schema.icebergId(),
            schema.id().idAsString(),
            partitionDefinition.icebergId(),
            partitionDefinition.id().idAsString());

        try (IcebergManifestListWriter.IcebergManifestListEntryWriter writer =
            IcebergManifestListWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                // TODO .putTableProperty(AVRO_COMPRESSION, avroCompression())
                .schema(nessieSchemaToIcebergSchema(schema))
                .partitionSpec(nessiePartitionDefinitionToIceberg(partitionDefinition))
                // .parentSnapshotId()  - TODO ??
                .snapshotId(snapshot.icebergSnapshotId())
                .tableProperties(snapshot.properties())
                //                .sequenceNumber(snapshot.icebergSnapshotSequenceNumber())
                .output(outputStream)
                .build()
                .entryWriter()) {

          for (NessieFileManifestGroupEntry manifest : nessieFileManifestGroup.manifests()) {
            IcebergManifestContent content;
            switch (manifest.content()) {
              case ICEBERG_DATA_FILE:
                content = IcebergManifestContent.DATA;
                break;
              case ICEBERG_DELETE_FILE:
                content = IcebergManifestContent.DELETES;
                break;
              default:
                throw new IllegalArgumentException(manifest.content().name());
            }
            List<IcebergPartitionFieldSummary> partitions =
                manifest.partitions().stream()
                    .map(
                        p ->
                            IcebergPartitionFieldSummary.builder()
                                .containsNan(p.containsNan())
                                .containsNull(p.containsNull())
                                .lowerBound(p.lowerBound())
                                .upperBound(p.upperBound())
                                .build())
                    .collect(Collectors.toList());

            LOGGER.info("  .. adding manifest file");
            IcebergManifestFile icebergManifestFile =
                IcebergManifestFile.builder()
                    .content(content)
                    //
                    //                    .manifestPath(manifest.icebergManifestPath())
                    // TODO the following would tweak the location of the manifest-file to Nessie
                    //  Catalog.
                    .manifestPath(tweakManifestPath.apply(manifest))
                    //
                    // TODO the length of the manifest file generated by the Nessie Catalog will be
                    //  different from the manifest file length reported by Iceberg.
                    .manifestLength(manifest.icebergManifestLength())
                    //
                    .addedSnapshotId(manifest.addedSnapshotId())
                    .sequenceNumber(manifest.sequenceNumber())
                    .minSequenceNumber(manifest.minSequenceNumber())
                    .partitionSpecId(manifest.partitionSpecId())
                    .partitions(partitions)
                    //
                    .addedDataFilesCount(manifest.addedDataFilesCount())
                    .addedRowsCount(manifest.addedRowsCount())
                    .deletedDataFilesCount(manifest.deletedDataFilesCount())
                    .deletedRowsCount(manifest.deletedRowsCount())
                    .existingDataFilesCount(manifest.existingDataFilesCount())
                    .existingRowsCount(manifest.existingRowsCount())
                    //
                    .keyMetadata(manifest.keyMetadata())
                    //
                    .build();
            writer.append(icebergManifestFile);
          }
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Compute the ID for the given Nessie {@link Content} object. */
  private ObjId snapshotIdFromContent(Content content) throws NessieContentNotFoundException {
    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergTable.getMetadataLocation())
          .hash(icebergTable.getSnapshotId())
          .generate();
    }
    if (content instanceof Namespace) {
      throw new NessieContentNotFoundException(
          ImmutableNessieError.builder()
              .errorCode(ErrorCode.CONTENT_NOT_FOUND)
              .message("No snapshots for Namespace: " + content)
              .reason("Not a table")
              .status(404)
              .build());
    }
    if (content instanceof IcebergView) {
      throw new UnsupportedOperationException("IMPLEMENT ME FOR " + content);
    }
    throw new UnsupportedOperationException("IMPLEMENT ME FOR " + content);
  }
}
