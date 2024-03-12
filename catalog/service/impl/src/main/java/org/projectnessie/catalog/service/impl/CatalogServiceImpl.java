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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent.fromNessieFileContentType;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_SEQUENCE_NUMBER;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergMetadataToContent;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieGroupEntryToIcebergManifestFile;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessiePartitionDefinitionToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieSchemaToIcebergSchema;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieTableSnapshotToIceberg;
import static org.projectnessie.catalog.model.api.NessieSnapshotResponse.nessieSnapshotResponse;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.avro.file.SeekableInput;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.api.base.transport.CatalogCommit;
import org.projectnessie.catalog.api.base.transport.CatalogOperation;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.IcebergSnapshotTweak;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConflictException;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.manifest.NessieDataFileFormat;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroupEntry;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.catalog.service.api.SnapshotFormat;
import org.projectnessie.catalog.service.api.SnapshotReqParams;
import org.projectnessie.catalog.service.api.SnapshotResponse;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
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
  public Stream<Supplier<CompletionStage<SnapshotResponse>>> retrieveTableSnapshots(
      SnapshotReqParams reqParams,
      List<ContentKey> keys,
      CatalogUriResolver catalogUriResolver,
      Consumer<Reference> effectiveReferenceConsumer)
      throws NessieNotFoundException {
    ParsedReference reference = reqParams.ref();

    // TODO remove this log information / move to "trace" / remove sensitive information
    LOGGER.info(
        "retrieveTableSnapshots ref-name:{} ref-hash:{} keys:{}",
        reference.name(),
        reference.hashWithRelativeSpec(),
        keys);

    GetMultipleContentsResponse contentResponse =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec())
            .keys(keys)
            .getWithResponse();

    IcebergStuff icebergStuff = new IcebergStuff(objectIO, persist, tasksService, executor);

    Reference effectiveReference = contentResponse.getEffectiveReference();
    effectiveReferenceConsumer.accept(effectiveReference);
    return contentResponse.getContents().stream()
        .map(
            c -> {
              ObjId snapshotId;
              try {
                snapshotId = snapshotIdFromContent(c.getContent());
              } catch (Exception e) {
                e.printStackTrace();
                return null;
              }
              return (Supplier<CompletionStage<SnapshotResponse>>)
                  () -> {
                    ContentKey key = c.getKey();
                    // TODO remove this log information / move to "trace" / remove sensitive
                    // information
                    LOGGER.info(
                        "retrieveTableSnapshots - individual ref-name:{} ref-hash:{} key:{}",
                        reference.name(),
                        reference.hashWithRelativeSpec(),
                        key);
                    CompletionStage<NessieTableSnapshot> snapshotStage =
                        icebergStuff.retrieveIcebergSnapshot(
                            snapshotId, c.getContent(), reqParams.snapshotFormat());
                    return snapshotResponseCompletionStage(
                        key,
                        reqParams,
                        catalogUriResolver,
                        snapshotStage,
                        effectiveReference,
                        snapshotId);
                  };
            })
        .filter(Objects::nonNull);
  }

  @Override
  public CompletionStage<SnapshotResponse> retrieveTableSnapshot(
      SnapshotReqParams reqParams, ContentKey key, CatalogUriResolver catalogUriResolver)
      throws NessieNotFoundException {

    ParsedReference reference = reqParams.ref();

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
    Reference effectiveReference = contentResponse.getEffectiveReference();

    ObjId snapshotId = snapshotIdFromContent(content);

    CompletionStage<NessieTableSnapshot> snapshotStage =
        new IcebergStuff(objectIO, persist, tasksService, executor)
            .retrieveIcebergSnapshot(snapshotId, content, reqParams.snapshotFormat());

    return snapshotResponseCompletionStage(
        key, reqParams, catalogUriResolver, snapshotStage, effectiveReference, snapshotId);
  }

  private CompletionStage<SnapshotResponse> snapshotResponseCompletionStage(
      ContentKey key,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver,
      CompletionStage<NessieTableSnapshot> snapshotStage,
      Reference effectiveReference,
      ObjId snapshotId) {
    return snapshotStage.thenApply(
        snapshot -> {
          Object result;
          String fileName;

          Consumer<Map<String, String>> tablePropertiesTweak =
              properties -> {
                properties.put("nessie.catalog.content-id", snapshot.entity().nessieContentId());
                properties.put("nessie.catalog.snapshot-id", snapshot.id().idAsString());
                properties.put("nessie.commit.id", effectiveReference.getHash());
                properties.put("nessie.commit.ref", effectiveReference.toPathString());
              };

          switch (reqParams.snapshotFormat()) {
            case NESSIE_SNAPSHOT:
              fileName =
                  String.join("/", key.getElements())
                      + '_'
                      + snapshot.id().idAsString()
                      + ".nessie-metadata.json";
              result = nessieSnapshotResponse(effectiveReference, snapshot);
              break;
            case NESSIE_SNAPSHOT_NO_MANIFESTS:
              fileName =
                  String.join("/", key.getElements())
                      + '_'
                      + snapshot.id().idAsString()
                      + ".nessie-metadata-no-manifests.json";
              result = nessieSnapshotResponse(effectiveReference, snapshot);
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
                      optionalIcebergSpec(reqParams.reqVersion()),
                      new NessieModelIceberg.IcebergSnapshotTweak() {
                        @Override
                        public String resolveManifestListLocation(String original) {
                          if (original == null || original.isEmpty()) {
                            return null;
                          }
                          URI manifestList =
                              catalogUriResolver.icebergManifestList(
                                  effectiveReference, key, snapshot);
                          if (reqParams.snapshotFormat().asImported()) {
                            return original;
                          }
                          return manifestList != null ? manifestList.toString() : null;
                        }

                        @Override
                        public List<String> resolveManifestFilesLocations(List<String> original) {
                          NessieFileManifestGroup fileManifestGroup = snapshot.fileManifestGroup();
                          if (fileManifestGroup == null) {
                            return emptyList();
                          }
                          return fileManifestGroup.manifests().stream()
                              .map(NessieFileManifestGroupEntry::icebergManifestPath)
                              .collect(toList());
                        }
                      },
                      tablePropertiesTweak);
              fileName = "00000-" + snapshotId + ".metadata.json";
              break;
            case ICEBERG_MANIFEST_LIST:
            case ICEBERG_MANIFEST_LIST_IMPORTED:
              return produceIcebergManifestList(
                  effectiveReference,
                  snapshot,
                  optionalIcebergSpec(reqParams.reqVersion()),
                  manifestListFileName(snapshot),
                  manifest ->
                      reqParams.snapshotFormat().asImported()
                          ? manifest.icebergManifestPath()
                          : catalogUriResolver
                              .icebergManifestFile(
                                  effectiveReference,
                                  key,
                                  NessieFileManifestEntry.id(manifest.icebergManifestPath()))
                              .toString(),
                  tablePropertiesTweak);
            case ICEBERG_MANIFEST_FILE:
              return produceIcebergManifestFile(
                  effectiveReference,
                  key,
                  snapshot,
                  reqParams.manifestFileId().orElseThrow(),
                  optionalIcebergSpec(reqParams.reqVersion()),
                  catalogUriResolver,
                  tablePropertiesTweak);
            default:
              throw new IllegalArgumentException("Unknown format " + reqParams.snapshotFormat());
          }

          return SnapshotResponse.forEntity(
              effectiveReference, result, fileName, "application/json");
        });
  }

  @Override
  public CompletionStage<Void> commit(
      ParsedReference reference,
      CatalogCommit commit,
      SnapshotReqParams reqParams,
      CatalogUriResolver catalogUriResolver)
      throws NessieNotFoundException, NessieConflictException {
    GetContentBuilder contentRequest =
        nessieApi
            .getContent()
            .refName(reference.name())
            .hashOnRef(reference.hashWithRelativeSpec());
    commit.getOperations().forEach(op -> contentRequest.key(op.getKey()));
    GetMultipleContentsResponse contentsResponse = contentRequest.getWithResponse();

    Branch target =
        Branch.of(
            reference.name(),
            reference.hashWithRelativeSpec() != null
                ? reference.hashWithRelativeSpec()
                : contentsResponse.getEffectiveReference().getHash());

    Map<ContentKey, Content> contents = contentsResponse.toContentsMap();

    IcebergStuff icebergStuff = new IcebergStuff(objectIO, persist, tasksService, executor);

    CommitMultipleOperationsBuilder nessieCommit =
        nessieApi
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("Iceberg commit"))
            .branch(Branch.of(reference.name(), reference.hashWithRelativeSpec()));

    CompletionStage<Void> commitBuilderStage = CompletableFuture.completedStage(null);
    for (CatalogOperation op : commit.getOperations()) {
      if (op.getType() != Content.Type.ICEBERG_TABLE && op.getType() != Content.Type.ICEBERG_VIEW) {
        throw new IllegalArgumentException("Unsupported entity type: " + op.getType());
      }

      IcebergCatalogOperation icebergOp = (IcebergCatalogOperation) op;

      String contentId;
      CompletionStage<NessieTableSnapshot> snapshotStage;
      Content content = contents.get(op.getKey());
      if (content == null) {
        contentId = null;
        snapshotStage = newSnapshot(icebergOp.updates());
      } else {
        contentId = content.getId();
        snapshotStage = loadExistingSnapshot(content);
      }

      CompletionStage<Content> contentStage =
          snapshotStage
              .thenApply(
                  nessieSnapshot -> {
                    try {
                      for (IcebergUpdateRequirement requirement : icebergOp.requirements()) {
                        requirement.checkForTable(
                            nessieSnapshot, content != null, reference.name(), op.getKey());
                      }
                      return new IcebergTableMetadataUpdateState(
                              nessieSnapshot, op.getKey(), target, content != null)
                          .applyUpdates(icebergOp.updates())
                          .snapshot();
                    } catch (IllegalStateException | IllegalArgumentException e) {
                      throw new RuntimeException(
                          new IcebergConflictException("CommitFailedException", e.getMessage()));
                    }
                  })
              .thenApply(
                  nessieSnapshot -> {
                    // TODO: support GZIP
                    // TODO: support TableProperties.WRITE_METADATA_LOCATION
                    String location =
                        String.format(
                            "%s/metadata/00000-%s.metadata.json",
                            nessieSnapshot.icebergLocation(), UUID.randomUUID());

                    IcebergTableMetadata icebergMetadata =
                        storeSnapshot(URI.create(location), nessieSnapshot);
                    return icebergMetadataToContent(location, icebergMetadata, contentId);
                  });

      // Form a chain of stages that complete sequentially and populate the commit builder.
      commitBuilderStage =
          contentStage.thenCombine(
              commitBuilderStage,
              (updated, nothing) -> {
                nessieCommit.operation(Operation.Put.of(op.getKey(), updated));
                return null;
              });
    }

    return commitBuilderStage.thenApply(
        v -> {
          try {
            CommitResponse commitResponse = nessieCommit.commitWithResponse();
            // TODO: return commitResponse?
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          return null;
        });
  }

  private CompletionStage<NessieTableSnapshot> newSnapshot(List<IcebergMetadataUpdate> updates)
      throws NessieContentNotFoundException {
    String icebergUuid =
        updates.stream()
            .filter(u -> u instanceof AssignUUID)
            .map(u -> ((AssignUUID) u).uuid())
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Missing UUID for new table"));

    // Use a transient ID for this snapshot because it is not stored as a Nessie snapshot,
    // but is used only to generate Iceberg snapshot data. A proper Nessie snapshot will
    // be created later, when the Iceberg snapshot is loaded after a successful Nessie commit.
    NessieId nessieId = NessieId.transientNessieId();
    NessieTable nessieTable =
        NessieTable.builder()
            .tableFormat(TableFormat.ICEBERG)
            .icebergUuid(icebergUuid)
            .nessieContentId(UUID.randomUUID().toString()) // Not used / redefined after commit
            // TODO: baselocation
            .baseLocation(BaseLocation.baseLocation(nessieId, "tmp", URI.create("file:///tmp")))
            .createdTimestamp(Instant.now())
            .build();
    return CompletableFuture.completedFuture(
        NessieTableSnapshot.builder()
            .id(nessieId)
            .entity(nessieTable)
            .lastUpdatedTimestamp(Instant.now())
            .icebergLastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
            .build());
  }

  private CompletionStage<NessieTableSnapshot> loadExistingSnapshot(Content content)
      throws NessieContentNotFoundException {
    ObjId snapshotId = snapshotIdFromContent(content);
    CompletionStage<NessieTableSnapshot> snapshotStage;
    try {
      snapshotStage =
          new IcebergStuff(objectIO, persist, tasksService, executor)
              .retrieveIcebergSnapshot(
                  snapshotId, content, SnapshotFormat.NESSIE_SNAPSHOT_NO_MANIFESTS);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return snapshotStage;
  }

  private IcebergTableMetadata storeSnapshot(URI location, NessieTableSnapshot snapshot) {
    IcebergTableMetadata tableMetadata =
        nessieTableSnapshotToIceberg(
            snapshot, Optional.empty(), IcebergSnapshotTweak.NOOP, p -> {});
    try (OutputStream out = objectIO.writeObject(location)) {
      IcebergJson.objectMapper().writeValue(out, tableMetadata);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return tableMetadata;
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
    return "snap-" + snapshot.icebergSnapshotId() + "-1-" + snapshot.id().idAsString() + ".avro";
  }

  private static String manifestFileName(NessieId manifestFileId) {
    return manifestFileId.idAsString() + "-m0.avro";
  }

  private SnapshotResponse produceIcebergManifestFile(
      Reference effectiveReference,
      ContentKey key,
      NessieTableSnapshot snapshot,
      NessieId manifestFileId,
      Optional<IcebergSpec> icebergSpec,
      CatalogUriResolver catalogUriResolver,
      Consumer<Map<String, String>> tablePropertiesTweak) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public Reference effectiveReference() {
        return effectiveReference;
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
                .filter(s -> s.id().equals(snapshot.currentSchemaId()))
                .findFirst()
                .orElseThrow();
        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitions().stream()
                .filter(p -> p.id().equals(snapshot.currentPartitionDefinitionId()))
                .findFirst()
                .orElseThrow();

        if (snapshot.fileManifestGroup() == null) {
          // TODO better exception & message
          throw new IllegalArgumentException("No manifest group");
        }

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
            snapshot.id().idAsString(),
            schema.icebergId(),
            schema.id().idAsString(),
            partitionDefinition.icebergId(),
            partitionDefinition.id().idAsString());

        IcebergSchema icebergSchema = nessieSchemaToIcebergSchema(schema);
        IcebergPartitionSpec partitionSpec =
            nessiePartitionDefinitionToIceberg(partitionDefinition);

        Map<String, String> properties = new HashMap<>(snapshot.properties());
        tablePropertiesTweak.accept(properties);

        IcebergManifestFileWriter.Builder manifestFileWriterBuilder =
            IcebergManifestFileWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                .schema(icebergSchema)
                .partitionSpec(partitionSpec)
                .tableProperties(properties)
                .output(outputStream)
                .manifestPath(nessieFileManifestGroupEntry.icebergManifestPath())
                .content(fromNessieFileContentType(nessieFileManifestGroupEntry.content()));
        safeApply(
            nessieFileManifestGroupEntry.addedSnapshotId(),
            manifestFileWriterBuilder::addedSnapshotId);
        safeApply(
            nessieFileManifestGroupEntry.sequenceNumber(),
            manifestFileWriterBuilder::sequenceNumber);
        safeApply(
            nessieFileManifestGroupEntry.minSequenceNumber(),
            manifestFileWriterBuilder::minSequenceNumber);
        try (IcebergManifestFileWriter.IcebergManifestFileEntryWriter writer =
            manifestFileWriterBuilder.build().entryWriter()) {

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
                  catalogUriResolver
                      .dataFile(effectiveReference, key, fileFormat, entry.dataFile().filePath())
                      .toString();
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

  private static void safeApply(Long boxed, LongConsumer consumer) {
    if (boxed != null) {
      consumer.accept(boxed);
    }
  }

  private SnapshotResponse produceIcebergManifestList(
      Reference effectiveReference,
      NessieTableSnapshot snapshot,
      Optional<IcebergSpec> icebergSpec,
      String fileName,
      Function<NessieFileManifestGroupEntry, String> tweakManifestPath,
      Consumer<Map<String, String>> tablePropertiesTweak) {
    return new SnapshotResponse() {
      @Override
      public Optional<Object> entityObject() {
        return Optional.empty();
      }

      @Override
      public Reference effectiveReference() {
        return effectiveReference;
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
                .filter(s -> s.id().equals(snapshot.currentSchemaId()))
                .findFirst()
                .orElseThrow();
        NessiePartitionDefinition partitionDefinition =
            snapshot.partitionDefinitions().stream()
                .filter(p -> p.id().equals(snapshot.currentPartitionDefinitionId()))
                .findFirst()
                .orElseThrow();

        NessieFileManifestGroup nessieFileManifestGroup = snapshot.fileManifestGroup();

        LOGGER.info(
            "Producing manifest list with {} entries for snapshot {}/{}, schema {}/{}, partition-definition {}/{}",
            nessieFileManifestGroup.manifests().size(),
            snapshot.icebergSnapshotId(),
            snapshot.id().idAsString(),
            schema.icebergId(),
            schema.id().idAsString(),
            partitionDefinition.icebergId(),
            partitionDefinition.id().idAsString());

        Map<String, String> properties = new HashMap<>(snapshot.properties());
        tablePropertiesTweak.accept(properties);

        try (IcebergManifestListWriter.IcebergManifestListEntryWriter writer =
            IcebergManifestListWriter.builder()
                .spec(icebergSpec.orElse(IcebergSpec.forVersion(snapshot.icebergFormatVersion())))
                // TODO .putTableProperty(AVRO_COMPRESSION, avroCompression())
                .schema(nessieSchemaToIcebergSchema(schema))
                .partitionSpec(nessiePartitionDefinitionToIceberg(partitionDefinition))
                // .parentSnapshotId()  - TODO ??
                .snapshotId(snapshot.icebergSnapshotId())
                .tableProperties(properties)
                //                .sequenceNumber(snapshot.icebergSnapshotSequenceNumber())
                .output(outputStream)
                .build()
                .entryWriter()) {

          for (NessieFileManifestGroupEntry manifest : nessieFileManifestGroup.manifests()) {
            IcebergManifestFile icebergManifestFile =
                nessieGroupEntryToIcebergManifestFile(manifest, tweakManifestPath.apply(manifest));
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
