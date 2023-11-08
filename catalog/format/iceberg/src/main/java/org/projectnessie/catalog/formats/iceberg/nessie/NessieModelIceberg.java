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
package org.projectnessie.catalog.formats.iceberg.nessie;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.catalog.model.schema.types.NessieType.DEFAULT_TIME_PRECISION;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.ObjIntConsumer;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergDataFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestContent;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestEntry;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFileReader;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.manifest.SeekableStreamInput;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergBlobMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergHistoryEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotLogEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTransform;
import org.projectnessie.catalog.formats.iceberg.types.IcebergDecimalType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergFixedType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergListType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergMapType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergStructType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.manifest.NessieDataFileManifest;
import org.projectnessie.catalog.model.manifest.NessieFieldSummary;
import org.projectnessie.catalog.model.manifest.NessieFieldValue;
import org.projectnessie.catalog.model.manifest.NessieFileContentType;
import org.projectnessie.catalog.model.manifest.NessieListManifestEntry;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieFieldTransform;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.schema.NessieSortField;
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.catalog.model.schema.types.NessieDecimalTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieFixedTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieListTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieMapTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieStructTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieTimeTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieTimestampTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieType;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.model.IcebergTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieModelIceberg {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieModelIceberg.class);

  public static final long NO_SNAPSHOT_ID = -1;
  public static final long INITIAL_SEQUENCE_NUMBER = 0;
  public static final int INITIAL_SPEC_ID = 0;
  public static final int INITIAL_SORT_ORDER_ID = 1;
  public static final int INITIAL_SCHEMA_ID = 0;
  public static final int INITIAL_COLUMN_ID = 0;

  public NessieModelIceberg() {}

  public static IcebergSortOrder nessieSortDefinitionToIceberg(
      NessieSortDefinition sortDefinition) {
    List<IcebergSortField> fields =
        sortDefinition.columns().stream()
            .map(
                f ->
                    IcebergSortField.sortField(
                        nessieTransformToIceberg(f.transformSpec()).toString(),
                        f.sourceField().icebergColumnId(),
                        f.direction(),
                        f.nullOrder()))
            .collect(Collectors.toList());
    return IcebergSortOrder.sortOrder(sortDefinition.icebergSortOrderId(), fields);
  }

  public static NessieSortDefinition icebergSortOrderToNessie(
      IcebergSortOrder sortOrder, Map<Integer, NessieField> icebergFieldIdToField) {
    List<NessieSortField> fields =
        sortOrder.fields().stream()
            .map(
                f -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(f.transform()));
                  NessieField sourceField = icebergFieldIdToField.get(f.sourceId());
                  checkArgument(
                      sourceField != null,
                      "Source field with ID %s for sort order with ID %s does not exist",
                      f.sourceId(),
                      sortOrder.orderId());
                  return NessieSortField.builder()
                      .sourceField(sourceField)
                      .nullOrder(f.nullOrder())
                      .direction(f.direction())
                      .transformSpec(transform)
                      .type(transform.transformedType(sourceField.type()))
                      .build();
                })
            .collect(Collectors.toList());

    return NessieSortDefinition.nessieSortDefinition(fields, sortOrder.orderId());
  }

  public static IcebergPartitionSpec nessiePartitionDefinitionToIceberg(
      NessiePartitionDefinition partitionDefinition) {
    List<IcebergPartitionField> fields =
        partitionDefinition.columns().stream()
            .map(
                f -> {
                  return IcebergPartitionField.partitionField(
                      f.name(),
                      nessieTransformToIceberg(f.transformSpec()).toString(),
                      f.sourceField().icebergColumnId(),
                      f.icebergFieldId());
                })
            .collect(Collectors.toList());
    return IcebergPartitionSpec.partitionSpec(partitionDefinition.icebergSpecId(), fields);
  }

  static IcebergTransform nessieTransformToIceberg(NessieFieldTransform nessieFieldTransform) {
    // TODO check if we keep the same syntax or do something else
    return IcebergTransform.fromString(nessieFieldTransform.toString());
  }

  static NessieFieldTransform icebergTransformToNessie(IcebergTransform icebergTransform) {
    // TODO check if we keep the same syntax or do something else
    return NessieFieldTransform.fromString(icebergTransform.toString());
  }

  public static NessiePartitionDefinition icebergPartitionSpecToNessie(
      IcebergPartitionSpec partitionSpec, Map<Integer, NessieField> icebergFieldIdToField) {
    List<NessiePartitionField> fields =
        partitionSpec.fields().stream()
            .map(
                p -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(p.transform()));
                  NessieField sourceField = icebergFieldIdToField.get(p.sourceId());
                  checkArgument(
                      sourceField != null,
                      "Source field with ID %s for partition spec with ID %s does not exist",
                      p.sourceId(),
                      partitionSpec.specId());
                  return NessiePartitionField.nessiePartitionField(
                      sourceField,
                      p.name(),
                      transform.transformedType(sourceField.type()),
                      transform,
                      p.fieldId());
                })
            .collect(Collectors.toList());

    return NessiePartitionDefinition.nessiePartitionDefinition(fields, partitionSpec.specId());
  }

  public static IcebergSchema nessieSchemaToIcebergSchema(NessieSchema schema) {
    Map<NessieId, Integer> idMap = new HashMap<>();
    List<IcebergNestedField> fields =
        schema.struct().fields().stream()
            .map(
                f -> {
                  idMap.put(f.fieldId(), f.icebergColumnId());
                  return IcebergNestedField.nestedField(
                      f.icebergColumnId(),
                      f.name(),
                      !f.nullable(),
                      nessieTypeToIcebergType(f.type()),
                      f.documentation());
                })
            .collect(Collectors.toList());

    List<Integer> identifierFieldIds =
        schema.identifierFieldIds().stream().map(idMap::get).collect(Collectors.toList());

    return IcebergSchema.schema(schema.icebergSchemaId(), identifierFieldIds, fields);
  }

  public static NessieSchema icebergSchemaToNessieSchema(
      IcebergSchema schema, Map<Integer, NessieField> icebergFieldIdToField) {
    NessieStruct struct =
        icebergStructFieldsToNessie(
            schema.fields(),
            "", // TODO
            (field, id) -> icebergFieldIdToField.put(id, field));

    List<NessieId> identifierFieldIds =
        schema.identifierFieldIds().stream()
            .map(icebergFieldIdToField::get)
            .map(NessieField::fieldId)
            .collect(Collectors.toList());

    return NessieSchema.nessieSchema(struct, schema.schemaId(), identifierFieldIds);
  }

  public static IcebergType nessieTypeToIcebergType(NessieTypeSpec type) {
    switch (type.type()) {
      case BOOLEAN:
        return IcebergType.booleanType();
      case INT:
        return IcebergType.integerType();
      case BIGINT:
        return IcebergType.longType();
      case FLOAT:
        return IcebergType.floatType();
      case DOUBLE:
        return IcebergType.doubleType();
      case UUID:
        return IcebergType.uuidType();
      case STRING:
        return IcebergType.stringType();
      case DATE:
        return IcebergType.dateType();
      case TIME:
        NessieTimeTypeSpec time = (NessieTimeTypeSpec) type;
        if (time.precision() != DEFAULT_TIME_PRECISION || time.withTimeZone()) {
          throw new IllegalArgumentException("Data type not supported in Iceberg: " + type);
        }
        return IcebergType.timeType();
      case TIMESTAMP:
        NessieTimestampTypeSpec timestamp = (NessieTimestampTypeSpec) type;
        if (timestamp.precision() != DEFAULT_TIME_PRECISION) {
          throw new IllegalArgumentException("Data type not supported in Iceberg: " + type);
        }
        return timestamp.withTimeZone()
            ? IcebergType.timestamptzType()
            : IcebergType.timestampType();
      case BINARY:
        return IcebergType.binaryType();
      case DECIMAL:
        NessieDecimalTypeSpec decimal = (NessieDecimalTypeSpec) type;
        return IcebergType.decimalType(decimal.precision(), decimal.scale());
      case FIXED:
        NessieFixedTypeSpec fixed = (NessieFixedTypeSpec) type;
        return IcebergType.fixedType(fixed.length());
      case LIST:
        NessieListTypeSpec list = (NessieListTypeSpec) type;
        return IcebergType.listType(
            list.icebergElementFieldId(),
            nessieTypeToIcebergType(list.elementType()),
            !list.elementsNullable());
      case MAP:
        NessieMapTypeSpec map = (NessieMapTypeSpec) type;
        return IcebergType.mapType(
            map.icebergKeyFieldId(),
            nessieTypeToIcebergType(map.keyType()),
            map.icebergValueFieldId(),
            nessieTypeToIcebergType(map.valueType()),
            !map.valuesNullable());
      case STRUCT:
        NessieStructTypeSpec struct = (NessieStructTypeSpec) type;
        return IcebergType.structType(
            struct.struct().fields().stream()
                .map(NessieModelIceberg::nessieFieldToIceberg)
                .collect(Collectors.toList()),
            struct.struct().icebergRecordName());
      case INTERVAL:
      case TINYINT:
      case SMALLINT:
      default:
        throw new IllegalArgumentException("Type unsupported in Iceberg: " + type);
    }
  }

  private static IcebergNestedField nessieFieldToIceberg(NessieField nessieField) {
    return IcebergNestedField.nestedField(
        nessieField.icebergColumnId(),
        nessieField.name(),
        !nessieField.nullable(),
        nessieTypeToIcebergType(nessieField.type()),
        nessieField.documentation());
  }

  public static NessieTypeSpec icebergTypeToNessieType(
      IcebergType type, ObjIntConsumer<NessieField> fieldConsumer) {
    switch (type.type()) {
      case IcebergType.TYPE_TIMESTAMP_TZ:
        return NessieType.timestampType(true);
      case IcebergType.TYPE_TIMESTAMP:
        return NessieType.timestampType(false);
      case IcebergType.TYPE_BOOLEAN:
        return NessieType.booleanType();
      case IcebergType.TYPE_UUID:
        return NessieType.uuidType();
      case IcebergType.TYPE_INT:
        return NessieType.intType();
      case IcebergType.TYPE_LONG:
        return NessieType.bigintType();
      case IcebergType.TYPE_FLOAT:
        return NessieType.floatType();
      case IcebergType.TYPE_DOUBLE:
        return NessieType.doubleType();
      case IcebergType.TYPE_DATE:
        return NessieType.dateType();
      case IcebergType.TYPE_TIME:
        return NessieType.timeType();
      case IcebergType.TYPE_STRING:
        return NessieType.stringType();
      case IcebergType.TYPE_BINARY:
        return NessieType.binaryType();
      default:
        break;
    }

    if (type instanceof IcebergDecimalType) {
      IcebergDecimalType f = (IcebergDecimalType) type;
      return NessieType.decimalType(f.scale(), f.precision());
    }
    if (type instanceof IcebergFixedType) {
      IcebergFixedType f = (IcebergFixedType) type;
      return NessieType.fixedType(f.length());
    }
    if (type instanceof IcebergStructType) {
      IcebergStructType s = (IcebergStructType) type;
      return NessieType.structType(
          icebergStructFieldsToNessie(s.fields(), s.avroRecordName(), fieldConsumer));
    }
    if (type instanceof IcebergMapType) {
      IcebergMapType m = (IcebergMapType) type;
      return NessieType.mapType(
          icebergTypeToNessieType(m.key(), fieldConsumer),
          m.keyId(),
          icebergTypeToNessieType(m.value(), fieldConsumer),
          m.valueId(),
          !m.valueRequired());
    }
    if (type instanceof IcebergListType) {
      IcebergListType l = (IcebergListType) type;
      return NessieType.listType(
          icebergTypeToNessieType(l.element(), fieldConsumer), l.elementId(), !l.elementRequired());
    }

    throw new IllegalArgumentException("Unsupported Iceberg type " + type);
  }

  static NessieStruct icebergStructFieldsToNessie(
      List<IcebergNestedField> fields,
      String recordName,
      ObjIntConsumer<NessieField> fieldConsumer) {
    return NessieStruct.nessieStruct(
        fields.stream()
            .map(
                f -> {
                  NessieTypeSpec nessieType = icebergTypeToNessieType(f.type(), fieldConsumer);
                  NessieId fieldId =
                      NessieIdHasher.nessieIdHasher()
                          .hash(f.name())
                          .hash(nessieType)
                          .hash(!f.required())
                          .hash(f.id())
                          .hash(f.doc())
                          .generate();
                  NessieField field =
                      NessieField.builder()
                          .fieldId(fieldId)
                          .name(f.name())
                          .type(nessieType)
                          .nullable(!f.required())
                          .icebergColumnId(f.id())
                          .documentation(f.doc())
                          .build();
                  fieldConsumer.accept(field, f.id());
                  return field;
                })
            .collect(Collectors.toList()),
        recordName);
  }

  public static NessieTableSnapshot icebergTableSnapshotToNessie(
      NessieId snapshotId,
      NessieTableSnapshot previous,
      NessieTable table,
      IcebergTableMetadata iceberg) {
    NessieTableSnapshot.Builder snapshot = NessieTableSnapshot.builder().snapshotId(snapshotId);
    if (previous != null) {
      snapshot.from(previous);
    }

    NessieTable.Builder entity = NessieTable.builder().from(table);

    int formatVersion = iceberg.formatVersion();
    snapshot.icebergFormatVersion(formatVersion);

    snapshot.icebergLastSequenceNumber(iceberg.lastSequenceNumber());
    snapshot.icebergLocation(iceberg.location());

    entity.icebergUuid(iceberg.tableUuid());

    int currentSchemaId =
        extractCurrentId(iceberg.currentSchemaId(), iceberg.schema(), IcebergSchema::schemaId);
    int defaultSpecId =
        extractCurrentId(iceberg.defaultSpecId(), null, IcebergPartitionSpec::specId);
    int defaultSortOrderId =
        extractCurrentId(iceberg.defaultSortOrderId(), null, IcebergSortOrder::orderId);

    Map<Integer, NessieField> icebergFieldIdToField = new HashMap<>();
    Map<Integer, NessieSchema> icebergSchemaIdToSchema = new HashMap<>();
    partsWithV1DefaultPart(iceberg.schemas(), iceberg.schema(), IcebergSchema::schemaId)
        .forEach(
            schema -> {
              boolean isCurrent = schema.schemaId() == currentSchemaId;

              NessieSchema nessieSchema =
                  icebergSchemaToNessieSchema(schema, icebergFieldIdToField);

              icebergSchemaIdToSchema.put(schema.schemaId(), nessieSchema);

              snapshot.addSchemas(nessieSchema);
              if (isCurrent) {
                snapshot.currentSchema(nessieSchema.schemaId());
              }
            });

    IcebergPartitionSpec partitionSpecV1;
    if (formatVersion == 1 && defaultSpecId != -1 && !iceberg.partitionSpec().isEmpty()) {
      partitionSpecV1 =
          IcebergPartitionSpec.builder()
              .specId(defaultSpecId)
              .fields(iceberg.partitionSpec())
              .build();
    } else {
      partitionSpecV1 = null;
    }

    partsWithV1DefaultPart(iceberg.partitionSpecs(), partitionSpecV1, IcebergPartitionSpec::specId)
        .forEach(
            partitionSpec -> {
              boolean isCurrent = partitionSpec.specId() == defaultSpecId;

              NessiePartitionDefinition partitionDefinition =
                  icebergPartitionSpecToNessie(partitionSpec, icebergFieldIdToField);

              snapshot.addPartitionDefinitions(partitionDefinition);
              if (isCurrent) {
                snapshot.currentPartitionDefinition(partitionDefinition.partitionDefinitionId());
              }
            });

    partsWithV1DefaultPart(iceberg.sortOrders(), null, IcebergSortOrder::orderId)
        .forEach(
            sortOrder -> {
              boolean isCurrent = sortOrder.orderId() == defaultSortOrderId;

              NessieSortDefinition sortDefinition =
                  icebergSortOrderToNessie(sortOrder, icebergFieldIdToField);

              snapshot.addSortDefinitions(sortDefinition);
              if (isCurrent) {
                snapshot.currentSortDefinition(sortDefinition.sortDefinitionId());
              }
            });

    IcebergSnapshot currentSnapshot =
        iceberg.snapshots().stream()
            .filter(s -> s.snapshotId() == iceberg.currentSnapshotId())
            .findFirst()
            .orElse(null);
    if (currentSnapshot != null) {
      snapshot.icebergSnapshotId(currentSnapshot.snapshotId());
      currentSnapshot
          .parentSnapshotId(); // TODO Can we leave this unset, as we do not return previous Iceberg
      // snapshots??
      Integer schemaId = currentSnapshot.schemaId();
      if (schemaId != null) {
        // TODO this overwrites the "current schema ID" with the schema ID of the current snapshot.
        //  Is this okay??
        NessieSchema currentSchema = icebergSchemaIdToSchema.get(schemaId);
        if (currentSchema != null) {
          snapshot.currentSchema(currentSchema.schemaId());
        }
      }
      snapshot.icebergSnapshotSequenceNumber(currentSnapshot.sequenceNumber());
      snapshot.snapshotCreatedTimestamp(Instant.ofEpochMilli(currentSnapshot.timestampMs()));
      snapshot.icebergSnapshotSummary(currentSnapshot.summary());

      snapshot.icebergManifestListLocation(currentSnapshot.manifestList());
      snapshot.icebergManifestFileLocations(currentSnapshot.manifests());

      currentSnapshot.manifestList(); // TODO
      currentSnapshot.manifests(); // TODO
    }
    snapshot.lastUpdatedTimestamp(Instant.ofEpochMilli(iceberg.lastUpdatedMs()));

    for (IcebergSnapshotLogEntry logEntry : iceberg.snapshotLog()) {
      // TODO ??
      logEntry.snapshotId();
      logEntry.timestampMs();
    }

    for (IcebergStatisticsFile statisticsFile : iceberg.statistics()) {
      // TODO ??
      statisticsFile.snapshotId();
      statisticsFile.statisticsPath();
      statisticsFile.fileSizeInBytes();
      statisticsFile.fileFooterSizeInBytes();
      for (IcebergBlobMetadata blobMetadata : statisticsFile.blobMetadata()) {
        blobMetadata.snapshotId();
        blobMetadata.fields();
        blobMetadata.type();
        blobMetadata.properties();
        blobMetadata.sequenceNumber();
      }
    }

    // TODO This is an idea to use the "base entity object" to track these values.
    //  It feels legit for "last column ID" and "last partition ID", but "last sequence number" is
    //  updated for (nearly) every DML operation, affecting performance / causing contention.
    //  Having a "global sequence number" might also _not_ help solving Nessie merge conflicts.
    entity.icebergLastColumnId(Math.max(iceberg.lastColumnId(), table.icebergLastColumnId()));
    entity.icebergLastPartitionId(
        Math.max(iceberg.lastPartitionId(), table.icebergLastPartitionId()));

    for (IcebergHistoryEntry historyEntry : iceberg.metadataLog()) {
      // TODO ??
      historyEntry.metadataFile();
      historyEntry.timestampMs();
    }

    snapshot.properties(iceberg.properties());
    iceberg.refs(); // TODO ??

    snapshot.entity(entity.build());

    return snapshot.build();
  }

  public static NessieTable newNessieTable(
      IcebergTable content, NessieId tableId, IcebergTableMetadata tableMetadata) {
    NessieTable table;
    table =
        NessieTable.builder()
            .id(tableId)
            .createdTimestamp(Instant.ofEpochMilli(tableMetadata.lastUpdatedMs()))
            .tableFormat(TableFormat.ICEBERG)
            .icebergUuid(tableMetadata.tableUuid())
            .icebergLastColumnId(tableMetadata.lastColumnId())
            .icebergLastPartitionId(tableMetadata.lastPartitionId())
            .nessieContentId(content.getId())
            .baseLocation(
                BaseLocation.baseLocation(
                    NessieId.emptyNessieId(), "dummy", URI.create("dummy://dummy")))
            .build();
    return table;
  }

  public static void importIcebergManifests(
      IcebergSnapshot icebergSnapshot,
      IntFunction<NessiePartitionDefinition> lookUpPartitionDefinitionBySpecId,
      Consumer<NessieListManifestEntry> listEntryConsumer,
      Consumer<NessieDataFileManifest> dataFileManifestConsumer,
      SeekableStreamInput.SourceProvider sourceProvider) {
    String manifestList = icebergSnapshot.manifestList();
    if (manifestList != null && !manifestList.isEmpty()) {
      // Common code path - use the manifest-list file per Iceberg snapshot.
      importIcebergManifestList(
          manifestList,
          lookUpPartitionDefinitionBySpecId,
          listEntryConsumer,
          dataFileManifestConsumer,
          sourceProvider);
    } else if (!icebergSnapshot.manifests().isEmpty()) {
      // Old table-metadata files, from an Iceberg version that does not write a manifest-list, but
      // just a list of manifest-file location.
      importManifestFilesForIcebergSpecV1(icebergSnapshot.manifests(), sourceProvider);
    }
  }

  /** Imports an Iceberg manifest-list. */
  public static void importIcebergManifestList(
      String manifestListLocation,
      IntFunction<NessiePartitionDefinition> lookUpPartitionDefinitionBySpecId,
      Consumer<NessieListManifestEntry> listEntryConsumer,
      Consumer<NessieDataFileManifest> dataFileManifestConsumer,
      SeekableStreamInput.SourceProvider sourceProvider) {
    LOGGER.info("Fetching Iceberg manifest-list from {}", manifestListLocation);

    // TODO there is a lot of back-and-forth re-serialization:
    //  between Java-types (in Avro record) and byte(buffers)
    //  The Nessie Catalog should have ONE distinct way to serialize values and reuse data as much
    //  as possible!!

    try (SeekableInput avroListInput =
            new SeekableStreamInput(URI.create(manifestListLocation), sourceProvider);
        IcebergManifestListReader.IcebergManifestListEntryReader listReader =
            IcebergManifestListReader.builder().build().entryReader(avroListInput)) {

      // TODO The information in the manifest-list header seems redundant?
      // TODO Do we need to store it separately?
      // TODO Do we need an assertion that it matches the values in the Iceberg snapshot?
      IcebergSpec listIcebergSpec = listReader.spec();
      // TODO assert if the snapshot ID's the same?
      long listSnapshotId = listReader.snapshotId();
      // TODO store parentSnapshotId in NessieTableSnapshot ?
      long listParentSnapshotId = listReader.parentSnapshotId();
      // TODO store the list's sequenceNumber separately in NessieTableSnapshot ?
      long listSequenceNumber = listReader.sequenceNumber();

      // TODO trace level
      LOGGER.info(
          "  format version {}, snapshot id {}, sequence number {}",
          listReader.spec(),
          listReader.snapshotId(),
          listReader.sequenceNumber());

      while (listReader.hasNext()) {
        IcebergManifestFile manifestListEntry = listReader.next();

        // TODO only import manifest files that do not already exist

        // TODO trace level
        LOGGER.info("  with manifest list entry {}", manifestListEntry);

        IcebergManifestContent entryContent = manifestListEntry.content();
        NessieFileContentType content =
            entryContent != null
                ? entryContent.nessieFileContentType()
                : NessieFileContentType.ICEBERG_DATA_FILE;

        NessiePartitionDefinition partitionDefinition =
            lookUpPartitionDefinitionBySpecId.apply(manifestListEntry.partitionSpecId());

        // TODO remove collection overhead
        List<NessieFieldSummary> partitions = new ArrayList<>();
        for (int i = 0; i < manifestListEntry.partitions().size(); i++) {
          IcebergPartitionFieldSummary fieldSummary = manifestListEntry.partitions().get(i);
          partitions.add(
              NessieFieldSummary.builder()
                  .fieldId(partitionDefinition.columns().get(i).icebergFieldId())
                  .containsNan(fieldSummary.containsNan())
                  .containsNull(fieldSummary.containsNull())
                  .lowerBound(fieldSummary.lowerBound())
                  .upperBound(fieldSummary.upperBound())
                  .build());
        }

        NessieListManifestEntry.Builder entry =
            NessieListManifestEntry.builder()
                .content(content)
                //
                .icebergManifestPath(manifestListEntry.manifestPath())
                .icebergManifestLength(manifestListEntry.manifestLength())
                //
                .addedSnapshotId(manifestListEntry.addedSnapshotId())
                .sequenceNumber(manifestListEntry.sequenceNumber())
                .minSequenceNumber(manifestListEntry.minSequenceNumber())
                .partitionSpecId(manifestListEntry.partitionSpecId())
                .partitions(partitions)
                //
                .addedDataFilesCount(manifestListEntry.addedDataFilesCount())
                .addedRowsCount(manifestListEntry.addedRowsCount())
                .deletedDataFilesCount(manifestListEntry.deletedDataFilesCount())
                .deletedRowsCount(manifestListEntry.deletedRowsCount())
                .existingDataFilesCount(manifestListEntry.existingDataFilesCount())
                .existingRowsCount(manifestListEntry.existingRowsCount())
                //
                .keyMetadata(manifestListEntry.keyMetadata());

        try (SeekableInput avroFileInput =
                new SeekableStreamInput(
                    URI.create(manifestListEntry.manifestPath()), sourceProvider);
            IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
                IcebergManifestFileReader.builder().build().entryReader(avroFileInput)) {
          IcebergSchema schema = entryReader.schema(); // TODO need this one?
          IcebergSpec icebergSpec = entryReader.spec(); // TODO need special handling??
          IcebergManifestContent manifestContent =
              entryReader.content(); // TODO assert if its equal to the manifest-list-entry?
          IcebergPartitionSpec partitionSpec = entryReader.partitionSpec();
          while (entryReader.hasNext()) {
            NessieDataFileManifest.Builder dataFileManifestBuilder =
                NessieDataFileManifest.builder();

            IcebergManifestEntry manifestEntry = entryReader.next();
            dataFileManifestBuilder
                .icebergFileSequenceNumber(manifestEntry.fileSequenceNumber())
                .icebergSequenceNumber(manifestEntry.sequenceNumber())
                .icebergSnapshotId(manifestEntry.snapshotId())
                .status(manifestEntry.status().nessieFileStatus());

            IcebergDataFile dataFile = manifestEntry.dataFile();

            // TODO map to Nessie IDs
            // TODO isn't this information redundant to the information in the manifest-list-entry?
            // TODO should we add the sort-order-ID to the Nessie Catalog's manifest-list-entry?
            dataFileManifestBuilder.specId(dataFile.specId()).sortOrderId(dataFile.sortOrderId());

            dataFileManifestBuilder
                .content(dataFile.content().nessieFileContentType())
                .fileFormat(dataFile.fileFormat().nessieDataFileFormat())
                .filePath(dataFile.filePath())
                .fileSizeInBytes(dataFile.fileSizeInBytes())
                .blockSizeInBytes(dataFile.blockSizeInBytes())
                .recordCount(dataFile.recordCount())
                .splitOffsets(dataFile.splitOffsets());

            GenericData.Record partition = dataFile.partition(); // Avro data record
            for (int i = 0; i < partitionSpec.fields().size(); i++) {
              IcebergPartitionField partitionField = partitionSpec.fields().get(i);
              byte[] serializedPartitionField =
                  partitionField.type(schema).serializeSingleValue(partition.get(i));
              NessiePartitionField nessiePartitionField =
                  partitionDefinition.icebergColumnMap().get(partitionField.fieldId());
              checkState(
                  nessiePartitionField != null,
                  "No partition field in definition for Iceberg field ID %s",
                  partitionField.fieldId());
              dataFileManifestBuilder.addPartitionElement(
                  NessieFieldValue.builder()
                      .fieldId(nessiePartitionField.id())
                      .value(serializedPartitionField)
                      .icebergFieldId(partitionField.fieldId())
                      .build());
            }

            // Unlike Iceberg, Nessie Catalog stores a field-summary object, so one object per field
            // in a single map, not many maps.
            Map<Integer, NessieFieldSummary.Builder> fieldSummaries = new HashMap<>();
            IntFunction<NessieFieldSummary.Builder> fieldSummary =
                id ->
                    fieldSummaries.computeIfAbsent(
                        id,
                        i ->
                            NessieFieldSummary.builder()
                                // TODO should use the Nessie ID for the field here
                                .fieldId(i));

            dataFile
                .columnSizes()
                .forEach((key, value) -> fieldSummary.apply(key).columnSize(value));
            dataFile
                .lowerBounds()
                .forEach((key, value) -> fieldSummary.apply(key).lowerBound(value));
            dataFile
                .upperBounds()
                .forEach((key, value) -> fieldSummary.apply(key).upperBound(value));
            dataFile
                .nanValueCounts()
                .forEach((key, value) -> fieldSummary.apply(key).nanValueCount(value));
            dataFile
                .nullValueCounts()
                .forEach((key, value) -> fieldSummary.apply(key).nullValueCount(value));
            dataFile
                .valueCounts()
                .forEach(
                    (key, value) ->
                        fieldSummaries
                            .computeIfAbsent(key, id -> NessieFieldSummary.builder().fieldId(id))
                            .valueCount(value));

            fieldSummaries.values().stream()
                .map(NessieFieldSummary.Builder::build)
                .forEach(dataFileManifestBuilder::addColumns);

            dataFileManifestBuilder.keyMetadata(dataFile.keyMetadata());

            NessieDataFileManifest nessieDataFile = dataFileManifestBuilder.build();

            dataFileManifestConsumer.accept(nessieDataFile);

            entry.addDataFiles(nessieDataFile.id());

            LOGGER.info(
                "Manifest file entry:\n    {}\n    nessieDataFile: {}",
                manifestEntry,
                nessieDataFile);
          }
        }

        listEntryConsumer.accept(entry.build());
      }

    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  /**
   * Construct an Iceberg manifest-list from a list of manifest-file locations, used for
   * compatibility with table-metadata representations that do not have a manifest-list.
   */
  public static void importManifestFilesForIcebergSpecV1(
      List<String> manifests, SeekableStreamInput.SourceProvider sourceProvider) {
    // TODO debug level
    LOGGER.info(
        "Constructing Iceberg manifest-list from list with {} manifest file locations",
        manifests.size());

    // TODO unify with code used by importManifestListFromList()

    for (String manifest : manifests) {

      try (SeekableInput avroFileInput =
              new SeekableStreamInput(URI.create(manifest), sourceProvider);
          IcebergManifestFileReader.IcebergManifestFileEntryReader entryReader =
              IcebergManifestFileReader.builder().build().entryReader(avroFileInput)) {
        entryReader.spec();
        entryReader.content();
        entryReader.partitionSpec();
        entryReader.schema();

        long minSeqNum = Long.MAX_VALUE;
        long seqNum = 0L;
        while (entryReader.hasNext()) {
          IcebergManifestEntry entry = entryReader.next();
          Long sequenceNumber = entry.sequenceNumber();
          if (sequenceNumber != null) {
            // TODO is this correct??
            minSeqNum = Math.min(minSeqNum, sequenceNumber);
            seqNum = Math.max(seqNum, sequenceNumber);
          }
          entry.snapshotId();
          entry.fileSequenceNumber();
          entry.status();

          IcebergDataFile dataFile = entry.dataFile();
          dataFile.content();

          dataFile.specId();
          dataFile.partition();
          dataFile.equalityIds();
          dataFile.sortOrderId();

          dataFile.fileFormat();
          dataFile.filePath();
          dataFile.fileSizeInBytes();
          dataFile.blockSizeInBytes();
          dataFile.recordCount();
          dataFile.splitOffsets();

          dataFile.keyMetadata();

          dataFile.columnSizes();
          dataFile.lowerBounds();
          dataFile.upperBounds();
          dataFile.nanValueCounts();
          dataFile.nullValueCounts();
          dataFile.valueCounts();
        }

        IcebergManifestFile.Builder manifestFile =
            IcebergManifestFile.builder()
                .manifestLength(avroFileInput.length())
                .manifestPath(manifest)
                .partitionSpecId(entryReader.partitionSpec().specId());
        if (minSeqNum != Long.MAX_VALUE) {
          manifestFile.minSequenceNumber(minSeqNum).sequenceNumber(seqNum);
        }
        manifestFile.build();

      } catch (Exception e) {
        // TODO some better error handling
        throw new RuntimeException(e);
      }
    }

    throw new UnsupportedOperationException("IMPLEMENT AND CHECK THIS");
  }

  static <T> int extractCurrentId(Integer fromTableMetadata, T single, ToIntFunction<T> extractId) {
    if (fromTableMetadata != null) {
      return fromTableMetadata;
    }
    if (single != null) {
      return extractId.applyAsInt(single);
    }
    // no current schema/spec/sort-order, return an ID value that's definitely not present
    return -1;
  }

  /**
   * Iceberg format spec allows the singleton fields {@code schema} referencing the default/current
   * schema, in addition to the lists containing these parts.
   *
   * <p>This function optionally concatenates the singleton to a given list, both parameters are
   * nullable.
   */
  private static <T> Stream<T> partsWithV1DefaultPart(
      List<T> list, T single, ToIntFunction<T> extractId) {
    if (single == null) {
      // no 'single', can short-cut here
      return list != null ? list.stream() : Stream.empty();
    }
    if (list == null || list.isEmpty()) {
      // no/empty 'list', return a singleton
      return Stream.of(single);
    }

    int singleId = extractId.applyAsInt(single);
    for (T t : list) {
      int id = extractId.applyAsInt(t);
      if (id == singleId) {
        // 'single' is in 'list', can short-cut here
        return list.stream();
      }
    }
    return Stream.concat(Stream.of(single), list.stream());
  }

  private static int safeUnbox(Integer value, int defaultValue) {
    return value != null ? value : defaultValue;
  }

  private static long safeUnbox(Long value, long defaultValue) {
    return value != null ? value : defaultValue;
  }

  public interface IcebergSnapshotTweak {
    String resolveManifestListLocation(String original);

    List<String> resolveManifestFilesLocations(List<String> original);

    IcebergSnapshotTweak NOOP =
        new IcebergSnapshotTweak() {
          @Override
          public String resolveManifestListLocation(String original) {
            return original;
          }

          @Override
          public List<String> resolveManifestFilesLocations(List<String> original) {
            return original;
          }
        };
  }

  public static IcebergTableMetadata nessieTableSnapshotToIceberg(
      NessieTableSnapshot nessie,
      Optional<IcebergSpec> requestedSpecVersion,
      IcebergSnapshotTweak tweak) {
    NessieTable entity = nessie.entity();

    checkArgument(entity.tableFormat() == TableFormat.ICEBERG, "Not an Iceberg table.");

    IcebergSpec spec =
        requestedSpecVersion.orElse(
            IcebergSpec.forVersion(safeUnbox(nessie.icebergFormatVersion(), 2)));

    long snapshotId = safeUnbox(nessie.icebergSnapshotId(), NO_SNAPSHOT_ID);

    Map<String, String> properties = new HashMap<>(nessie.properties());
    properties.put("nessie.catalog.content-id", entity.nessieContentId());
    properties.put("nessie.catalog.snapshot-id", nessie.snapshotId().idAsString());

    IcebergTableMetadata.Builder metadata =
        IcebergTableMetadata.builder()
            .tableUuid(entity.icebergUuid())
            .formatVersion(spec.version())
            .currentSnapshotId(snapshotId)
            .lastColumnId(safeUnbox(entity.icebergLastColumnId(), INITIAL_COLUMN_ID))
            .lastPartitionId(safeUnbox(entity.icebergLastPartitionId(), INITIAL_COLUMN_ID))
            .lastSequenceNumber(nessie.icebergLastSequenceNumber())
            .location(nessie.icebergLocation())
            .properties(properties)
            .lastUpdatedMs(nessie.lastUpdatedTimestamp().toEpochMilli());

    entity.baseLocation();

    switch (spec) {
      case V1:
        metadata.lastSequenceNumber(0L);
        break;
      case V2:
        metadata.lastSequenceNumber(nessie.icebergLastSequenceNumber());
        break;
      default:
        throw new IllegalArgumentException(spec.toString());
    }

    int currentSchemaId = INITIAL_SCHEMA_ID;
    for (NessieSchema schema : nessie.schemas()) {
      IcebergSchema iceberg = nessieSchemaToIcebergSchema(schema);
      metadata.addSchemas(iceberg);
      if (schema.schemaId().equals(nessie.currentSchema())) {
        currentSchemaId = schema.icebergSchemaId();
        if (spec.version() == 1) {
          metadata.schema(iceberg);
        }
      }
    }
    metadata.currentSchemaId(currentSchemaId);

    int defaultSpecId = INITIAL_SPEC_ID;
    for (NessiePartitionDefinition partitionDefinition : nessie.partitionDefinitions()) {
      IcebergPartitionSpec iceberg = nessiePartitionDefinitionToIceberg(partitionDefinition);
      metadata.addPartitionSpecs(iceberg);
      if (partitionDefinition.partitionDefinitionId().equals(nessie.currentPartitionDefinition())) {
        defaultSpecId = partitionDefinition.icebergSpecId();
        if (spec.version() == 1) {
          metadata.partitionSpec(iceberg.fields());
        }
      }
    }
    metadata.defaultSpecId(defaultSpecId);

    int defaultSortOrderId = INITIAL_SORT_ORDER_ID;
    for (NessieSortDefinition sortDefinition : nessie.sortDefinitions()) {
      IcebergSortOrder iceberg = nessieSortDefinitionToIceberg(sortDefinition);
      metadata.addSortOrders(iceberg);
      if (sortDefinition.sortDefinitionId().equals(nessie.currentSortDefinition())) {
        defaultSortOrderId = sortDefinition.icebergSortOrderId();
      }
    }
    metadata.defaultSortOrderId(defaultSortOrderId);

    if (snapshotId != NO_SNAPSHOT_ID) {
      long timestampMs = nessie.snapshotCreatedTimestamp().toEpochMilli();
      IcebergSnapshot.Builder snapshot =
          IcebergSnapshot.builder()
              .snapshotId(snapshotId)
              .schemaId(currentSchemaId) // TODO is this fine?
              // TODO can we safely omit the list of manifest files, unconditionally ?
              // .manifests(tweak.resolveManifestFilesLocations(nessie.icebergManifestFileLocations()))
              .manifestList(tweak.resolveManifestListLocation(nessie.icebergManifestListLocation()))
              .summary(nessie.icebergSnapshotSummary())
              .timestampMs(timestampMs)
              .sequenceNumber(nessie.icebergSnapshotSequenceNumber());
      metadata.addSnapshots(snapshot.build());

      metadata.putRef(
          "main", IcebergSnapshotRef.builder().snapshotId(snapshotId).type("branch").build());

      metadata.addSnapshotLog(
          IcebergSnapshotLogEntry.builder()
              .snapshotId(snapshotId)
              .timestampMs(timestampMs)
              .build());
    }

    // TODO metadata.statistics()
    //    metadata.addMetadataLog();
    //    metadata.addSnapshotLog();
    //    metadata.addStatistics();

    return metadata.build();
  }

  public static IcebergDataFile nessieDataFileToIceberg(
      IcebergSchema icebergSchema,
      IcebergPartitionSpec partitionSpec,
      NessieDataFileManifest dataFileManifest,
      Schema avroPartitionSchema) {
    IcebergDataFile.Builder dataFile =
        IcebergDataFile.builder()
            .fileFormat(IcebergFileFormat.fromNessieDataFileFormat(dataFileManifest.fileFormat()))
            .filePath(dataFileManifest.filePath())
            .content(IcebergDataContent.fromNessieFileContentType(dataFileManifest.content()))
            .fileSizeInBytes(dataFileManifest.fileSizeInBytes())
            .recordCount(dataFileManifest.recordCount())
            .sortOrderId(dataFileManifest.sortOrderId())
            .keyMetadata(dataFileManifest.keyMetadata());
    if (!dataFileManifest.equalityIds().isEmpty()) {
      // Nullable collection
      dataFile.equalityIds(dataFileManifest.equalityIds());
    }
    if (!dataFileManifest.splitOffsets().isEmpty()) {
      // Nullable collection
      dataFile.splitOffsets(dataFileManifest.splitOffsets());
    }
    for (NessieFieldSummary column : dataFileManifest.columns()) {
      Long v = column.columnSize();
      if (v != null) {
        dataFile.putColumnSize(column.fieldId(), v);
      }
      dataFile
          .putLowerBound(column.fieldId(), column.lowerBound())
          .putUpperBound(column.fieldId(), column.upperBound());
      v = column.nanValueCount();
      if (v != null) {
        dataFile.putNanValueCount(column.fieldId(), v);
      }
      v = column.nullValueCount();
      if (v != null) {
        dataFile.putNullValueCount(column.fieldId(), v);
      }
      v = column.valueCount();
      if (v != null) {
        dataFile.putValueCount(column.fieldId(), v);
      }

      GenericData.Record record = new GenericData.Record(avroPartitionSchema);
      for (int i = 0; i < dataFileManifest.partitionElements().size(); i++) {
        NessieFieldValue nessieFieldValue = dataFileManifest.partitionElements().get(i);
        IcebergType partitionFieldType = partitionSpec.fields().get(i).type(icebergSchema);
        Object value = partitionFieldType.deserializeSingleValue(nessieFieldValue.value());
        record.put(i, value);
      }
      dataFile.partition(record);
    }
    return dataFile.build();
  }
}
