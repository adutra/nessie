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
import static org.projectnessie.catalog.model.schema.types.NessieType.DEFAULT_TIME_PRECISION;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ObjIntConsumer;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergBlobMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergHistoryEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
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
                  return NessiePartitionField.builder()
                      .name(p.name())
                      .sourceField(sourceField)
                      .transformSpec(transform)
                      .type(transform.transformedType(sourceField.type()))
                      .icebergFieldId(p.fieldId())
                      .build();
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

  public static IcebergTableMetadata nessieTableSnapshotToIceberg(
      NessieTableSnapshot nessie, Optional<IcebergSpec> requestedSpecVersion) {
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
              .manifests(nessie.icebergManifestFileLocations()) // TODO replace
              .manifestList(nessie.icebergManifestListLocation()) // TODO replace
              .summary(nessie.icebergSnapshotSummary())
              .timestampMs(timestampMs)
              .sequenceNumber(nessie.icebergSnapshotSequenceNumber());
      metadata.addSnapshots(snapshot.build());

      metadata.putRefs(
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
}
