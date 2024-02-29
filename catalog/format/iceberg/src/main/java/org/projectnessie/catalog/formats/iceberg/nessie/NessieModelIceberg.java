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
import static java.util.Collections.emptyList;
import static org.projectnessie.catalog.model.schema.types.NessieType.DEFAULT_TIME_PRECISION;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
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
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestFile;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergManifestListReader;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergBlobMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergHistoryEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.AddPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.AddSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.AddSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.AddSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.AssignUUID;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.SetLocation;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergMetadataUpdate.SetProperties;
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
import org.projectnessie.catalog.model.manifest.BooleanArray;
import org.projectnessie.catalog.model.manifest.NessieFieldSummary;
import org.projectnessie.catalog.model.manifest.NessieFieldValue;
import org.projectnessie.catalog.model.manifest.NessieFieldsSummary;
import org.projectnessie.catalog.model.manifest.NessieFileContentType;
import org.projectnessie.catalog.model.manifest.NessieFileManifestEntry;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroup;
import org.projectnessie.catalog.model.manifest.NessieFileManifestGroupEntry;
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
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;

public class NessieModelIceberg {

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
                        f.sourceField().icebergId(),
                        f.direction(),
                        f.nullOrder()))
            .collect(Collectors.toList());
    return IcebergSortOrder.sortOrder(sortDefinition.icebergSortOrderId(), fields);
  }

  public static NessieSortDefinition icebergSortOrderToNessie(
      IcebergSortOrder sortOrder, Map<Integer, NessieField> icebergFields) {
    List<NessieSortField> fields =
        sortOrder.fields().stream()
            .map(
                f -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(f.transform()));
                  NessieField sourceField = icebergFields.get(f.sourceId());
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
        partitionDefinition.fields().stream()
            .map(
                f ->
                    IcebergPartitionField.partitionField(
                        f.name(),
                        nessieTransformToIceberg(f.transformSpec()).toString(),
                        f.sourceField().icebergId(),
                        f.icebergId()))
            .collect(Collectors.toList());
    return IcebergPartitionSpec.partitionSpec(partitionDefinition.icebergId(), fields);
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
      IcebergPartitionSpec partitionSpec,
      Map<Integer, NessiePartitionField> icebergPartitionFields,
      Map<Integer, NessieField> icebergFields) {
    List<NessiePartitionField> fields =
        partitionSpec.fields().stream()
            .map(
                p -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(p.transform()));
                  NessieField sourceField = icebergFields.get(p.sourceId());
                  checkArgument(
                      sourceField != null,
                      "Source field with ID %s for partition spec with ID %s does not exist",
                      p.sourceId(),
                      partitionSpec.specId());
                  NessiePartitionField existing = icebergPartitionFields.get(p.fieldId());
                  UUID id = existing != null ? existing.id() : UUID.randomUUID();
                  NessiePartitionField partitionField =
                      NessiePartitionField.nessiePartitionField(
                          id,
                          sourceField,
                          p.name(),
                          transform.transformedType(sourceField.type()),
                          transform,
                          p.fieldId());
                  icebergPartitionFields.put(p.fieldId(), partitionField);
                  return partitionField;
                })
            .collect(Collectors.toList());

    return NessiePartitionDefinition.nessiePartitionDefinition(fields, partitionSpec.specId());
  }

  public static IcebergSchema nessieSchemaToIcebergSchema(NessieSchema schema) {
    Map<UUID, Integer> idMap = new HashMap<>();
    List<IcebergNestedField> fields =
        schema.struct().fields().stream()
            .map(
                f -> {
                  idMap.put(f.id(), f.icebergId());
                  return IcebergNestedField.nestedField(
                      f.icebergId(),
                      f.name(),
                      !f.nullable(),
                      nessieTypeToIcebergType(f.type()),
                      f.doc());
                })
            .collect(Collectors.toList());

    List<Integer> identifierFieldIds =
        schema.identifierFields().stream().map(idMap::get).collect(Collectors.toList());

    return IcebergSchema.schema(schema.icebergId(), identifierFieldIds, fields);
  }

  public static NessieSchema icebergSchemaToNessieSchema(
      IcebergSchema schema, Map<Integer, NessieField> icebergFields) {
    NessieStruct struct =
        icebergStructFieldsToNessie(
            schema.fields(),
            null, // TODO
            icebergFields);

    List<UUID> identifierFieldIds =
        schema.identifierFieldIds().stream()
            .map(icebergFields::get)
            .map(NessieField::id)
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
        nessieField.icebergId(),
        nessieField.name(),
        !nessieField.nullable(),
        nessieTypeToIcebergType(nessieField.type()),
        nessieField.doc());
  }

  static NessieTypeSpec icebergTypeToNessieType(
      IcebergType type, Map<Integer, NessieField> icebergFields) {
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
          icebergStructFieldsToNessie(s.fields(), s.avroRecordName(), icebergFields));
    }
    if (type instanceof IcebergMapType) {
      IcebergMapType m = (IcebergMapType) type;
      return NessieType.mapType(
          icebergTypeToNessieType(m.key(), icebergFields),
          m.keyId(),
          icebergTypeToNessieType(m.value(), icebergFields),
          m.valueId(),
          !m.valueRequired());
    }
    if (type instanceof IcebergListType) {
      IcebergListType l = (IcebergListType) type;
      return NessieType.listType(
          icebergTypeToNessieType(l.element(), icebergFields), l.elementId(), !l.elementRequired());
    }

    throw new IllegalArgumentException("Unsupported Iceberg type " + type);
  }

  static NessieStruct icebergStructFieldsToNessie(
      List<IcebergNestedField> fields, String recordName, Map<Integer, NessieField> icebergFields) {
    return NessieStruct.nessieStruct(
        fields.stream()
            .map(
                f -> {
                  NessieTypeSpec nessieType = icebergTypeToNessieType(f.type(), icebergFields);

                  NessieField existing = icebergFields.get(f.id());
                  UUID id = existing != null ? existing.id() : UUID.randomUUID();
                  // TODO should we check whether the type is convertible, if different?

                  NessieField field =
                      NessieField.builder()
                          .id(id)
                          .name(f.name())
                          .type(nessieType)
                          .nullable(!f.required())
                          .icebergId(f.id())
                          .doc(f.doc())
                          .build();
                  icebergFields.put(f.id(), field);
                  return field;
                })
            .collect(Collectors.toList()),
        recordName);
  }

  public static NessieTableSnapshot icebergTableSnapshotToNessie(
      NessieId snapshotId,
      NessieTableSnapshot previous,
      NessieTable table,
      IcebergTableMetadata iceberg,
      Function<IcebergSnapshot, String> manifestListLocation) {
    NessieTableSnapshot.Builder snapshot = NessieTableSnapshot.builder().snapshotId(snapshotId);
    if (previous != null) {
      snapshot.from(previous);
    }

    int formatVersion = iceberg.formatVersion();
    snapshot
        .entity(table)
        .icebergFormatVersion(formatVersion)
        .icebergLastSequenceNumber(iceberg.lastSequenceNumber())
        .icebergLocation(iceberg.location())
        .lastUpdatedTimestamp(Instant.ofEpochMilli(iceberg.lastUpdatedMs()))
        .icebergLastColumnId(iceberg.lastColumnId())
        .icebergLastPartitionId(iceberg.lastPartitionId())
        .properties(iceberg.properties());

    int currentSchemaId =
        extractCurrentId(iceberg.currentSchemaId(), iceberg.schema(), IcebergSchema::schemaId);
    int defaultSpecId =
        extractCurrentId(iceberg.defaultSpecId(), null, IcebergPartitionSpec::specId);
    int defaultSortOrderId =
        extractCurrentId(iceberg.defaultSortOrderId(), null, IcebergSortOrder::orderId);

    Map<Integer, NessieField> icebergFields = new HashMap<>();
    Map<Integer, NessiePartitionField> icebergPartitionFields = new HashMap<>();
    if (previous != null) {
      for (NessieSchema schema : previous.schemas()) {
        collectSchemaFields(schema, icebergFields);
      }
      for (NessiePartitionDefinition partitionDefinition : previous.partitionDefinitions()) {
        collectPartitionFields(partitionDefinition, icebergPartitionFields);
      }
    }

    Map<Integer, NessieSchema> icebergSchemaIdToSchema = new HashMap<>();

    partsWithV1DefaultPart(iceberg.schemas(), iceberg.schema(), IcebergSchema::schemaId)
        .forEach(
            schema -> {
              boolean isCurrent = schema.schemaId() == currentSchemaId;

              NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);

              icebergSchemaIdToSchema.put(schema.schemaId(), nessieSchema);

              snapshot.addSchemas(nessieSchema);
              if (isCurrent) {
                snapshot.currentSchema(nessieSchema.id());
              }
            });

    IcebergPartitionSpec partitionSpecV1;
    if (formatVersion == 1 && defaultSpecId != -1 && !iceberg.partitionSpec().isEmpty()) {
      // TODO add test(s) for this case
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
              // TODO add test(s) for this case
              boolean isCurrent = partitionSpec.specId() == defaultSpecId;

              NessiePartitionDefinition partitionDefinition =
                  icebergPartitionSpecToNessie(
                      partitionSpec, icebergPartitionFields, icebergFields);

              snapshot.addPartitionDefinitions(partitionDefinition);
              if (isCurrent) {
                snapshot.currentPartitionDefinition(partitionDefinition.id());
              }
            });

    partsWithV1DefaultPart(iceberg.sortOrders(), null, IcebergSortOrder::orderId)
        .forEach(
            sortOrder -> {
              // TODO add test(s) for this case
              boolean isCurrent = sortOrder.orderId() == defaultSortOrderId;

              NessieSortDefinition sortDefinition =
                  icebergSortOrderToNessie(sortOrder, icebergFields);

              snapshot.addSortDefinitions(sortDefinition);
              if (isCurrent) {
                snapshot.currentSortDefinition(sortDefinition.sortDefinitionId());
              }
            });

    iceberg
        .currentSnapshot()
        .ifPresent(
            currentSnapshot -> {
              snapshot.icebergSnapshotId(currentSnapshot.snapshotId());
              currentSnapshot
                  .parentSnapshotId(); // TODO Can we leave this unset, as we do not return previous
              // Iceberg
              // snapshots??
              Integer schemaId = currentSnapshot.schemaId();
              if (schemaId != null) {
                // TODO this overwrites the "current schema ID" with the schema ID of the current
                // snapshot.
                //  Is this okay??
                NessieSchema currentSchema = icebergSchemaIdToSchema.get(schemaId);
                if (currentSchema != null) {
                  snapshot.currentSchema(currentSchema.id());
                }
              }

              String listLocation = manifestListLocation.apply(currentSnapshot);

              snapshot
                  .icebergSnapshotSequenceNumber(currentSnapshot.sequenceNumber())
                  .snapshotCreatedTimestamp(Instant.ofEpochMilli(currentSnapshot.timestampMs()))
                  .icebergSnapshotSummary(currentSnapshot.summary())
                  .icebergManifestListLocation(listLocation)
                  .icebergManifestFileLocations(currentSnapshot.manifests());

              if (listLocation == null) {
                // Empty manifest-group if there are no manifest files.
                snapshot.fileManifestGroup(NessieFileManifestGroup.builder().build());
              }

              currentSnapshot.manifestList(); // TODO
              currentSnapshot.manifests(); // TODO
            });

    for (IcebergSnapshotLogEntry logEntry : iceberg.snapshotLog()) {
      // TODO ??
      logEntry.snapshotId();
      logEntry.timestampMs();
    }

    for (IcebergStatisticsFile statisticsFile : iceberg.statistics()) {
      // TODO needed??
      // TODO add test(s) for this case??
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

    for (IcebergHistoryEntry historyEntry : iceberg.metadataLog()) {
      // TODO needed??
      // TODO add test(s) for this case??
      historyEntry.metadataFile();
      historyEntry.timestampMs();
    }

    iceberg.refs(); // TODO ??

    return snapshot.build();
  }

  private static int max(Integer i1, Integer i2) {
    return Math.max(safeUnbox(i1, 0), safeUnbox(i2, 0));
  }

  private static void collectSchemaFields(
      NessieSchema schema, Map<Integer, NessieField> icebergFields) {
    for (NessieField field : schema.struct().fields()) {
      if (field.icebergId() != NessieField.NO_COLUMN_ID) {
        icebergFields.put(field.icebergId(), field);
      }
    }
  }

  private static void collectPartitionFields(
      NessiePartitionDefinition partitionDefinition,
      Map<Integer, NessiePartitionField> icebergPartitionFields) {
    for (NessiePartitionField field : partitionDefinition.fields()) {
      if (field.icebergId() != NessiePartitionField.NO_FIELD_ID) {
        icebergPartitionFields.put(field.icebergId(), field);
      }
    }
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
    // TODO add test(s) for this function
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

  public static int safeUnbox(Integer value, int defaultValue) {
    return value != null ? value : defaultValue;
  }

  public static long safeUnbox(Long value, long defaultValue) {
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
      IcebergSnapshotTweak tweak,
      Consumer<Map<String, String>> tablePropertiesTweak) {
    NessieTable entity = nessie.entity();

    checkArgument(entity.tableFormat() == TableFormat.ICEBERG, "Not an Iceberg table.");

    IcebergSpec spec =
        requestedSpecVersion.orElse(
            IcebergSpec.forVersion(safeUnbox(nessie.icebergFormatVersion(), 2)));

    long snapshotId = safeUnbox(nessie.icebergSnapshotId(), NO_SNAPSHOT_ID);

    Map<String, String> properties = new HashMap<>(nessie.properties());
    tablePropertiesTweak.accept(properties);

    IcebergTableMetadata.Builder metadata =
        IcebergTableMetadata.builder()
            .tableUuid(entity.icebergUuid())
            .formatVersion(spec.version())
            .currentSnapshotId(snapshotId)
            .lastColumnId(safeUnbox(nessie.icebergLastColumnId(), INITIAL_COLUMN_ID))
            .lastPartitionId(safeUnbox(nessie.icebergLastPartitionId(), INITIAL_COLUMN_ID))
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
      if (schema.id().equals(nessie.currentSchema())) {
        currentSchemaId = schema.icebergId();
        if (spec.version() == 1) {
          metadata.schema(iceberg);
        }
      }
    }
    metadata.currentSchemaId(currentSchemaId);

    int defaultSpecId = INITIAL_SPEC_ID;
    for (NessiePartitionDefinition partitionDefinition : nessie.partitionDefinitions()) {
      // TODO add test(s) for this
      IcebergPartitionSpec iceberg = nessiePartitionDefinitionToIceberg(partitionDefinition);
      metadata.addPartitionSpecs(iceberg);
      if (partitionDefinition.id().equals(nessie.currentPartitionDefinition())) {
        defaultSpecId = partitionDefinition.icebergId();
        if (spec.version() == 1) {
          metadata.partitionSpec(iceberg.fields());
        }
      }
    }
    metadata.defaultSpecId(defaultSpecId);

    int defaultSortOrderId = INITIAL_SORT_ORDER_ID;
    for (NessieSortDefinition sortDefinition : nessie.sortDefinitions()) {
      // TODO add test(s) for this
      IcebergSortOrder iceberg = nessieSortDefinitionToIceberg(sortDefinition);
      metadata.addSortOrders(iceberg);
      if (sortDefinition.sortDefinitionId().equals(nessie.currentSortDefinition())) {
        defaultSortOrderId = sortDefinition.icebergSortOrderId();
      }
    }
    metadata.defaultSortOrderId(defaultSortOrderId);

    if (snapshotId != NO_SNAPSHOT_ID) {
      long timestampMs = nessie.snapshotCreatedTimestamp().toEpochMilli();
      String manifestListLocation =
          tweak.resolveManifestListLocation(nessie.icebergManifestListLocation());
      // Only populate the `manifests` field for Iceberg spec v1 if the `manifest-list` location is
      // _not_ specified.
      List<String> manifestsLocations =
          spec == IcebergSpec.V1 && (manifestListLocation == null || manifestListLocation.isEmpty())
              ? tweak.resolveManifestFilesLocations(nessie.icebergManifestFileLocations())
              : emptyList();

      IcebergSnapshot.Builder snapshot =
          IcebergSnapshot.builder()
              .snapshotId(snapshotId)
              .schemaId(currentSchemaId) // TODO is this fine?
              .manifests(manifestsLocations)
              .manifestList(manifestListLocation)
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
      NessieFileManifestEntry fileManifest,
      Schema avroPartitionSchema) {
    // TODO add test(s) for this function

    IcebergDataFile.Builder dataFile =
        IcebergDataFile.builder()
            .fileFormat(IcebergFileFormat.fromNessieDataFileFormat(fileManifest.fileFormat()))
            .filePath(fileManifest.filePath())
            .content(IcebergDataContent.fromNessieFileContentType(fileManifest.content()))
            .fileSizeInBytes(fileManifest.fileSizeInBytes())
            .recordCount(fileManifest.recordCount())
            .sortOrderId(fileManifest.sortOrderId())
            .keyMetadata(fileManifest.keyMetadata());
    if (!fileManifest.equalityIds().isEmpty()) {
      // Nullable collection
      dataFile.equalityIds(fileManifest.equalityIds());
    }
    if (!fileManifest.splitOffsets().isEmpty()) {
      // Nullable collection
      dataFile.splitOffsets(fileManifest.splitOffsets());
    }
    for (NessieFieldSummary column : fileManifest.columns()) {
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
      for (int i = 0; i < fileManifest.partitionElements().size(); i++) {
        NessieFieldValue nessieFieldValue = fileManifest.partitionElements().get(i);
        IcebergType partitionFieldType = partitionSpec.fields().get(i).type(icebergSchema);
        Object value = partitionFieldType.deserializeSingleValue(nessieFieldValue.value());
        record.put(i, value);
      }
      dataFile.partition(record);
    }
    return dataFile.build();
  }

  public static NessieFileManifestGroup icebergManifestListToNessieGroup(
      SeekableInput avroListInput,
      IntFunction<NessiePartitionDefinition> lookupPartitionSpec,
      Function<IcebergManifestFile, NessieId> manifestIdProvider) {
    NessieFileManifestGroup.Builder manifestGroup = NessieFileManifestGroup.builder();
    try (IcebergManifestListReader.IcebergManifestListEntryReader listReader =
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

      while (listReader.hasNext()) {
        IcebergManifestFile manifestListEntry = listReader.next();

        NessieId manifestId = manifestIdProvider.apply(manifestListEntry);

        NessiePartitionDefinition partitionDefinition =
            lookupPartitionSpec.apply(manifestListEntry.partitionSpecId());

        NessieFileManifestGroupEntry groupEntry =
            icebergManifestFileToNessieGroupEntry(
                manifestListEntry, manifestId, partitionDefinition);

        manifestGroup.addManifest(groupEntry);
      }

      return manifestGroup.build();
    } catch (Exception e) {
      // TODO some better error handling
      throw new RuntimeException(e);
    }
  }

  public static NessieFileManifestGroupEntry icebergManifestFileToNessieGroupEntry(
      IcebergManifestFile manifestFile,
      NessieId manifestId,
      NessiePartitionDefinition partitionDefinition) {
    IcebergManifestContent entryContent = manifestFile.content();
    NessieFileContentType content =
        entryContent != null
            ? entryContent.nessieFileContentType()
            : NessieFileContentType.ICEBERG_DATA_FILE;

    NessieFileManifestGroupEntry.Builder entry =
        NessieFileManifestGroupEntry.builder()
            .manifestId(manifestId)
            .content(content)
            //
            .icebergManifestPath(manifestFile.manifestPath())
            .icebergManifestLength(manifestFile.manifestLength())
            //
            .addedSnapshotId(manifestFile.addedSnapshotId())
            .sequenceNumber(manifestFile.sequenceNumber())
            .minSequenceNumber(manifestFile.minSequenceNumber())
            .partitionSpecId(manifestFile.partitionSpecId())
            //
            .addedDataFilesCount(manifestFile.addedDataFilesCount())
            .addedRowsCount(manifestFile.addedRowsCount())
            .deletedDataFilesCount(manifestFile.deletedDataFilesCount())
            .deletedRowsCount(manifestFile.deletedRowsCount())
            .existingDataFilesCount(manifestFile.existingDataFilesCount())
            .existingRowsCount(manifestFile.existingRowsCount())
            //
            .keyMetadata(manifestFile.keyMetadata());

    int size = manifestFile.partitions().size();
    int[] fieldIds = new int[size];
    BooleanArray containsNan = new BooleanArray(size);
    BooleanArray containsNull = new BooleanArray(size);
    byte[][] lowerBound = new byte[size][];
    byte[][] upperBound = new byte[size][];
    for (int i = 0; i < size; i++) {
      IcebergPartitionFieldSummary fieldSummary = manifestFile.partitions().get(i);
      fieldIds[i] = partitionDefinition.fields().get(i).icebergId();
      containsNan.set(i, fieldSummary.containsNan());
      containsNull.set(i, fieldSummary.containsNull());
      lowerBound[i] = fieldSummary.lowerBound();
      upperBound[i] = fieldSummary.upperBound();
    }

    entry.partitions(
        NessieFieldsSummary.builder()
            .fieldIds(fieldIds)
            .containsNan(containsNan.nullIfAllElementsNull())
            .containsNull(containsNan.nullIfAllElementsNull())
            .lowerBound(allNullToNull(lowerBound))
            .upperBound(allNullToNull(upperBound))
            .build());

    return entry.build();
  }

  private static byte[][] allNullToNull(byte[][] byteArrays) {
    for (byte[] bytes : byteArrays) {
      if (bytes != null) {
        return byteArrays;
      }
    }
    return null;
  }

  public static IcebergManifestFile nessieGroupEntryToIcebergManifestFile(
      NessieFileManifestGroupEntry groupEntry, String manifestPath) {
    IcebergManifestContent content;
    switch (groupEntry.content()) {
      case ICEBERG_DATA_FILE:
        content = IcebergManifestContent.DATA;
        break;
      case ICEBERG_DELETE_FILE:
        content = IcebergManifestContent.DELETES;
        break;
      default:
        throw new IllegalArgumentException(groupEntry.content().name());
    }

    List<IcebergPartitionFieldSummary> partitions;
    NessieFieldsSummary partitionStats = groupEntry.partitions();
    if (partitionStats != null) {
      BooleanArray containsNan = partitionStats.containsNan();
      BooleanArray containsNull = partitionStats.containsNull();
      byte[][] lowerBound = partitionStats.lowerBound();
      byte[][] upperBound = partitionStats.upperBound();

      int length = partitionStats.fieldIds().length;
      partitions = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        IcebergPartitionFieldSummary.Builder partFieldStats =
            IcebergPartitionFieldSummary.builder();
        if (containsNan != null) {
          partFieldStats.containsNan(containsNan.get(i));
        }
        partFieldStats.containsNull(containsNull != null && containsNull.get(i));
        if (lowerBound != null) {
          partFieldStats.lowerBound(lowerBound[i]);
        }
        if (upperBound != null) {
          partFieldStats.upperBound(upperBound[i]);
        }
        partitions.add(partFieldStats.build());
      }
    } else {
      partitions = emptyList();
    }

    return IcebergManifestFile.builder()
        .content(content)
        //
        //                    .manifestPath(manifest.icebergManifestPath())
        // TODO the following would tweak the location of the manifest-file to Nessie
        //  Catalog.
        .manifestPath(manifestPath)
        //
        // TODO the length of the manifest file generated by the Nessie Catalog will be
        //  different from the manifest file length reported by Iceberg.
        .manifestLength(groupEntry.icebergManifestLength())
        //
        .addedSnapshotId(groupEntry.addedSnapshotId())
        .sequenceNumber(groupEntry.sequenceNumber())
        .minSequenceNumber(groupEntry.minSequenceNumber())
        .partitionSpecId(groupEntry.partitionSpecId())
        .partitions(partitions)
        //
        .addedDataFilesCount(groupEntry.addedDataFilesCount())
        .addedRowsCount(groupEntry.addedRowsCount())
        .deletedDataFilesCount(groupEntry.deletedDataFilesCount())
        .deletedRowsCount(groupEntry.deletedRowsCount())
        .existingDataFilesCount(groupEntry.existingDataFilesCount())
        .existingRowsCount(groupEntry.existingRowsCount())
        //
        .keyMetadata(groupEntry.keyMetadata())
        //
        .build();
  }

  public static void assignUUID(
      AssignUUID u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    String uuid = u.uuid();
    Preconditions.checkArgument(uuid != null, "Null entity UUID is not permitted.");
    Preconditions.checkArgument(
        uuid.equals(Objects.requireNonNull(snapshot.entity().icebergUuid()).toString()),
        "UUID mismatch: assigned: %s, new: %s",
        snapshot.entity().icebergUuid(),
        uuid);
  }

  public static void setLocation(
      SetLocation u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    // TODO: set base location in the NessieEntity?
    builder.icebergLocation(u.location());
  }

  public static void setProperties(
      SetProperties u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    builder.putAllProperties(u.updates());
  }

  public static void addPartitionSpec(
      AddPartitionSpec u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    Map<Integer, NessiePartitionField> icebergPartitionFields = new HashMap<>();
    for (NessiePartitionDefinition partitionDefinition : snapshot.partitionDefinitions()) {
      collectPartitionFields(partitionDefinition, icebergPartitionFields);
    }

    IcebergPartitionSpec newSpec = u.spec();

    Map<Integer, NessieField> icebergFields = new HashMap<>();
    for (NessieSchema schema : snapshot.schemas()) {
      collectSchemaFields(schema, icebergFields);
    }

    NessiePartitionDefinition def =
        icebergPartitionSpecToNessie(newSpec, icebergPartitionFields, icebergFields);
    builder.addPartitionDefinition(def);
  }

  public static void addSchema(
      AddSchema u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    Map<Integer, NessieField> icebergFields = new HashMap<>();
    for (NessieSchema schema : snapshot.schemas()) {
      collectSchemaFields(schema, icebergFields);
    }

    IcebergSchema schema = u.schema();
    NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);
    builder.addSchema(nessieSchema); // TODO: last column ID?
  }

  public static void addSortOrder(
      AddSortOrder u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    Map<Integer, NessieField> icebergFields = new HashMap<>();
    for (NessieSchema schema : snapshot.schemas()) {
      collectSchemaFields(schema, icebergFields);
    }

    IcebergSortOrder sortOrder = u.sortOrder();
    NessieSortDefinition sortDefinition = icebergSortOrderToNessie(sortOrder, icebergFields);

    builder.addSortDefinition(sortDefinition);
    if (snapshot.currentSortDefinition() == null) {
      builder.currentSortDefinition(sortDefinition.sortDefinitionId());
    }
  }

  public static void addSnapshot(
      AddSnapshot u, NessieTableSnapshot.Builder builder, NessieTableSnapshot snapshot) {
    IcebergSnapshot icebergSnapshot = u.snapshot();
    Integer schemaId = icebergSnapshot.schemaId();
    if (schemaId != null) {
      NessieSchema schema = snapshot.schemaByIcebergId().get(schemaId);
      builder.currentSchema(schema.id());
    }

    builder.icebergSnapshotId(icebergSnapshot.snapshotId());
    builder.icebergSnapshotSequenceNumber(icebergSnapshot.sequenceNumber());
    builder.icebergLastSequenceNumber(
        Math.max(
            safeUnbox(snapshot.icebergLastSequenceNumber(), INITIAL_SEQUENCE_NUMBER),
            safeUnbox(icebergSnapshot.sequenceNumber(), INITIAL_SEQUENCE_NUMBER)));
    builder.snapshotCreatedTimestamp(Instant.ofEpochMilli(icebergSnapshot.timestampMs()));
    builder.icebergSnapshotSummary(icebergSnapshot.summary());
    builder.icebergManifestListLocation(icebergSnapshot.manifestList());
    builder.icebergManifestFileLocations(icebergSnapshot.manifests());
  }

  public static NessieTableSnapshot updateSnapshot(
      NessieTableSnapshot snapshot, List<IcebergMetadataUpdate> updates) {
    for (IcebergMetadataUpdate u : updates) {
      NessieTableSnapshot.Builder builder = NessieTableSnapshot.builder().from(snapshot);
      u.apply(builder, snapshot);
      snapshot = builder.build();
    }

    return snapshot;
  }

  public static Content icebergMetadataToContent(
      String location, IcebergTableMetadata snapshot, String contentId) {
    return IcebergTable.of(
        location,
        snapshot.currentSnapshotId(),
        safeUnbox(snapshot.currentSchemaId(), INITIAL_SCHEMA_ID),
        safeUnbox(snapshot.defaultSpecId(), INITIAL_SPEC_ID),
        safeUnbox(snapshot.defaultSortOrderId(), INITIAL_SORT_ORDER_ID),
        contentId);
  }
}
