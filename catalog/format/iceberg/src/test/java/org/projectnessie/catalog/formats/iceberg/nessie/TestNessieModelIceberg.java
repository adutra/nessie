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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.INITIAL_COLUMN_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.INITIAL_SCHEMA_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.INITIAL_SEQUENCE_NUMBER;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.INITIAL_SORT_ORDER_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.INITIAL_SPEC_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.NO_SNAPSHOT_ID;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.binaryType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.booleanType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.dateType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.decimalType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.doubleType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.fixedType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.floatType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.integerType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.listType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.mapType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.structType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timeType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestampType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestamptzType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.uuidType;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotLogEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTransform;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.locations.BaseLocation;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieFieldTransform;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieModelIceberg {
  @InjectSoftAssertions protected SoftAssertions soft;

  static <T> T icebergJsonSerializeDeserialize(T obj, Class<T> type) throws IOException {
    ObjectMapper mapper = IcebergJson.objectMapper();
    String json = mapper.writeValueAsString(obj);
    return mapper.readValue(json, type);
  }

  @ParameterizedTest
  @MethodSource
  public void transformSpec(IcebergTransform iceberg, NessieFieldTransform nessie) {
    NessieFieldTransform toNessie = NessieModelIceberg.icebergTransformToNessie(iceberg);
    soft.assertThat(toNessie).isEqualTo(nessie);

    IcebergTransform toIceberg = NessieModelIceberg.nessieTransformToIceberg(nessie);
    soft.assertThat(toIceberg).isEqualTo(iceberg);
  }

  static Stream<Arguments> transformSpec() {
    return Stream.of(
        arguments(IcebergTransform.identity(), NessieFieldTransform.identity()),
        arguments(IcebergTransform.voidTransform(), NessieFieldTransform.voidTransform()),
        arguments(IcebergTransform.bucket(1), NessieFieldTransform.bucket(1)),
        arguments(IcebergTransform.bucket(42), NessieFieldTransform.bucket(42)),
        arguments(
            IcebergTransform.unknownTransform("unknown_transform"),
            NessieFieldTransform.unknownTransform("unknown_transform")),
        arguments(IcebergTransform.hour(), NessieFieldTransform.hour()),
        arguments(IcebergTransform.day(), NessieFieldTransform.day()),
        arguments(IcebergTransform.month(), NessieFieldTransform.month()),
        arguments(IcebergTransform.year(), NessieFieldTransform.year()),
        arguments(IcebergTransform.truncate(42), NessieFieldTransform.truncate(42)),
        arguments(IcebergTransform.truncate(1), NessieFieldTransform.truncate(1)));
  }

  @ParameterizedTest
  @MethodSource("icebergTypes")
  public void icebergNessieSchema(IcebergType icebergType) throws Exception {
    IcebergSchema icebergSchema =
        IcebergSchema.builder()
            .schemaId(42)
            .type("struct")
            .addFields(
                IcebergNestedField.builder()
                    .id(123)
                    .doc("doc")
                    .type(icebergType)
                    .name("field_name")
                    .required(true)
                    .build())
            .build();
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSchema, IcebergSchema.class))
        .isEqualTo(icebergSchema);

    Map<Integer, NessieField> fieldsMap = new HashMap<>();
    Map<Integer, NessiePartitionField> partitionFieldsMap = new HashMap<>();
    NessieSchema nessieSchema =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergSchema, fieldsMap);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieSchema, NessieSchema.class))
        .isEqualTo(nessieSchema);

    IcebergSchema icebergAgain = NessieModelIceberg.nessieSchemaToIcebergSchema(nessieSchema);

    soft.assertThat(icebergAgain).isEqualTo(icebergSchema);

    NessieSchema nessieAgain =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergAgain, fieldsMap);

    soft.assertThat(nessieAgain).isEqualTo(nessieSchema);

    // partition-spec

    IcebergPartitionSpec icebergPartitionSpec =
        IcebergPartitionSpec.partitionSpec(
            42,
            singletonList(
                IcebergPartitionField.partitionField(
                    "field_part", IcebergTransform.identity().toString(), 123, 999)));
    soft.assertThat(
            icebergJsonSerializeDeserialize(icebergPartitionSpec, IcebergPartitionSpec.class))
        .isEqualTo(icebergPartitionSpec);

    NessiePartitionDefinition nessiePartitionDefinition =
        NessieModelIceberg.icebergPartitionSpecToNessie(
            icebergPartitionSpec, partitionFieldsMap, fieldsMap);
    soft.assertThat(
            icebergJsonSerializeDeserialize(
                nessiePartitionDefinition, NessiePartitionDefinition.class))
        .isEqualTo(nessiePartitionDefinition);

    IcebergPartitionSpec icebergPartitionSpecConv =
        NessieModelIceberg.nessiePartitionDefinitionToIceberg(nessiePartitionDefinition);
    soft.assertThat(icebergPartitionSpecConv).isEqualTo(icebergPartitionSpec);

    NessiePartitionDefinition nessiePartitionDefinitionAgain =
        NessieModelIceberg.icebergPartitionSpecToNessie(
            icebergPartitionSpecConv, partitionFieldsMap, fieldsMap);
    soft.assertThat(nessiePartitionDefinitionAgain).isEqualTo(nessiePartitionDefinition);

    // sort-order

    IcebergSortOrder icebergSortOrder =
        IcebergSortOrder.sortOrder(
            42,
            singletonList(
                IcebergSortField.sortField(
                    IcebergTransform.identity().toString(),
                    123,
                    IcebergSortField.ASC,
                    IcebergSortField.NULLS_FIRST)));
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSortOrder, IcebergSortOrder.class))
        .isEqualTo(icebergSortOrder);

    NessieSortDefinition nessieSortDefinition =
        NessieModelIceberg.icebergSortOrderToNessie(icebergSortOrder, fieldsMap);
    soft.assertThat(
            icebergJsonSerializeDeserialize(nessieSortDefinition, NessieSortDefinition.class))
        .isEqualTo(nessieSortDefinition);

    IcebergSortOrder icebergSortOrderConv =
        NessieModelIceberg.nessieSortDefinitionToIceberg(nessieSortDefinition);
    soft.assertThat(icebergSortOrderConv).isEqualTo(icebergSortOrder);

    NessieSortDefinition nessieSortDefinitionAgain =
        NessieModelIceberg.icebergSortOrderToNessie(icebergSortOrderConv, fieldsMap);
    soft.assertThat(nessieSortDefinitionAgain).isEqualTo(nessieSortDefinition);
  }

  @ParameterizedTest
  @MethodSource
  public void icebergTableMetadata(IcebergTableMetadata icebergTableMetadata) throws Exception {
    soft.assertThat(
            icebergJsonSerializeDeserialize(icebergTableMetadata, IcebergTableMetadata.class))
        .isEqualTo(icebergTableMetadata);

    NessieTable table =
        NessieTable.builder()
            .id(NessieId.randomNessieId())
            .createdTimestamp(Instant.now())
            .baseLocation(
                BaseLocation.baseLocation(
                    NessieId.randomNessieId(),
                    "some-location",
                    URI.create("s3://somebucket/some/path")))
            .icebergLastColumnId(INITIAL_COLUMN_ID)
            .icebergLastPartitionId(INITIAL_COLUMN_ID)
            .icebergUuid(icebergTableMetadata.tableUuid())
            .nessieContentId(UUID.randomUUID().toString())
            .tableFormat(TableFormat.ICEBERG)
            .build();

    NessieId snapshotId = NessieId.randomNessieId();

    NessieTableSnapshot nessie =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, null, table, icebergTableMetadata);
    soft.assertThat(icebergJsonSerializeDeserialize(nessie, NessieTableSnapshot.class))
        .isEqualTo(nessie);

    IcebergTableMetadata iceberg =
        NessieModelIceberg.nessieTableSnapshotToIceberg(
            nessie, Optional.empty(), NessieModelIceberg.IcebergSnapshotTweak.NOOP);
    IcebergTableMetadata icebergWithCatalogProps =
        IcebergTableMetadata.builder()
            .from(icebergTableMetadata)
            .putAllProperties(
                iceberg.properties().entrySet().stream()
                    .filter(e -> e.getKey().startsWith("nessie."))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .schema(
                icebergTableMetadata.formatVersion() > 1
                    ? null
                    : iceberg.schemas().isEmpty() ? null : iceberg.schemas().get(0))
            .build();
    soft.assertThat(iceberg).isEqualTo(icebergWithCatalogProps);

    NessieTableSnapshot nessieAgain =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, nessie, nessie.entity(), iceberg);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieAgain, NessieTableSnapshot.class))
        .isEqualTo(nessieAgain);
  }

  static Stream<IcebergTableMetadata> icebergTableMetadata() {
    IcebergSchema schemaAllTypes = icebergSchemaAllTypes();
    int snapshotId = 11;
    return Stream.of(
            // bare one
            IcebergTableMetadata.builder()
                .tableUuid(UUID.randomUUID())
                .lastUpdatedMs(111111111L)
                .location("table-location")
                .currentSnapshotId(NO_SNAPSHOT_ID)
                .lastColumnId(INITIAL_COLUMN_ID)
                .lastPartitionId(INITIAL_COLUMN_ID)
                .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
                .currentSchemaId(INITIAL_SCHEMA_ID)
                .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
                .defaultSpecId(INITIAL_SPEC_ID)
                .putProperty("prop", "value"),
            // just a schema
            IcebergTableMetadata.builder()
                .tableUuid(UUID.randomUUID())
                .lastUpdatedMs(111111111L)
                .location("table-location")
                .currentSnapshotId(NO_SNAPSHOT_ID)
                .lastColumnId(schemaAllTypes.fields().get(schemaAllTypes.fields().size() - 1).id())
                .lastPartitionId(INITIAL_COLUMN_ID)
                .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
                .currentSchemaId(schemaAllTypes.schemaId())
                .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
                .defaultSpecId(INITIAL_SPEC_ID)
                .putProperty("prop", "value")
                .addSchemas(schemaAllTypes),
            // snapshot
            IcebergTableMetadata.builder()
                .tableUuid(UUID.randomUUID())
                .lastUpdatedMs(111111111L)
                .location("table-location")
                .currentSnapshotId(snapshotId)
                .lastColumnId(schemaAllTypes.fields().get(schemaAllTypes.fields().size() - 1).id())
                .lastPartitionId(INITIAL_COLUMN_ID)
                .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
                .currentSchemaId(schemaAllTypes.schemaId())
                .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
                .defaultSpecId(INITIAL_SPEC_ID)
                .putProperty("prop", "value")
                .addSchemas(schemaAllTypes)
                .addSnapshots(
                    IcebergSnapshot.builder()
                        .snapshotId(snapshotId)
                        .schemaId(schemaAllTypes.schemaId())
                        .putSummary("operation", "testing")
                        .sequenceNumber(123L)
                        .timestampMs(12345678L)
                        .build())
                .putRef(
                    "main",
                    IcebergSnapshotRef.builder().type("branch").snapshotId(snapshotId).build())
                .addSnapshotLog(
                    IcebergSnapshotLogEntry.builder()
                        .snapshotId(snapshotId)
                        .timestampMs(12345678L)
                        .build()))
        .flatMap(
            builder ->
                Stream.of(
                    builder.formatVersion(IcebergSpec.V1.version()).build(),
                    builder.formatVersion(IcebergSpec.V2.version()).build()));
  }

  @Test
  public void icebergNessieSchemaAllTypes() throws Exception {
    IcebergSchema icebergSchema = icebergSchemaAllTypes();
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSchema, IcebergSchema.class))
        .isEqualTo(icebergSchema);

    Map<Integer, NessieField> fieldsMap = new HashMap<>();
    NessieSchema nessieSchema =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergSchema, fieldsMap);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieSchema, NessieSchema.class))
        .isEqualTo(nessieSchema);

    IcebergSchema icebergAgain = NessieModelIceberg.nessieSchemaToIcebergSchema(nessieSchema);

    soft.assertThat(icebergAgain).isEqualTo(icebergSchema);

    NessieSchema nessieAgain =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergAgain, fieldsMap);

    soft.assertThat(nessieAgain).isEqualTo(nessieSchema);
  }

  @ParameterizedTest
  @MethodSource("icebergTypes")
  public void icebergNessieTypeConversion(IcebergType icebergType) throws Exception {
    soft.assertThat(icebergJsonSerializeDeserialize(icebergType, IcebergType.class))
        .isEqualTo(icebergType);

    Map<Integer, NessieField> icebergFieldIdToField = new HashMap<>();

    NessieTypeSpec nessieType =
        NessieModelIceberg.icebergTypeToNessieType(icebergType, icebergFieldIdToField);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieType, NessieTypeSpec.class))
        .isEqualTo(nessieType);

    IcebergType icebergAgain = NessieModelIceberg.nessieTypeToIcebergType(nessieType);
    soft.assertThat(icebergAgain).isEqualTo(icebergType);
    NessieTypeSpec nessieAgain =
        NessieModelIceberg.icebergTypeToNessieType(icebergAgain, icebergFieldIdToField);
    soft.assertThat(nessieAgain).isEqualTo(nessieType);

    // Verify that generated 'NessieId's are deterministic
    soft.assertThat(nessieIdHasher("NessieTypeSpec").hash(nessieType).generate())
        .isEqualTo(nessieIdHasher("NessieTypeSpec").hash(nessieType).generate())
        .isEqualTo(nessieIdHasher("NessieTypeSpec").hash(nessieAgain).generate());
  }

  private static IcebergSchema icebergSchemaAllTypes() {
    IcebergSchema.Builder icebergSchemaBuilder =
        IcebergSchema.builder().schemaId(42).type("struct");

    int i = 0;
    for (Iterator<IcebergType> iter = icebergTypes().iterator(); iter.hasNext(); i++) {
      IcebergType icebergType = iter.next();
      icebergSchemaBuilder.addFields(
          IcebergNestedField.builder()
              .id(1001 + i)
              .doc("doc_" + i)
              .type(icebergType)
              .name("field_name_" + i)
              .required((i & 1) == 0)
              .build());
    }

    return icebergSchemaBuilder.build();
  }

  static Stream<IcebergType> icebergTypes() {
    return Stream.of(
        booleanType(),
        uuidType(),
        stringType(),
        binaryType(),
        integerType(),
        longType(),
        floatType(),
        doubleType(),
        dateType(),
        timeType(),
        structType(singletonList(nestedField(11, "field11", true, stringType(), null)), null),
        structType(singletonList(nestedField(11, "field11", false, stringType(), null)), null),
        listType(1, stringType(), true),
        listType(1, stringType(), false),
        listType(1, uuidType(), true),
        listType(1, uuidType(), false),
        mapType(3, stringType(), 4, dateType(), true),
        mapType(3, stringType(), 4, dateType(), false),
        mapType(3, uuidType(), 4, timeType(), true),
        mapType(3, uuidType(), 4, timeType(), false),
        decimalType(10, 3),
        fixedType(42),
        timestampType(),
        timestamptzType());
  }
}
