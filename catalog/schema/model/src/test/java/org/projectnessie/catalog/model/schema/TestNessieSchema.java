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
package org.projectnessie.catalog.model.schema;

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.types.NessieType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieSchema {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void allTypes() throws Exception {
    NessieStruct.Builder struct = NessieStruct.builder();

    Function<NessieField.Builder, Stream<NessieField>> nullNotNull =
        builder -> {
          NessieField notNull = builder.nullable(true).build();
          return Stream.of(
              notNull,
              NessieField.builder()
                  .from(notNull)
                  .name(notNull.name() + " not null")
                  .documentation(notNull.documentation() + " not null")
                  .nullable(false)
                  .build());
        };

    AtomicInteger columnId = new AtomicInteger();
    Stream.concat(
            NessieType.primitiveTypes().stream(),
            Stream.of(
                NessieType.fixedType(666),
                NessieType.timeType(3, false),
                NessieType.timeType(3, true),
                NessieType.timeType(9, false),
                NessieType.timeType(9, true),
                NessieType.timestampType(3, false),
                NessieType.timestampType(3, true),
                NessieType.timestampType(9, false),
                NessieType.timestampType(9, true)))
        .flatMap(
            primitiveType ->
                nullNotNull.apply(
                    NessieField.builder()
                        .name(primitiveType.asString())
                        .fieldId(NessieId.randomNessieId())
                        .type(primitiveType)
                        .documentation("doc for " + primitiveType.asString())
                        .icebergColumnId(columnId.incrementAndGet())))
        .forEach(struct::addFields);

    NessieId schemaId = NessieId.randomNessieId();
    NessieSchema schema = NessieSchema.nessieSchema(schemaId, struct.build(), 42, emptyList());

    ObjectMapper mapper =
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).findAndRegisterModules();
    String json = mapper.writeValueAsString(schema);
    NessieSchema deserialized = mapper.readValue(json, NessieSchema.class);

    soft.assertThat(deserialized.struct().fields())
        .containsExactlyElementsOf(schema.struct().fields());
    soft.assertThat(deserialized).isEqualTo(schema);
    soft.assertThat(mapper.writeValueAsString(deserialized)).isEqualTo(json);
  }
}
