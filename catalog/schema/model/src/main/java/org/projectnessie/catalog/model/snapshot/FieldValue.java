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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;

@JsonSerialize(using = FieldValue.FieldValueSerializer.class)
@JsonDeserialize(using = FieldValue.FieldValueDeserializer.class)
public abstract class FieldValue {
  // TODO The FieldValue type is intended to allow type-specific serialization in all needed
  //  serialization formats, needs specific subtypes and custom (de)serialization code.
  //  - JSON: an integer as a JSON integer value
  //  - JSON: a boolean as a JSON integer value
  //  - Avro: ints/floats/bool/bin/string using the native types
  //  - Parquet: ints/floats/bool/bin/string using the native types

  // TODO add efficient (de)serailization helpers for Avro + Parquet, i.e. without an intermediate
  //  Java representation

  // Delta: serialization format:
  //   https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization

  // Iceberg: serialization format:
  //   https://iceberg.apache.org/spec/#appendix-d-single-value-serialization

  abstract void jacksonSerialize(JsonGenerator gen) throws IOException;

  public <T> T convertToJava(NessieTypeSpec type) {
    throw new UnsupportedOperationException("IMPLEMENT FOR " + this.getClass());
  }

  public static FieldValue fieldValueNull() {
    return FieldValueNull.INSTANCE;
  }

  public static FieldValue fieldValueTrue() {
    return FieldValueBool.INSTANCE_TRUE;
  }

  public static FieldValue fieldValueFalse() {
    return FieldValueBool.INSTANCE_FALSE;
  }

  public static FieldValue fieldValueString(String value) {
    return new FieldValueString(value);
  }

  public static FieldValue fieldValueInt(long value) {
    return new FieldValueInt(value);
  }

  public static FieldValue fieldValueFloat(double value) {
    return new FieldValueFloat(value);
  }

  static final class FieldValueNull extends FieldValue {
    public static final FieldValue INSTANCE = new FieldValueNull();

    private FieldValueNull() {}

    @Override
    void jacksonSerialize(JsonGenerator gen) throws IOException {
      gen.writeNull();
    }

    @Override
    public String toString() {
      return "(null)";
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof FieldValueNull;
    }
  }

  static final class FieldValueBool extends FieldValue {
    public static final FieldValue INSTANCE_TRUE = new FieldValueBool(true);
    public static final FieldValue INSTANCE_FALSE = new FieldValueBool(false);

    private final boolean value;

    private FieldValueBool(boolean value) {
      this.value = value;
    }

    @Override
    void jacksonSerialize(JsonGenerator gen) throws IOException {
      gen.writeBoolean(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FieldValueBool that = (FieldValueBool) o;

      return value == that.value;
    }

    @Override
    public int hashCode() {
      return (value ? 1 : 0);
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  static final class FieldValueString extends FieldValue {
    private final String value;

    FieldValueString(String value) {
      this.value = value;
    }

    @Override
    void jacksonSerialize(JsonGenerator gen) throws IOException {
      gen.writeString(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FieldValueString that = (FieldValueString) o;

      return value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public String toString() {
      return "'" + value + '\'';
    }
  }

  static final class FieldValueFloat extends FieldValue {
    private final double value;

    FieldValueFloat(double value) {
      this.value = value;
    }

    @Override
    void jacksonSerialize(JsonGenerator gen) throws IOException {
      gen.writeNumber(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FieldValueFloat that = (FieldValueFloat) o;

      return Double.compare(value, that.value) == 0;
    }

    @Override
    public int hashCode() {
      long temp = Double.doubleToLongBits(value);
      return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  static final class FieldValueInt extends FieldValue {
    private final long value;

    FieldValueInt(long value) {
      this.value = value;
    }

    @Override
    void jacksonSerialize(JsonGenerator gen) throws IOException {
      gen.writeNumber(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      FieldValueInt that = (FieldValueInt) o;

      return value == that.value;
    }

    @Override
    public int hashCode() {
      return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  static final class FieldValueSerializer extends JsonSerializer<FieldValue> {
    @Override
    public void serialize(FieldValue value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      value.jacksonSerialize(gen);
    }
  }

  static final class FieldValueDeserializer extends JsonDeserializer<FieldValue> {
    @Override
    public FieldValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      switch (p.currentToken()) {
        case VALUE_NULL:
          return fieldValueNull();
        case VALUE_FALSE:
          return fieldValueFalse();
        case VALUE_TRUE:
          return fieldValueTrue();
        case VALUE_STRING:
          return fieldValueString(p.getText());
        case VALUE_NUMBER_FLOAT:
          return fieldValueFloat(p.getDoubleValue());
        case VALUE_NUMBER_INT:
          return fieldValueInt(p.getLongValue());
        default:
          throw new IllegalArgumentException("Unexpected token " + p.currentToken());
      }
    }
  }
}
