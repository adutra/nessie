/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.objectstoragemock.gcs;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableErrorResponse.class)
@JsonDeserialize(as = ImmutableErrorResponse.class)
@Value.Immutable
public interface ErrorResponse {
  int code();

  String message();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  JsonNode error();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  ErrorDetails errors();

  @JsonSerialize(as = ImmutableErrorDetails.class)
  @JsonDeserialize(as = ImmutableErrorDetails.class)
  @Value.Immutable
  @Value.Style(
      defaults = @Value.Immutable(lazyhash = true),
      allParameters = true,
      forceJacksonPropertyNames = false,
      clearBuilder = true,
      depluralize = true,
      jdkOnly = true,
      get = {"get*", "is*"})
  interface ErrorDetails {

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String domain();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String location();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String locationType();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String message();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String reason();
  }
}
