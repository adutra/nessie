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
package org.projectnessie.catalog.formats.iceberg.rest;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

public class IcebergOAuthTokenEndpointException extends IcebergRestBaseException {

  public IcebergOAuthTokenEndpointException(int code, String error, String errorDescription) {
    super(code, error, errorDescription);
  }

  public Map<String, String> getDetails() {
    Map<String, String> r = new HashMap<>();
    r.put("error", getType());
    r.put("error_description", getMessage());
    return unmodifiableMap(r);
  }
}
