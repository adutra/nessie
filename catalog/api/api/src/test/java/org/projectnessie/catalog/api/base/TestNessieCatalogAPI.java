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
package org.projectnessie.catalog.api.base;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.model.NessieCatalog;

public class TestNessieCatalogAPI {
  @Test
  public void foo() {
    NessieCatalogAPI api =
        NessieCatalogAPI.builder()
            .withProperties(
                Map.of(
                    "nessie.catalog.uri",
                    "http://127.0.0.1:42",
                    "nessie.catalog.transport.name",
                    "config-test",
                    "nessie.catalog.configtest.a.a1",
                    "foo"),
                "test-config")
            .build();
    NessieCatalog catalog = api.connect();
    System.err.println(catalog);
  }
}
