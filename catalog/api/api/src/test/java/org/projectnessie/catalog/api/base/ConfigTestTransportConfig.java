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

import io.smallrye.config.ConfigMapping;
import java.util.Optional;

@ConfigMapping(prefix = "nessie.catalog.configtest")
@SuppressWarnings("MethodName")
public interface ConfigTestTransportConfig {
  Optional<A> a();

  Optional<B> b();

  interface A {
    String a1();

    Optional<String> a2();

    String toString();
  }

  interface B {
    String b1();

    String b2();

    String toString();
  }

  String toString();
}
