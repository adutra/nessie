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
package org.projectnessie.catalog.files.local;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import org.projectnessie.catalog.files.api.ObjectIO;

public class LocalObjectIO implements ObjectIO {
  @Override
  public InputStream readObject(URI uri) throws IOException {
    // Awesome implementation of a local object IO :D
    if (uri.getScheme() == null) {
      uri = Paths.get(uri.getPath()).toUri();
    }
    return uri.toURL().openStream();
  }
}
