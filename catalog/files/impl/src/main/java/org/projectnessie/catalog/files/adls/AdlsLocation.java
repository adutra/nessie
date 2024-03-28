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
package org.projectnessie.catalog.files.adls;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Optional;

public final class AdlsLocation {
  private final String storageAccount;
  private final String container;
  private final String path;

  private AdlsLocation(String storageAccount, String container, String path) {
    this.storageAccount = requireNonNull(storageAccount);
    this.container = container;
    this.path = requireNonNull(path);
  }

  public static AdlsLocation adlsLocation(String storageAccount, String container, String path) {
    return new AdlsLocation(storageAccount, container, path);
  }

  public static AdlsLocation adlsLocation(URI location) {
    checkArgument(location != null, "Invalid location: null");
    String scheme = location.getScheme();
    checkArgument(
        "abfs".equals(scheme) || "abfss".equals(scheme), "Invalid ADLS scheme: %s", location);

    String authority = location.getAuthority();
    String[] parts = authority.split("@", -1);
    String container;
    String storageAccount;
    if (parts.length > 1) {
      container = parts[0];
      storageAccount = parts[1];
    } else {
      container = null;
      storageAccount = authority;
    }

    String path = location.getPath();
    path = path == null ? "" : path.startsWith("/") ? path.substring(1) : path;

    return new AdlsLocation(storageAccount, container, path);
  }

  public String storageAccount() {
    return storageAccount;
  }

  public Optional<String> container() {
    return Optional.ofNullable(container);
  }

  public String path() {
    return path;
  }
}
