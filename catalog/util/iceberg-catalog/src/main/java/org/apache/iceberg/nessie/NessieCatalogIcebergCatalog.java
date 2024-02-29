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
package org.apache.iceberg.nessie;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTSerializers;
import org.projectnessie.catalog.iceberg.httpfileio.HttpFileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieCatalogIcebergCatalog extends NessieCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(NessieCatalogIcebergCatalog.class);

  private HttpClient httpClient;

  // TODO 'fileIO' field is private in NessieCatalog
  private FileIO fileIO;
  // TODO 'client' field is private in NessieCatalog
  private NessieIcebergClient client;
  private boolean sendUpdatesToServer;

  @Override
  public void initialize(String name, Map<String, String> options) {
    super.initialize(name, withDefaultOptions(options));
  }

  @Override
  public void initialize(
      String name, NessieIcebergClient client, FileIO fileIO, Map<String, String> catalogOptions) {
    NessieApiV1 api = client.getApi();

    this.httpClient =
        api.unwrapClient(HttpClient.class)
            .orElseThrow(() -> new IllegalArgumentException("Nessie client must use HTTP"));

    try {
      // TODO: make this configurable via public Nessie client API
      Field configField = httpClient.getClass().getDeclaredField("config");
      configField.setAccessible(true);
      HttpRuntimeConfig config = (HttpRuntimeConfig) configField.get(httpClient);
      ObjectMapper mapper = config.getMapper();

      RESTSerializers.registerAll(mapper); // for MetadataUpdate objects

    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    fileIO = new HttpFileIO((DelegateFileIO) fileIO);

    this.client = client;
    this.fileIO = fileIO;

    super.initialize(name, client, fileIO, catalogOptions);

    this.sendUpdatesToServer =
        Boolean.parseBoolean(catalogOptions.getOrDefault("send-updates-to-server", "true"));
  }

  private Map<String, String> withDefaultOptions(Map<String, String> options) {
    HashMap<String, String> result = new HashMap<>();
    result.put("io-impl", "org.apache.iceberg.io.ResolvingFileIO");
    result.putAll(options);
    return result;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    // TODO this is mostly a copy from NessieCatalog - we need to override some methods via
    //  NessieCatalogTableOperations
    TableReference tr = parseTableReference(tableIdentifier);
    ContentKey contentKey =
        ContentKey.of(Namespace.of(tableIdentifier.namespace().levels()), tr.getName());
    return new NessieCatalogTableOperations(
        contentKey,
        client.withReference(tr.getReference(), tr.getHash()),
        new RedirectingFileIO((DelegateFileIO) fileIO, contentKey, client),
        properties(),
        sendUpdatesToServer);
  }

  private TableReference parseTableReference(TableIdentifier tableIdentifier) {
    // TODO this is an exact copy from NessieCatalog (it's private there)
    TableReference tr = TableReference.parse(tableIdentifier.name());
    Preconditions.checkArgument(
        !tr.hasTimestamp(),
        "Invalid table name: # is only allowed for hashes (reference by "
            + "timestamp is not supported)");
    return tr;
  }
}
