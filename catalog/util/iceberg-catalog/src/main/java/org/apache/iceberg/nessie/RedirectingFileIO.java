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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.model.ContentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedirectingFileIO implements HadoopConfigurable, DelegateFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(RedirectingFileIO.class);

  private final DelegateFileIO delegate;
  private final String contentKey;
  private final transient NessieIcebergClient client;

  public RedirectingFileIO(
      DelegateFileIO delegate, ContentKey contentKey, NessieIcebergClient client) {
    this.delegate = delegate;
    this.contentKey = contentKey.toPathString();
    this.client = client;
  }

  @Override
  public InputFile newInputFile(String path) {
    path = maybeRedirectPath(path);
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    path = maybeRedirectPath(path);
    return delegate.newInputFile(path, length);
  }

  private static final Pattern MANIFEST_LIST_PATTERN =
      Pattern.compile("^.*/snap-[0-9]+-[0-9]+-[0-9a-z-]+[.]avro$");
  private static final Pattern MANIFEST_FILE_PATTERN =
      Pattern.compile("^.*/[0-9a-z-]+-m[0-9]+[.]avro$");

  String maybeRedirectPath(String path) {
    if (MANIFEST_LIST_PATTERN.matcher(path).matches()) {
      // Iceberg produces snapshots/manifest-lists/manifests using optimistic concurrency.
      // If something goes wrong during one attempt, it retries. This can include rewriting the
      // manifest-list. When `SnapshotProducer.commit()` completes, it deletes all written files,
      // that
      // are not referenced via TableMetadata's Snapshot.manifestList(). This means, if we tweak the
      // TableMetadata/Snapshot.manifestList attribute to directly point to the Nessie Catalog
      // server
      // (aka return a different path), Iceberg WILL DELETE the manifest-list and manifest files
      // during
      // the commit. The following code translates the original path to the path used to fetch the
      // manifest-list from Nessie Catalog.
      //
      // See `org.apache.iceberg.SnapshotProducer.commit()`

      String ref = client.getRef().getReference().toPathString();

      NessieApiV1 api = client.getApi();
      HttpClient httpClient =
          api.unwrapClient(HttpClient.class)
              .orElseThrow(() -> new IllegalArgumentException("Nessie client must use HTTP"));

      URI baseUri = httpClient.getBaseUri();
      String pre =
          baseUri.getPath().endsWith("/") ? "../../catalog/v1/trees/" : "../catalog/v1/trees/";

      String mapped = baseUri.resolve(pre + ref + "/manifest-list/" + contentKey).toString();

      LOG.info("Redirecting manifest-list from {} to {}", path, mapped);

      path = mapped;
    }

    // TODO the following code would translate manifest-file paths to fetch those from Nessie
    //  Catalog - but that's not a good idea (traffic volume).
    //
    //    if (MANIFEST_FILE_PATTERN.matcher(path).matches()) {
    //      String ref = client.getRef().getReference().toPathString();
    //
    //      NessieApiV1 api = client.getApi();
    //      HttpClient httpClient =
    //          api.unwrapClient(HttpClient.class)
    //              .orElseThrow(() -> new IllegalArgumentException("Nessie client must use HTTP"));
    //
    //      URI baseUri = httpClient.getBaseUri();
    //      String pre =
    //          baseUri.getPath().endsWith("/") ? "../../catalog/v1/trees/" :
    // "../catalog/v1/trees/";
    //
    //      String mapped =
    //          baseUri
    //              .resolve(
    //                  pre + ref + "/manifest-file/" + contentKey + "?manifest-file=" +
    // hashPath(path))
    //              .toString();
    //
    //      LOG.info("Redirecting manifest-file from {} to {}", path, mapped);
    //
    //      path = mapped;
    //    }

    return path;
  }

  static String hashPath(String path) {
    HashCode hash = Hashing.sha256().newHasher().putString(path, UTF_8).hash();
    String hashBase64 = Base64.getEncoder().encodeToString(hash.asBytes());
    return hashBase64;
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public void deleteFile(InputFile file) {
    delegate.deleteFile(file);
  }

  @Override
  public void deleteFile(OutputFile file) {
    delegate.deleteFile(file);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return delegate.listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    delegate.deletePrefix(prefix);
  }

  @Override
  public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    delegate.deleteFiles(pathsToDelete);
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> function) {
    if (delegate instanceof HadoopConfigurable) {
      ((HadoopConfigurable) delegate).serializeConfWith(function);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (delegate instanceof HadoopConfigurable) {
      ((HadoopConfigurable) delegate).setConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    if (delegate instanceof HadoopConfigurable) {
      return ((HadoopConfigurable) delegate).getConf();
    }
    throw new UnsupportedOperationException("oops");
  }
}
