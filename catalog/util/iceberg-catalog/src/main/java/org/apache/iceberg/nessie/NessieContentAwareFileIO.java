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
package org.apache.iceberg.nessie;

import static java.lang.String.format;
import static java.net.URLEncoder.encode;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.projectnessie.catalog.iceberg.httpfileio.ReferencedCloseables;

public class NessieContentAwareFileIO implements HadoopConfigurable, DelegateFileIO, Serializable {

  // Implementation note: this class must be Serializable to allow it to be used in Spark.

  private final String contentKey;
  private DelegateFileIO delegate;
  private String reference;

  public NessieContentAwareFileIO(DelegateFileIO delegate, String contentKey) {
    this.delegate = delegate;
    this.contentKey = contentKey;
  }

  public void updateReference(URI catalogBaseUri, String reference) {
    if (!Objects.equals(this.reference, reference)) {
      Map<String, String> properties = new LinkedHashMap<>(delegate.properties());
      try {
        String p =
            format("trees/%s/sign/%s", encode(reference, "UTF-8"), encode(contentKey, "UTF-8"));
        URI signerEndpoint = catalogBaseUri.resolve(p);
        properties.put(S3V4RestSignerClient.S3_SIGNER_ENDPOINT, signerEndpoint.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      Configuration conf =
          delegate instanceof HadoopConfigurable ? ((HadoopConfigurable) delegate).getConf() : null;
      delegate.close();
      delegate =
          (DelegateFileIO) CatalogUtil.loadFileIO(delegate.getClass().getName(), properties, conf);
      ReferencedCloseables.addCloseable(this, delegate);
    }
    this.reference = reference;
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate.newInputFile(path, length);
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
    DelegateFileIO delegate = this.delegate;
    if (delegate != null) {
      delegate.close();
    }
    this.delegate = null;
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
