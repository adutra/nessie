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
package org.projectnessie.catalog.iceberg.httpfileio;

import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quick-n-dirty implementation of "resolving file IO" that can read files via http and otherwise
 * defers to {@link ResolvingFileIO}.
 *
 * <p>Ideally Iceberg's {@link ResolvingFileIO} should use Java's {@linkplain
 * java.util.ServiceLoader service loading mechanism} to make {@linkplain DelegateFileIO file IO
 * implementations} pluggable.
 */
public class HttpFileIO implements HadoopConfigurable, DelegateFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(HttpFileIO.class);

  private final DelegateFileIO delegate;

  public HttpFileIO() {
    // TODO make the delegate configurable - requires changes to the methods implementing
    //  HadoopConfigurable
    this(new ResolvingFileIO());
  }

  public HttpFileIO(DelegateFileIO delegate) {
    super();
    this.delegate = delegate;
  }

  private static boolean isHttp(String path) {
    return path.startsWith("http://") || path.startsWith("https://");
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

  @Override
  public void initialize(Map<String, String> properties) {
    LOG.info("Initializing HttpFileIO");
    delegate.initialize(properties);
  }

  @Override
  public InputFile newInputFile(String path) {
    LOG.info("newInputFile {}", path);
    if (isHttp(path)) {
      return openHttpInputFile(path);
    }
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    LOG.info("newInputFile {} {}", path, length);
    if (isHttp(path)) {
      return openHttpInputFile(path);
    }
    return delegate.newInputFile(path, length);
  }

  private HttpInputFile openHttpInputFile(String path) {
    return new HttpInputFile(path, properties().get("nessie.http.file.spill-dir"));
  }

  private static void failForHttp(String path) {
    if (isHttp(path)) {
      failForHttp();
    }
  }

  private static void failForHttp() {
    throw new UnsupportedOperationException("Operation not supported for http/https");
  }

  @Override
  public OutputFile newOutputFile(String path) {
    LOG.info("newOutputFile {}", path);
    failForHttp(path);
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    LOG.info("deleteFile {}", path, new Exception());
    failForHttp(path);
    delegate.deleteFile(path);
  }

  @Override
  public void deleteFile(InputFile file) {
    LOG.info("deleteFile {}", file.location(), new Exception());
    failForHttp();
    delegate.deleteFile(file);
  }

  @Override
  public void deleteFile(OutputFile file) {
    LOG.info("deleteFile {}", file.location(), new Exception());
    failForHttp();
    delegate.deleteFile(file);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Iterable<FileInfo> listPrefix(String path) {
    LOG.info("listPrefix {}", path);
    failForHttp(path);
    return delegate.listPrefix(path);
  }

  @Override
  public void deletePrefix(String path) {
    LOG.info("deletePrefix {}", path);
    failForHttp(path);
    delegate.deletePrefix(path);
  }

  @Override
  public void deleteFiles(Iterable<String> iterable) throws BulkDeletionFailureException {
    LOG.info("deleteFiles");
    delegate.deleteFiles(iterable);
  }
}
