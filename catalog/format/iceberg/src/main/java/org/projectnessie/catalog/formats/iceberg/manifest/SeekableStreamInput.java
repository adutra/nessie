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
package org.projectnessie.catalog.formats.iceberg.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SeekableStreamInput implements SeekableInput {
  private static final Logger LOGGER = LoggerFactory.getLogger(SeekableStreamInput.class);
  private final URI uri;
  private final Path tempFile;
  private final SeekableFileInput seekableFileInput;
  private boolean closed;

  @FunctionalInterface
  public interface SourceProvider {
    InputStream open(URI uri) throws IOException;
  }

  public SeekableStreamInput(URI uri, SourceProvider source) throws IOException {
    this.uri = uri;
    this.tempFile = Files.createTempFile("manifest-list-temp-", ".avro");
    // TODO Need some tooling to create an Avro `SeekableInput` from an `InputStream` without
    //  copying it to a temporary file. Something like org.apache.iceberg.aws.s3.S3InputStream ?
    try (InputStream inputStream = source.open(uri)) {
      Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
    }
    this.seekableFileInput = new SeekableFileInput(tempFile.toFile());
  }

  @Override
  public void seek(long p) throws IOException {
    LOGGER.info("seek({}) for {}", p, uri);
    seekableFileInput.seek(p);
  }

  @Override
  public long tell() throws IOException {
    LOGGER.info("tell() for {}", uri);
    return seekableFileInput.tell();
  }

  @Override
  public long length() throws IOException {
    LOGGER.info("length() for {}", uri);
    return seekableFileInput.length();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    LOGGER.info("read({}, {}, {}) for {}", b.length, off, len, uri);
    return seekableFileInput.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    LOGGER.info("close() for {}", uri);
    try {
      seekableFileInput.close();
    } finally {
      closed = true;
      Files.deleteIfExists(tempFile);
    }
  }
}
