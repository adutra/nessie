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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HttpSeekableInputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(HttpSeekableInputStream.class);

  private final StreamSupplier streamSupplier;
  private final StackTraceElement[] createStack;
  private final String location;

  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private final Counter readBytes;
  private final Counter readOperations;

  @FunctionalInterface
  interface StreamSupplier {
    InputStream open() throws IOException;
  }

  HttpSeekableInputStream(String location, StreamSupplier streamSupplier) {
    this(location, streamSupplier, MetricsContext.nullMetrics());
  }

  HttpSeekableInputStream(String location, StreamSupplier streamSupplier, MetricsContext metrics) {
    this.location = location;
    this.streamSupplier = streamSupplier;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return stream.read();
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read(bytes, off, len);
    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  // TODO implement RangeReadable
  //
  //  @Override
  //  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException
  // {
  //    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);
  //
  //    String range = String.format("bytes=%s-%s", position, position + length - 1);
  //
  //    IOUtil.readFully(readRange(range), buffer, offset, length);
  //  }
  //
  //  @Override
  //  public int readTail(byte[] buffer, int offset, int length) throws IOException {
  //    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);
  //
  //    String range = String.format("bytes=-%s", length);
  //
  //    return IOUtil.readRemaining(readRange(range), buffer, offset, length);
  //  }
  //
  //  private InputStream readRange(String range) {
  //    GetObjectRequest.Builder requestBuilder =
  //      GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).range(range);
  //
  //    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);
  //
  //    return s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
  //  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream();
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      skipBytes(skip);
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", location, next);
    pos = next;
    openStream();
  }

  private void skipBytes(long skip) {
    if (skip <= 0L) {
      return;
    }
    // already buffered or seek is small enough
    LOG.debug("Read-through seek for {} to offset {}", location, next);
    try {
      ByteStreams.skipFully(stream, skip);
      pos += skip;
    } catch (IOException ignored) {
      // will retry by re-opening the stream
    }
  }

  private void openStream() throws IOException {
    closeStream();

    try {
      stream = streamSupplier.open();
      skipBytes(next);
    } catch (IOException e) {
      throw new NotFoundException(e, "Location does not exist: %s", location);
    }
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      // if we aren't at the end of the stream, and the stream is abortable, then
      // call abort() so we don't read the remaining data with the Apache HTTP client
      // TODO abortStream(); ?
      try {
        stream.close();
      } catch (IOException e) {
        // the Apache HTTP client will throw a ConnectionClosedException
        // when closing an aborted stream, which is expected
        if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
          throw e;
        }
      }
      stream = null;
    }
  }

  //  private void abortStream() {
  //    try {
  //      if (stream instanceof Abortable && stream.read() != -1) {
  //        ((Abortable) stream).abort();
  //      }
  //    } catch (Exception e) {
  //      LOG.warn("An error occurred while aborting the stream", e);
  //    }
  //  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "removal"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
