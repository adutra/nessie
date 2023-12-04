/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.cassandra;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Synchronization helper for asynchronous "child" queries for {@link CassandraPersist#erase()},
 * {@link CassandraPersist#deleteObjs(ObjId[])}, {@link CassandraPersist#storeObjs(Obj[])}.
 *
 * <p>Note: this implementation does not actively prevent submitting a new query, but it prevents
 * making further progress by blocking inside {@link #submitted(CompletionStage)}.
 */
final class LimitedConcurrentRequests implements AutoCloseable {

  /** Currently available "permits" for child queries. */
  final Semaphore permits;

  /** Holds the potential failure. */
  final Throwable[] failureHolder = new Throwable[1];

  /** Number of started queries. */
  @GuardedBy("this")
  int started;

  /** Number of finished queries. */
  @GuardedBy("this")
  int finished;

  LimitedConcurrentRequests(int maxChildQueries) {
    permits = new Semaphore(maxChildQueries);
  }

  void submitted(CompletionStage<?> cs) {
    synchronized (this) {
      // Increment the number of started queries.
      started++;
    }

    // Acquire a permit for the started query.
    try {
      permits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    cs.whenComplete(
        (resultSet, throwable) -> {
          try {
            // Release the acquired permit
            permits.release();

            // Record the failure (if the query failed)
            if (throwable != null) {
              synchronized (failureHolder) {
                Throwable ex = failureHolder[0];
                if (ex == null) {
                  failureHolder[0] = throwable;
                } else {
                  ex.addSuppressed(throwable);
                }
              }
            }

          } finally {
            synchronized (this) {
              // Increment the number of finished queries.
              finished++;
              // Notify potential waiter (`close()`).
              notify();
            }
          }
        });
  }

  @Override
  public void close() {
    try {
      // Wait until all started queries have finished.
      while (true) {
        synchronized (this) {
          if (finished == started) {
            break;
          }
          try {
            wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } finally {
      maybeThrow();
    }
  }

  private void maybeThrow() {
    synchronized (failureHolder) {
      Throwable f = failureHolder[0];
      if (f != null) {
        if (f instanceof RuntimeException) {
          throw (RuntimeException) f;
        }
        throw new RuntimeException(f);
      }
    }
  }
}
