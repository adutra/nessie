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
package org.projectnessie.catalog.iceberg.httpfileio;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to close {@link AutoCloseable}s when a referenced object is no longer reachable.
 * This class is useful when no proper lifecycle ensuring that a resource is properly closed is
 * possible, and it avoids the issues using {@link Object#finalize()}.
 *
 * <p>The implementation does <em>not</em> ensure that the enqueued {@link AutoCloseable}s will be
 * eventually called when the JVM shuts down. It does ensure that the enqueued {@link
 * AutoCloseable}s do get called when the referent is no longer reachable according to the
 * specification of a {@linkplain java.lang.ref phantom reference}.
 *
 * <p>The Java {@linkplain ReferenceQueue reference queue} used by this implementation is polled
 * regularly. Polling happens using an executor service that allows all threads to terminate to
 * prevent memory leaks via class loaders.
 */
public final class ReferencedCloseables {
  public static void addCloseable(Object referent, AutoCloseable closeable) {
    ReferencedCloseablesLazy.INSTANCE.addCloseable(referent, closeable);
  }

  @VisibleForTesting
  static final class ReferencedCloseablesLazy {
    static final ReferencedCloseablesImpl INSTANCE = new ReferencedCloseablesImpl();
  }

  @VisibleForTesting
  static final class ReferencedCloseablesImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReferencedCloseablesImpl.class);

    private final ReferenceQueue<Object> referents = new ReferenceQueue<>();
    private final Map<Reference<Object>, AutoCloseable> referencedCloseable = new HashMap<>();

    private final Lock lock = new ReentrantLock();
    private final ScheduledThreadPoolExecutor executor;
    private ScheduledFuture<?> referenceHandling;

    ReferencedCloseablesImpl() {
      ScheduledThreadPoolExecutor pool =
          new ScheduledThreadPoolExecutor(
              0,
              new ThreadFactory() {
                private final String namePrefix =
                    "obj-refs-cleaner-"
                        + ThreadLocalRandom.current().nextLong(1_000_000, 10_000_000)
                        + "-";
                private final AtomicInteger cnt = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                  return new Thread(r, namePrefix + cnt.incrementAndGet());
                }
              });
      pool.allowCoreThreadTimeOut(true);
      pool.setKeepAliveTime(100, MILLISECONDS);
      executor = pool;
    }

    void addCloseable(Object referent, AutoCloseable closeable) {
      Lock l = lock;
      l.lock();
      try {
        Reference<Object> ref = new PhantomReference<>(referent, referents);
        referencedCloseable.put(ref, closeable);

        ScheduledFuture<?> future = referenceHandling;
        if (future == null || future.isDone() || future.isCancelled()) {
          schedule();
        }
      } finally {
        l.unlock();
      }
    }

    private void handleReferences() {
      List<AutoCloseable> closeList = new ArrayList<>();
      lock.lock();
      try {
        while (true) {
          Reference<?> unref = referents.poll();
          if (unref == null) {
            break;
          }
          AutoCloseable closeable = referencedCloseable.remove(unref);
          if (closeable != null) {
            closeList.add(closeable);
          }
        }

        if (!referencedCloseable.isEmpty()) {
          schedule();
        }
      } finally {
        lock.unlock();
      }

      // Perform actual .close() outside the lock.
      for (AutoCloseable closeable : closeList) {
        try {
          LOGGER.trace("Closing a {}", closeable.getClass().getName());
          closeable.close();
        } catch (Throwable t) {
          LOGGER.error("Failed to closing a {}", closeable.getClass().getName(), t);
        }
      }
    }

    private void schedule() {
      referenceHandling = executor.schedule(this::handleReferences, 100, MILLISECONDS);
    }
  }
}
