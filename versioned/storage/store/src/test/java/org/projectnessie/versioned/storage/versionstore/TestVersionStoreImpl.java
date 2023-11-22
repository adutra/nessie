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
package org.projectnessie.versioned.storage.versionstore;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_RETRIES;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.AbstractVersionStoreTests;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;

public class TestVersionStoreImpl extends AbstractVersionStoreTests {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Override
  protected VersionStore store() {
    return ValidatingVersionStoreImpl.of(soft, persist);
  }

  @Test
  public void commitWithInfiniteConcurrentConflict(
      @NessieStoreConfig(name = CONFIG_COMMIT_RETRIES, value = "3")
          @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "999999999")
          @NessiePersist
          Persist persist)
      throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty()).getHash();

    AtomicInteger intercepted = new AtomicInteger();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @jakarta.annotation.Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull @jakarta.annotation.Nonnull Reference reference,
              @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
              throws RefNotFoundException, RefConditionFailedException {

            int num = intercepted.incrementAndGet();
            // Intercept the reference-pointer-bump and inject a concurrent commit
            // here
            try {
              store.commit(
                  branch,
                  Optional.of(branch1),
                  fromMessage("conflicting pointer bump"),
                  singletonList(
                      Put.of(
                          ContentKey.of("other-key-" + num),
                          IcebergTable.of("meta", 42, 43, 44, 45))));
            } catch (ReferenceNotFoundException | ReferenceConflictException e) {
              throw new RuntimeException(e);
            }

            return super.updateReferencePointer(reference, newPointer);
          }
        };

    VersionStore storeTested = new VersionStoreImpl(tested);
    assertThatThrownBy(
            () ->
                storeTested.commit(
                    branch,
                    Optional.of(branch1),
                    fromMessage("commit foo"),
                    singletonList(
                        Put.of(
                            ContentKey.of("some-key"), IcebergTable.of("meta", 42, 43, 44, 45)))))
        .isInstanceOf(ReferenceRetryFailureException.class)
        .hasMessageStartingWith(
            "The commit operation could not be performed after 3 retries within the configured commit timeout after ");
  }

  @Test
  public void commitWithSingleConcurrentConflict() throws Exception {
    VersionStore store = new VersionStoreImpl(persist);

    BranchName branch = BranchName.of("branch1");
    Hash branch1 = store.create(branch, Optional.empty()).getHash();

    AtomicBoolean intercepted = new AtomicBoolean();

    Persist tested =
        new PersistDelegate(persist) {
          @Nonnull
          @jakarta.annotation.Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull @jakarta.annotation.Nonnull Reference reference,
              @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
              throws RefNotFoundException, RefConditionFailedException {

            if (intercepted.compareAndSet(false, true)) {
              // Intercept the reference-pointer-bump and inject a concurrent commit
              // here
              try {
                store.commit(
                    branch,
                    Optional.of(branch1),
                    fromMessage("conflicting pointer bump"),
                    singletonList(
                        Put.of(
                            ContentKey.of("other-key"), IcebergTable.of("meta", 42, 43, 44, 45))));
              } catch (ReferenceNotFoundException | ReferenceConflictException e) {
                throw new RuntimeException(e);
              }
            }

            return super.updateReferencePointer(reference, newPointer);
          }
        };

    VersionStore storeTested = new VersionStoreImpl(tested);
    storeTested.commit(
        branch,
        Optional.of(branch1),
        fromMessage("commit foo"),
        singletonList(Put.of(ContentKey.of("some-key"), IcebergTable.of("meta", 42, 43, 44, 45))));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64})
  void hashCollision(int parallelism) throws Exception {
    BranchName branch = BranchName.of("main");
    ContentKey key = ContentKey.of("table");

    Set<ObjId> ids = Collections.newSetFromMap(new ConcurrentHashMap<>());

    Persist tested =
        new PersistDelegate(persist) {
          @Override
          public void storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
              throws ObjTooLargeException {
            super.storeObjs(objs);
            for (Obj obj : objs) {
              if (obj instanceof CommitObj && !ids.add(obj.id())) {
                throw new IllegalStateException("duplicate id " + obj);
              }
            }
          }

          @Override
          public void storeObj(@Nonnull @jakarta.annotation.Nonnull Obj obj)
              throws ObjTooLargeException {
            super.storeObj(obj);
            if (obj instanceof CommitObj && !ids.add(obj.id())) {
              throw new IllegalStateException("duplicate id " + obj);
            }
          }
        };

    VersionStore store = new VersionStoreImpl(tested);

    store.commit(
        branch,
        Optional.empty(),
        CommitMeta.fromMessage("initial commit"),
        singletonList(Put.of(key, newOnRef("some value"))),
        v -> {},
        (k, c) -> {});

    ContentResult value = store.getValue(branch, key);
    List<Operation> operations = singletonList(Put.of(key, value.content()));
    CommitValidator validator = v -> {};
    BiConsumer<ContentKey, String> consumer = (k, c) -> {};

    Runnable task =
        () -> {
          try {
            UUID uuid = store.generateTimeUuid();
            Instant commitTime = store.extractInstant(uuid).orElseThrow();
            // System.out.println(commitTime);
            CommitMeta meta =
                CommitMeta.builder()
                    .message("update value")
                    // Removing both commit time and commit time uuid is VERY likely to cause
                    // a hash collision, except for nbr of threads = 1.
                    // Removing just the commit time uuid is also likely to cause a hash
                    // collision for high nbr of threads (8 or more on my laptop),
                    // depending on the system clock resolution.
                    .commitTime(commitTime)
                    .putProperties(CommitMeta.COMMIT_TIME_UUID_KEY, uuid.toString())
                    .build();
            store.commit(branch, Optional.empty(), meta, operations, validator, consumer);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    if (parallelism == 1) {
      for (int i = 0; i < 10000; i++) {
        task.run();
      }
    } else {
      ExecutorService pool = Executors.newFixedThreadPool(parallelism);
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 10000; i++) {
        futures.add(pool.submit(task));
      }
      for (Future<?> future : futures) {
        assertThatCode(future::get).doesNotThrowAnyException();
      }
      pool.shutdownNow();
    }

    assertThat(store.getCommits(branch, false)).toIterable().hasSize(10001);

    try (CloseableIterator<Obj> objects = persist.scanAllObjects(EnumSet.of(ObjType.COMMIT))) {
      Iterable<Obj> iterable = () -> objects;
      long count =
          StreamSupport.stream(iterable.spliterator(), false)
              .map(CommitObj.class::cast)
              .filter(c -> c.message().equals("update value"))
              .count();
      if (parallelism == 1) {
        assertThat(count).isEqualTo(10000);
      } else {
        // with parallelism > 1, account for orphaned commits due to reference bump conflicts
        assertThat(count).isGreaterThanOrEqualTo(10000);
      }
      System.out.printf(
          "count = %d, garbage ratio = %.2f%%%n", count, (count - 10000) / (double) count * 100d);
    }
  }
}
