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

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COLS_OBJS_ALL;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_CREATED_AT;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REFS_PREVIOUS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.CREATE_TABLE_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.CREATE_TABLE_REFS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ERASE_OBJ;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ERASE_OBJS_SCAN;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ERASE_REF;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ERASE_REFS_SCAN;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MAX_CONCURRENT_BATCH_READS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MAX_CONCURRENT_DELETES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.SELECT_BATCH_SIZE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.TABLE_REFS;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.jetbrains.annotations.NotNull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CassandraBackend implements Backend {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBackend.class);

  private final CassandraBackendConfig config;
  private final boolean closeClient;

  private final Map<String, PreparedStatement> statements = new ConcurrentHashMap<>();
  private final CqlSession session;

  CassandraBackend(CassandraBackendConfig config, boolean closeClient) {
    this.config = config;
    this.session = requireNonNull(config.client());
    this.closeClient = closeClient;
  }

  <K, R> BatchedQuery<K, R> newBatchedQuery(
      Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder,
      Function<Row, R> rowToResult,
      Function<R, K> idExtractor,
      int results,
      Class<? extends R> elementType) {
    return new BatchedQueryImpl<>(queryBuilder, rowToResult, idExtractor, results, elementType);
  }

  interface BatchedQuery<K, R> extends AutoCloseable {
    void add(K key, int index);

    R[] finish();

    @Override
    void close();
  }

  private static final class BatchedQueryImpl<K, R> implements BatchedQuery<K, R> {

    private static final AtomicLong ID_GEN = new AtomicLong();
    private static final long BATCH_TIMEOUT_MILLIS = SECONDS.toMillis(30);
    private final long id;
    private final Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder;
    private final List<K> keys = new ArrayList<>();
    private final Semaphore permits = new Semaphore(MAX_CONCURRENT_BATCH_READS);
    private final Function<Row, R> rowToResult;
    private final Function<R, K> idExtractor;
    private final Object2IntHashMap<K> idToIndex;
    private final AtomicReferenceArray<R> result;
    private final Class<? extends R> elementType;
    private volatile Throwable failure;
    private volatile int queryCount;
    private volatile int queriesCompleted;
    // A "hard", long timeout that's reset for every new submitted query.
    private volatile long timeoutAt;

    BatchedQueryImpl(
        Function<List<K>, CompletionStage<AsyncResultSet>> queryBuilder,
        Function<Row, R> rowToResult,
        Function<R, K> idExtractor,
        int results,
        Class<? extends R> elementType) {
      this.idToIndex = new Object2IntHashMap<>(results * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);
      this.result = new AtomicReferenceArray<>(results);
      this.elementType = elementType;
      this.rowToResult = rowToResult;
      this.idExtractor = idExtractor;
      this.queryBuilder = queryBuilder;
      this.id = ID_GEN.incrementAndGet();
      setNewTimeout();
    }

    private void setNewTimeout() {
      this.timeoutAt = System.currentTimeMillis() + BATCH_TIMEOUT_MILLIS;
    }

    @Override
    public void add(K key, int index) {
      idToIndex.put(key, index);
      keys.add(key);
      if (keys.size() == SELECT_BATCH_SIZE) {
        flush();
      }
    }

    private void noteException(Throwable ex) {
      synchronized (this) {
        Throwable curr = failure;
        if (curr != null) {
          curr.addSuppressed(ex);
        } else {
          failure = ex;
        }
      }
    }

    private void flush() {
      if (keys.isEmpty()) {
        return;
      }

      List<K> batchKeys = new ArrayList<>(keys);
      keys.clear();

      synchronized (this) {
        queryCount++;
      }

      Consumer<CompletionStage<AsyncResultSet>> terminate =
          query -> {
            // Remove the completed query from the queue, so another query can be submitted
            permits.release();
            // Increment the number of completed queries and notify the "driver"
            synchronized (this) {
              queriesCompleted++;
              this.notify();
            }
          };

      CompletionStage<AsyncResultSet> query = queryBuilder.apply(batchKeys);

      BiFunction<AsyncResultSet, Throwable, ?> pageHandler =
          new BiFunction<>() {
            @Override
            public Object apply(AsyncResultSet rs, Throwable ex) {
              if (ex != null) {
                noteException(ex);
                terminate.accept(query);
              } else {
                try {
                  for (Row row : rs.currentPage()) {
                    R resultItem = rowToResult.apply(row);
                    K id = idExtractor.apply(resultItem);
                    int i = idToIndex.getValue(id);
                    if (i != -1) {
                      result.set(i, resultItem);
                    }
                  }

                  if (rs.hasMorePages()) {
                    rs.fetchNextPage().handleAsync(this);
                  } else {
                    terminate.accept(query);
                  }

                } catch (Throwable t) {
                  noteException(t);
                  terminate.accept(query);
                }
              }
              return null;
            }
          };

      try {
        permits.acquire();
        setNewTimeout();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      query.handleAsync(pageHandler);
    }

    @Override
    public void close() {
      finish();
    }

    @Override
    public R[] finish() {
      flush();

      while (true) {
        synchronized (this) {
          // If a failure happened, there's not much that can be done, just re-throw
          Throwable f = failure;
          if (f != null) {
            if (f instanceof RuntimeException) {
              throw (RuntimeException) f;
            }
            throw new RuntimeException(f);
          } else if (queriesCompleted == queryCount) {
            // No failure, all queries completed.
            break;
          }

          // Such a timeout should really never happen, it indicates a bug in the code above.
          checkState(
              System.currentTimeMillis() < timeoutAt,
              "Batched Cassandra queries bcq%s timed out: completed: %s, queries: %s",
              id,
              queriesCompleted,
              queryCount);

          try {
            this.wait(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }

      return resultToArray();
    }

    private R[] resultToArray() {
      int l = result.length();
      @SuppressWarnings("unchecked")
      R[] r = (R[]) Array.newInstance(elementType, l);
      for (int i = 0; i < l; i++) {
        r[i] = result.get(i);
      }
      return r;
    }
  }

  @NotNull
  BoundStatement buildStatement(String cql, Object... values) {
    PreparedStatement prepared =
        statements.computeIfAbsent(cql, c -> session.prepare(format(c, config.keyspace())));
    return prepared
        .boundStatementBuilder(values)
        .setTimeout(config.dmlTimeout())
        .setConsistencyLevel(LOCAL_QUORUM)
        .setSerialConsistencyLevel(LOCAL_SERIAL)
        .build();
  }

  @NotNull
  BoundStatementBuilder newBoundStatementBuilder(String cql) {
    PreparedStatement prepared =
        statements.computeIfAbsent(cql, c -> session.prepare(format(c, config.keyspace())));
    return prepared.boundStatementBuilder();
  }

  boolean executeCas(BoundStatement stmt) {
    try {
      ResultSet rs = execute(stmt);
      return rs.wasApplied();
    } catch (DriverException e) {
      handleDriverException(e);
      return false;
    }
  }

  ResultSet execute(BoundStatement stmt) {
    return session.execute(stmt);
  }

  CompletionStage<AsyncResultSet> executeAsync(BoundStatement stmt) {
    return session.executeAsync(stmt);
  }

  void handleDriverException(DriverException e) {
    if (e instanceof CASWriteUnknownException) {
      logCASWriteUnknown((CASWriteUnknownException) e);
    } else {
      throw e;
    }
  }

  @SuppressWarnings("Slf4jDoNotLogMessageOfExceptionExplicitly")
  private static void logCASWriteUnknown(CASWriteUnknownException e) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Assuming CAS failed due to CASWriteUnknownException with message'{}', coordinator: {}, errors: {}, warnings: {}",
          e.getMessage(),
          e.getExecutionInfo().getCoordinator(),
          e.getExecutionInfo().getErrors(),
          e.getExecutionInfo().getWarnings());
    }
  }

  @Override
  @Nonnull
  public PersistFactory createFactory() {
    return new CassandraPersistFactory(this);
  }

  @Override
  public void close() {
    if (closeClient) {
      session.close();
    }
  }

  @Override
  public void setupSchema() {
    Metadata metadata = session.getMetadata();
    Optional<KeyspaceMetadata> keyspace = metadata.getKeyspace(config.keyspace());

    checkState(
        keyspace.isPresent(),
        "Cassandra Keyspace '%s' must exist, but does not exist.",
        config.keyspace());

    createTableIfNotExists(
        keyspace.get(),
        TABLE_REFS,
        CREATE_TABLE_REFS,
        Stream.of(
                COL_REPO_ID,
                COL_REFS_NAME,
                COL_REFS_POINTER,
                COL_REFS_DELETED,
                COL_REFS_CREATED_AT,
                COL_REFS_EXTENDED_INFO,
                COL_REFS_PREVIOUS)
            .collect(toImmutableSet()),
        List.of(COL_REPO_ID, COL_REFS_NAME));
    createTableIfNotExists(
        keyspace.get(),
        TABLE_OBJS,
        CREATE_TABLE_OBJS,
        Stream.concat(Stream.of(COL_REPO_ID), COLS_OBJS_ALL.stream()).collect(toImmutableSet()),
        List.of(COL_REPO_ID, COL_OBJ_ID));
  }

  private void createTableIfNotExists(
      KeyspaceMetadata meta,
      String tableName,
      String createTable,
      Set<CqlColumn> expectedColumns,
      List<CqlColumn> expectedPrimaryKey) {

    Optional<TableMetadata> table = meta.getTable(tableName);

    createTable = format(createTable, meta.getName());

    if (table.isPresent()) {

      checkState(
          checkPrimaryKey(table.get(), expectedPrimaryKey),
          "Expected primary key columns %s do not match existing primary key columns %s for table '%s'. DDL template:\n%s",
          expectedPrimaryKey.stream()
              .map(col -> entry(col.name(), col.type().dataType()))
              .collect(toImmutableMap(Entry::getKey, Entry::getValue)),
          table.get().getPartitionKey().stream()
              .map(col -> entry(col.getName(), col.getType()))
              .collect(toImmutableMap(Entry::getKey, Entry::getValue)),
          tableName,
          createTable);

      List<String> missingColumns = checkColumns(table.get(), expectedColumns);
      if (!missingColumns.isEmpty()) {
        throw new IllegalStateException(
            format(
                "The database table %s is missing mandatory columns %s.%nFound columns : %s%nExpected columns : %s%nDDL template:\n%s",
                tableName,
                sortedColumnNames(missingColumns),
                sortedColumnNames(table.get().getColumns().keySet()),
                sortedColumnNames(expectedColumns),
                createTable));
      }

      // Existing table looks compatible
      return;
    }

    SimpleStatement stmt =
        SimpleStatement.builder(createTable).setTimeout(config.ddlTimeout()).build();
    session.execute(stmt);
  }

  private static String sortedColumnNames(Collection<?> input) {
    return input.stream().map(Object::toString).sorted().collect(Collectors.joining(","));
  }

  private boolean checkPrimaryKey(TableMetadata table, List<CqlColumn> expectedPrimaryKey) {
    List<ColumnMetadata> partitionKey = table.getPartitionKey();
    if (partitionKey.size() == expectedPrimaryKey.size()) {
      for (int i = 0; i < partitionKey.size(); i++) {
        ColumnMetadata column = partitionKey.get(i);
        CqlColumn expectedColumn = expectedPrimaryKey.get(i);
        if (!column.getName().asInternal().equals(expectedColumn.name())
            || !column.getType().equals(expectedColumn.type().dataType())) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private List<String> checkColumns(TableMetadata table, Set<CqlColumn> expectedColumns) {
    List<String> missing = new ArrayList<>();
    for (CqlColumn expectedColumn : expectedColumns) {
      if (table.getColumn(expectedColumn.name()).isEmpty()) {
        missing.add(expectedColumn.name());
      }
    }
    return missing;
  }

  @Override
  public String configInfo() {
    return "keyspace: "
        + config.keyspace()
        + " DDL timeout: "
        + config.ddlTimeout()
        + " DML timeout: "
        + config.dmlTimeout();
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    ArrayList<String> repoIdList = new ArrayList<>(repositoryIds);

    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_DELETES)) {
      for (Row row : execute(buildStatement(ERASE_REFS_SCAN, repoIdList))) {
        String repoId = row.getString(0);
        String ref = row.getString(1);
        requests.submitted(executeAsync(buildStatement(ERASE_REF, repoId, ref)));
      }

      for (Row row : execute(buildStatement(ERASE_OBJS_SCAN, repoIdList))) {
        String repoId = row.getString(0);
        String objId = row.getString(1);
        requests.submitted(executeAsync(buildStatement(ERASE_OBJ, repoId, objId)));
      }
    }
    // We must ensure that the system clock advances a little, so that C*'s next write-timestamp
    // does not collide with the write-timestamps of the DELETE statements above. Otherwise, the
    // above DELETEs will silently "overrule" a following INSERT/UPDATE statement. In C*, if a
    // DELETE and another INSERT/UPDATE have the same write-timestamp, the DELETE wins. This makes
    // Nessie tests fail on machines that are "fast enough".
    try {
      Thread.sleep(2L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
