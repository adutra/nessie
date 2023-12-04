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

import com.google.common.collect.ImmutableSet;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.storage.cassandra.serializers.ObjSerializers;

public final class CassandraConstants {

  static final int SELECT_BATCH_SIZE = 20;
  static final int MAX_CONCURRENT_BATCH_READS = 20;
  static final int MAX_CONCURRENT_DELETES = 20;
  static final int MAX_CONCURRENT_STORES = 20;

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";
  static final CqlColumn COL_REPO_ID = new CqlColumn("repo", CqlColumnType.NAME);

  static final CqlColumn COL_OBJ_ID = new CqlColumn("obj_id", CqlColumnType.OBJ_ID);
  static final String DELETE_OBJ =
      "DELETE FROM %s." + TABLE_OBJS + " WHERE " + COL_REPO_ID + "=? AND " + COL_OBJ_ID + "=?";
  static final CqlColumn COL_OBJ_TYPE = new CqlColumn("obj_type", CqlColumnType.NAME);

  public static final String INSERT_OBJ_PREFIX =
      "INSERT INTO %s."
          + TABLE_OBJS
          + " ("
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + ", "
          + COL_OBJ_TYPE
          + ", ";

  public static final String INSERT_OBJ_VALUES =
      ") VALUES (:" + COL_REPO_ID + ", :" + COL_OBJ_ID + ", :" + COL_OBJ_TYPE + ", ";
  public static final String STORE_OBJ_SUFFIX = " IF NOT EXISTS";

  static final Set<CqlColumn> COLS_OBJS_ALL =
      Stream.concat(
              Stream.of(COL_OBJ_ID, COL_OBJ_TYPE),
              ObjSerializers.ALL_SERIALIZERS.stream()
                  .flatMap(serializer -> serializer.columns().stream())
                  .sorted(Comparator.comparing(CqlColumn::name)))
          .collect(ImmutableSet.toImmutableSet());

  static final String CREATE_TABLE_OBJS;

  static {
    StringBuilder sb =
        new StringBuilder()
            .append("CREATE TABLE %s.")
            .append(TABLE_OBJS)
            .append(" (\n    ")
            .append(COL_REPO_ID)
            .append(" ")
            .append(CqlColumnType.NAME.cqlName());
    for (CqlColumn col : COLS_OBJS_ALL) {
      sb.append(",\n    ").append(col.name()).append(" ").append(col.type().cqlName());
    }
    sb.append(",\n    PRIMARY KEY ((")
        .append(COL_REPO_ID)
        .append(", ")
        .append(COL_OBJ_ID)
        .append("))\n  )");
    CREATE_TABLE_OBJS = sb.toString();
  }

  static final CqlColumn COL_REFS_NAME = new CqlColumn("ref_name", CqlColumnType.NAME);
  static final CqlColumn COL_REFS_POINTER = new CqlColumn("pointer", CqlColumnType.OBJ_ID);
  static final CqlColumn COL_REFS_DELETED = new CqlColumn("deleted", CqlColumnType.BOOL);
  static final CqlColumn COL_REFS_CREATED_AT = new CqlColumn("created_at", CqlColumnType.BIGINT);
  static final CqlColumn COL_REFS_EXTENDED_INFO = new CqlColumn("ext_info", CqlColumnType.OBJ_ID);
  static final CqlColumn COL_REFS_PREVIOUS = new CqlColumn("prev_ptr", CqlColumnType.VARBINARY);

  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE %s."
          + TABLE_REFS
          + " SET "
          + COL_REFS_POINTER
          + "=?, "
          + COL_REFS_PREVIOUS
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String PURGE_REFERENCE =
      "DELETE FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String MARK_REFERENCE_AS_DELETED =
      "UPDATE %s."
          + TABLE_REFS
          + " SET "
          + COL_REFS_DELETED
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String ADD_REFERENCE =
      "INSERT INTO %s."
          + TABLE_REFS
          + " ("
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + ", "
          + COL_REFS_POINTER
          + ", "
          + COL_REFS_DELETED
          + ", "
          + COL_REFS_CREATED_AT
          + ", "
          + COL_REFS_EXTENDED_INFO
          + ", "
          + COL_REFS_PREVIOUS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
  static final String FIND_REFERENCES =
      "SELECT "
          + COL_REFS_NAME
          + ", "
          + COL_REFS_POINTER
          + ", "
          + COL_REFS_DELETED
          + ", "
          + COL_REFS_CREATED_AT
          + ", "
          + COL_REFS_EXTENDED_INFO
          + ", "
          + COL_REFS_PREVIOUS
          + " FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + " IN ?";
  static final String CREATE_TABLE_REFS =
      "CREATE TABLE %s."
          + TABLE_REFS
          + "\n  (\n    "
          + COL_REPO_ID
          + " "
          + COL_REPO_ID.type().cqlName()
          + ",\n    "
          + COL_REFS_NAME
          + " "
          + COL_REFS_NAME.type().cqlName()
          + ",\n    "
          + COL_REFS_POINTER
          + " "
          + COL_REFS_POINTER.type().cqlName()
          + ",\n    "
          + COL_REFS_DELETED
          + " "
          + COL_REFS_DELETED.type().cqlName()
          + ",\n    "
          + COL_REFS_CREATED_AT
          + " "
          + COL_REFS_CREATED_AT.type().cqlName()
          + ",\n    "
          + COL_REFS_EXTENDED_INFO
          + " "
          + COL_REFS_EXTENDED_INFO.type().cqlName()
          + ",\n    "
          + COL_REFS_PREVIOUS
          + " "
          + COL_REFS_PREVIOUS.type().cqlName()
          + ",\n    PRIMARY KEY (("
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + "))\n  )";

  static final String FETCH_OBJ_TYPE =
      "SELECT "
          + COL_OBJ_TYPE
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN ?";

  static final String FIND_OBJS =
      "SELECT "
          + COLS_OBJS_ALL.stream().map(CqlColumn::name).collect(Collectors.joining(", "))
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN ?";

  static final String SCAN_OBJS =
      "SELECT "
          + COLS_OBJS_ALL.stream().map(CqlColumn::name).collect(Collectors.joining(", "))
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? ALLOW FILTERING";

  static final String ERASE_OBJS_SCAN =
      "SELECT "
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + " IN ? ALLOW FILTERING";
  static final String ERASE_REFS_SCAN =
      "SELECT "
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + " FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + " IN ? ALLOW FILTERING";

  static final String ERASE_OBJ = DELETE_OBJ;
  static final String ERASE_REF =
      "DELETE FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + " = ? AND "
          + COL_REFS_NAME
          + " = ?";

  private CassandraConstants() {}
}
