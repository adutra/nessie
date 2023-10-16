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
package org.projectnessie.catalog.model.id;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Represents an ID in the Nessie Catalog. Users of this class must not interpret the value/content
 * of the ID. Nessie Catalog IDs consist only of bytes.
 */
public final class NessieIdGeneric implements NessieId {
  public static final char[] HEX =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  private static final NessieId EMPTY_ID = NessieIdGeneric.nessieIdFromString("");
  private final byte[] id;

  private NessieIdGeneric(byte[] id) {
    this.id = id;
  }

  static NessieId emptyNessieId() {
    return EMPTY_ID;
  }

  static NessieIdGeneric randomId() {
    byte[] id = new byte[32];
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    for (int i = 0; i < id.length; i++) {
      id[i] = (byte) rand.nextInt();
    }
    return new NessieIdGeneric(id);
  }

  static NessieIdGeneric nessieIdFromUUID(UUID id) {
    byte[] bin = new byte[16];
    longToBytes(id.getMostSignificantBits(), bin, 0);
    longToBytes(id.getLeastSignificantBits(), bin, 8);
    return new NessieIdGeneric(bin);
  }

  static NessieIdGeneric nessieIdFromBytes(byte[] bytes) {
    return new NessieIdGeneric(bytes);
  }

  static NessieIdGeneric nessieIdFromString(String id) {
    int len = id.length() / 2;
    byte[] bin = new byte[len];
    for (int i = 0, c = 0; i < len; i++) {
      int high = nibble(id.charAt(c++));
      int low = nibble(id.charAt(c++));
      bin[i] = (byte) (high << 4 | low);
    }
    return new NessieIdGeneric(bin);
  }

  static NessieIdGeneric nessieIdFromStringBase64(String id) {
    byte[] decoded = Base64.getDecoder().decode(id);
    return new NessieIdGeneric(decoded);
  }

  /** Constructs a new ID instance starting with the value of this ID plus the given UUID. */
  public NessieIdGeneric compositeWithUUID(UUID id) {
    int len = this.id.length;
    byte[] bin = Arrays.copyOf(this.id, len + 16);
    longToBytes(id.getMostSignificantBits(), bin, len);
    longToBytes(id.getLeastSignificantBits(), bin, len + 8);
    return new NessieIdGeneric(bin);
  }

  private static void longToBytes(long value, byte[] bin, int off) {
    bin[off++] = (byte) (value >> 56);
    bin[off++] = (byte) (value >> 48);
    bin[off++] = (byte) (value >> 40);
    bin[off++] = (byte) (value >> 32);
    bin[off++] = (byte) (value >> 24);
    bin[off++] = (byte) (value >> 16);
    bin[off++] = (byte) (value >> 8);
    bin[off] = (byte) value;
  }

  /**
   * Extracts the last 16 bytes as a {@link UUID}, assuming this ID instance was created via {@link
   * #compositeWithUUID(UUID)}.
   */
  public UUID uuidFromComposite() {
    int len = this.id.length;

    long msb = ((long) this.id[len++]) & 0xff;
    for (int i = 1; i < 8; i++) {
      msb <<= 8;
      msb |= ((long) this.id[len++]) & 0xff;
    }

    long lsb = ((long) this.id[len++]) & 0xff;
    for (int i = 1; i < 8; i++) {
      lsb <<= 8;
      lsb |= ((long) this.id[len++]) & 0xff;
    }

    return new UUID(msb, lsb);
  }

  private static int nibble(char c) {
    if (c >= '0' && c <= '9') {
      return (int) c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return (int) c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return (int) c - 'F' + 10;
    }
    throw new IllegalArgumentException("Illegal hex character '" + c + "'");
  }

  public ByteBuffer id() {
    return ByteBuffer.wrap(id).asReadOnlyBuffer();
  }

  public String idAsString() {
    return idBytesString(new StringBuilder(id.length * 2)).toString();
  }

  @Override
  public String idAsBase64() {
    return Base64.getEncoder().encodeToString(id);
  }

  private StringBuilder idBytesString(StringBuilder sb) {
    for (byte b : id) {
      sb.append(HEX[(b >> 4) & 15]);
      sb.append(HEX[b & 15]);
    }
    return sb;
  }

  /**
   * Builds the string representation, assuming this instance was created via {@link
   * #compositeWithUUID(UUID)}.
   */
  public String idAsStringWithUUID() {
    return idBytesString(new StringBuilder(id.length * 2 + 4))
        // Assuming that a UUID.toString() (via java.lang.Long.fastUUID()) is more efficient than a
        // manually crafted UUID representation.
        .append(uuidFromComposite())
        .toString();
  }

  @Override
  public byte[] idAsBytes() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NessieIdGeneric nessieId = (NessieIdGeneric) o;

    return Arrays.equals(id, nessieId.id);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(id);
  }

  @Override
  public String toString() {
    return "NessieId{id=" + idAsString() + '}';
  }
}
