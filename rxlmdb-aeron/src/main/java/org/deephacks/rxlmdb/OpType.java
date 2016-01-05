package org.deephacks.rxlmdb;

enum OpType {
  PUT(0x00),
  GET(0x01),
  DELETE(0x02),
  BATCH(0x03),
  SCAN(0x04);

  private static OpType[] typesById;

  final int id;

  static {
    int max = 0;

    for (OpType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new OpType[max + 1];

    for (OpType t : values()) {
      typesById[t.id] = t;
    }
  }

  OpType(final int id)
  {
    this.id = id;
  }

}
