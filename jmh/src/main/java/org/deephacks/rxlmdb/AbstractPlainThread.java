package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Transaction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.deephacks.rxlmdb.DirectBufferComparator.compareTo;

public class AbstractPlainThread {
  static AtomicInteger THREAD_ID = new AtomicInteger(0);
  public final int id = THREAD_ID.getAndIncrement();
  public BufferCursor cursor;
  public Transaction tx;
  public DirectBuffer stop;
  public byte[] start;
  public Consumer<BufferCursor> consumer;

  public AbstractPlainThread(RangedRowsSetup setup, Consumer<BufferCursor> consumer) {
    this.tx = setup.lmdb.env.createReadTransaction();
    this.cursor = setup.db.db.bufferCursor(tx);
    this.stop = new DirectBuffer(setup.keyRanges[id].stop);
    this.start = setup.keyRanges[id].start;
    this.cursor.seek(start);
    this.consumer = consumer;
  }

  public final void next() {
    if (cursor.next() && compareTo(cursor.keyBuffer(), stop) <= 0) {
      consumer.accept(cursor);
    } else {
      cursor.seek(start);
    }
  }
}
