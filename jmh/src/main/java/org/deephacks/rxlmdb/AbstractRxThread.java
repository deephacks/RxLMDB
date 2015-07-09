package org.deephacks.rxlmdb;


import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractRxThread<T> {
  static AtomicInteger THREAD_ID = new AtomicInteger(0);
  public final int id = THREAD_ID.getAndIncrement();
  public RxTx tx;
  public Iterator<T> values;
  public Iterator<List<T>> obs;
  public Scan<T> scan;
  private RangedRowsSetup setup;

  public AbstractRxThread(RangedRowsSetup setup, Scan<T> scan) {
    this.tx = setup.lmdb.readTx();
    this.scan = scan;
    this.setup = setup;
    this.values = Collections.emptyIterator();
    this.obs = Collections.emptyIterator();
  }

  public final void next() {
    if (values.hasNext()) {
      values.next();
    } else if (obs.hasNext()) {
      values = obs.next().iterator();
    } else {
      obs = setup.db.scan(100_000, tx, scan, setup.keyRanges[id])
        .toBlocking().toIterable().iterator();
      values = obs.next().iterator();
    }
  }
}
