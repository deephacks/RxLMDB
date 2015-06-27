package org.deephacks.rxlmdb;


import org.fusesource.lmdbjni.Transaction;

public class RxTx {
  final Transaction tx;

  RxTx(Transaction tx) {
    this.tx = tx;
  }

  public void abort() {
    tx.abort();
  }

  public void commit() {
    tx.commit();
  }

  public void close() {
    tx.close();
  }

}
