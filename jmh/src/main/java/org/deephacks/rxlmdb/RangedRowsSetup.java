package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RangedRowsSetup {
  public RxDB db;
  public RxLMDB lmdb;
  public KeyRange[] keyRanges = new KeyRange[16];
  public RangedRowsSetup(Class testcase) {
    Path path = Paths.get("/tmp/rxlmdb-jmh-" + testcase.getSimpleName());
    try {
      Files.createDirectories(path);
      lmdb = RxLMDB.builder().path(path).size(ByteUnit.GIGA, 1).build();
      db = RxDB.builder().lmdb(lmdb).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < keyRanges.length; i++) {
      keyRanges[i] = KeyRange.range(new byte[]{(byte) i}, new byte[]{(byte) i});
    }
  }

  public KeyRange[] writeRanges() {
    if (db.scan().count().toBlocking().first() != 0) {
      return keyRanges;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        for (int j = 0; j < keyRanges.length; j++) {
          DirectBuffer key = new DirectBuffer(new byte[5]);
          for (int i = 0; i < 100_000; i++) {
            key.putByte(0, (byte) j);
            key.putInt(1, i, ByteOrder.BIG_ENDIAN);
            subscriber.onNext(new KeyValue(key.byteArray(), key.byteArray()));
          }
        }
        subscriber.onCompleted();
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);
    return keyRanges;
  }

  public KeyRange[] writeRangesBig() {
    if (db.scan().count().toBlocking().first() != 0) {
      return keyRanges;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        byte[] valBytes = new byte[1024];
        for (int j = 0; j < keyRanges.length; j++) {
          for (int i = 0; i < 100_000; i++) {
            DirectBuffer key = new DirectBuffer(new byte[5]);
            key.putByte(0, (byte) j);
            key.putInt(1, (byte) i, ByteOrder.BIG_ENDIAN);
            subscriber.onNext(new KeyValue(key.byteArray(), valBytes));
          }
        }
        subscriber.onCompleted();
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);
    return keyRanges;
  }

  public KeyRange[] writeProtoRanges() {
    if (db.scan().count().toBlocking().first() != 0) {
      return keyRanges;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        for (int j = 0; j < keyRanges.length; j++) {
          for (int i = 0; i < 100_000; i++) {
            byte[] bytes = RowValues.users[i % RowValues.users.length].toByteArray();
            DirectBuffer key = new DirectBuffer(new byte[5]);
            DirectBuffer val = new DirectBuffer(new byte[bytes.length]);
            key.putByte(0, (byte) j);
            key.putInt(1, (byte) i, ByteOrder.BIG_ENDIAN);
            val.putBytes(0, bytes);
            subscriber.onNext(new KeyValue(key.byteArray(), val.byteArray()));
          }
        }
        subscriber.onCompleted();
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);
    return keyRanges;
  }

  public KeyRange[] writeValsRanges() {
    if (db.scan().count().toBlocking().first() != 0) {
      return keyRanges;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        for (int j = 0; j < keyRanges.length; j++) {
          for (int i = 0; i < 100_000; i++) {
            byte[] bytes = RowValues.uservals[i % RowValues.uservals.length].toByteArray();
            DirectBuffer key = new DirectBuffer(new byte[5]);
            DirectBuffer val = new DirectBuffer(new byte[bytes.length]);
            key.putByte(0, (byte) j);
            key.putInt(1, (byte) i, ByteOrder.BIG_ENDIAN);
            val.putBytes(0, bytes);
            subscriber.onNext(new KeyValue(key.byteArray(), val.byteArray()));
          }
        }
        subscriber.onCompleted();
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);
    return keyRanges;
  }
}
