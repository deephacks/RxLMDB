package org.deephacks.rxlmdb;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.deephacks.rxlmdb.Rxdb.*;
import org.fusesource.lmdbjni.LMDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.util.List;
import java.util.concurrent.TimeUnit;

final class RxDbServiceGrpc implements DatabaseServiceGrpc.DatabaseService {
  private static final Logger logger = LoggerFactory.getLogger(RxDbServiceGrpc.class);
  private static final KeyRange.KeyRangeType[] TYPES = KeyRange.KeyRangeType.values();
  private RxLmdb lmdb;
  private RxDb db;

  RxDbServiceGrpc(RxDb db) {
    this.lmdb = db.lmdb;
    this.db = db;
  }

  @Override
  public void put(PutMsg msg, StreamObserver<Empty> response) {
    try {
      db.put(new KeyValue(msg.getKey().toByteArray(), msg.getVal().toByteArray()));
      response.onCompleted();
    } catch (LMDBException e) {
      if (logger.isErrorEnabled()) {
        logger.error("LMDB error " + e.getErrorCode() + " '" + e.getMessage() + "'");
      }
      throw new StatusRuntimeException(Status.INTERNAL);
    }
  }

  @Override
  public StreamObserver<PutMsg> batch(StreamObserver<Empty> response) {
    SerializedSubject<KeyValue, KeyValue> subject = PublishSubject.<KeyValue>create().toSerialized();
    Observable<List<KeyValue>> buffer = subject.buffer(10, TimeUnit.NANOSECONDS, 512);
    db.batch(buffer);
    return new StreamObserver<PutMsg>() {
      @Override
      public void onNext(PutMsg msg) {
        subject.onNext(new KeyValue(msg.getKey().toByteArray(), msg.getVal().toByteArray()));
      }

      @Override
      public void onError(Throwable t) {
        subject.onError(t);
      }

      @Override
      public void onCompleted() {
        subject.onCompleted();
      }
    };
  }

  @Override
  public void get(Rxdb.GetMsg msg, StreamObserver<ValueMsg> response) {
    try {
      byte[] bytes = db.get(msg.getKey().toByteArray());
      ValueMsg.Builder builder = ValueMsg.newBuilder();
      if (bytes != null) {
        builder.setVal(ByteString.copyFrom(bytes));
      }
      response.onNext(builder.build());
      response.onCompleted();
    } catch (LMDBException e) {
      if (logger.isErrorEnabled()) {
        logger.error("LMDB error " + e.getErrorCode() + " '" + e.getMessage() + "'");
      }
      throw new StatusRuntimeException(Status.INTERNAL);
    }
  }

  @Override
  public void delete(DeleteMsg msg, StreamObserver<BooleanMsg> response) {
    try {
      boolean delete = db.delete(msg.getKey().toByteArray());
      response.onNext(BooleanMsg.newBuilder().setValue(delete).build());
      response.onCompleted();
    } catch (LMDBException e) {
      if (logger.isErrorEnabled()) {
        logger.error("LMDB error " + e.getErrorCode() + " '" + e.getMessage() + "'");
      }
      throw new StatusRuntimeException(Status.INTERNAL);
    }
  }

  @Override
  public void scan(KeyRangeMsg msg, StreamObserver<KeyValueMsg> response) {
    KeyRange.KeyRangeType type = TYPES[msg.getType().getNumber()];
    KeyRange keyRange = new KeyRange(msg.getStart().toByteArray(), msg.getStop().toByteArray(), type);
    try {
      db.scan(keyRange).subscribe(new Observer<List<KeyValue>>() {
        @Override
        public void onCompleted() {
          response.onCompleted();
        }

        @Override
        public void onError(Throwable throwable) {
          if (throwable instanceof LMDBException) {
            if (logger.isErrorEnabled()) {
              LMDBException e = (LMDBException) throwable;
              logger.error("LMDB error " + e.getErrorCode() + " '" + e.getMessage() + "'");
            }
            response.onError(new StatusRuntimeException(Status.INTERNAL));
          } else {
            response.onError(throwable);
          }
        }

        @Override
        public void onNext(List<KeyValue> kvs) {
          for (KeyValue kv : kvs) {
            ByteString k = ByteString.copyFrom(kv.key());
            ByteString v = ByteString.copyFrom(kv.value());
            KeyValueMsg msg = KeyValueMsg.newBuilder().setKey(k).setVal(v).build();
            response.onNext(msg);
          }
        }
      });
    } catch (LMDBException e) {
      if (logger.isErrorEnabled()) {
        logger.error("LMDB error " + e.getErrorCode() + " '" + e.getMessage() + "'");
      }
      throw new StatusRuntimeException(Status.INTERNAL);
    }
  }
}