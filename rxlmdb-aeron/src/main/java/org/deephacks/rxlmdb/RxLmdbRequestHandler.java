package org.deephacks.rxlmdb;

import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.List;
import java.util.concurrent.TimeUnit;

class RxLmdbRequestHandler extends RequestHandler implements Loggable {
  RxDb db;
  SerializedSubject<KeyValue, KeyValue> subject = PublishSubject.<KeyValue>create().toSerialized();
  Observable<List<KeyValue>> buffer = subject.buffer(10, TimeUnit.MICROSECONDS, 512);

  RxLmdbRequestHandler(RxDb db) {
    this.db = db;
    this.db.batch(buffer);
  }

  @Override
  public Publisher<Payload> handleRequestResponse(Payload payload) {
    UnsafeBuffer metadata = new UnsafeBuffer(payload.getMetadata());
    OpType type = OpType.values()[metadata.getInt(0)];
    if (type == OpType.PUT) {
      db.put(KeyValuePayload.getKeyValue(payload));
      return s -> {
        s.onNext(Payloads.EMPTY_PAYLOAD);
        s.onComplete();
      };
    } else if (type == OpType.GET) {
      return s -> {
        byte[] key = KeyValuePayload.getByteArray(payload);
        byte[] val = db.get(key);
        if (val != null) {
          s.onNext(new KeyValuePayload(key, val, OpType.GET));
        } else {
          s.onNext(Payloads.EMPTY_PAYLOAD);
        }
        s.onComplete();
      };
    } else {
      logger().error("No OpType found for type {}", type);
      return s -> {
        s.onNext(Payloads.EMPTY_PAYLOAD);
        s.onComplete();
      };
    }
  }

  @Override
  public Publisher<Payload> handleRequestStream(Payload payload) {
    UnsafeBuffer metadata = new UnsafeBuffer(payload.getMetadata());
    OpType type = OpType.values()[metadata.getInt(0)];
    if (type == OpType.SCAN) {
      return RxReactiveStreams.toPublisher(db.scan()
        .flatMap(Observable::from)
        .map(kv -> {
          // need to clean up the payload
          return new KeyValuePayload(kv.key, kv.value, OpType.SCAN);
        }));
    } else {
      logger().error("No OpType found for type {}", type);
      return s -> {
        s.onNext(Payloads.EMPTY_PAYLOAD);
        s.onComplete();
      };
    }
  }

  @Override
  public Publisher<Void> handleFireAndForget(Payload payload) {
    return s -> {
      OpType type = OpType.values()[payload.getMetadata().getShort()];
      if (type == OpType.PUT) {
        subject.onNext(KeyValuePayload.getKeyValue(payload));
      } else {
        System.err.println(type + " not fireForget");
      }
      s.onComplete();
    };
  }

  @Override
  public Publisher<Payload> handleSubscription(Payload payload) {
    return RequestHandler.NO_REQUEST_SUBSCRIPTION_HANDLER.apply(payload);
  }

  @Override
  public Publisher<Payload> handleChannel(Publisher<Payload> payloads) {
    return RequestHandler.NO_REQUEST_CHANNEL_HANDLER.apply(payloads);
  }

  @Override
  public Publisher<Void> handleMetadataPush(Payload payload) {
    return RequestHandler.NO_METADATA_PUSH_HANDLER.apply(payload);
  }

}
