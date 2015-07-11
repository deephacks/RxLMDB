package org.deephacks.rxlmdb;

import generated.proto.User;
import okio.ByteString;
import org.fusesource.lmdbjni.DirectBuffer;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

public class RangedRowsSetup {
  public RxDB db;
  public RxLMDB lmdb;
  public KeyRange[] keyRanges = new KeyRange[16];

  public static User[] PROTO = new User[10];

  static {
    for (int i = 0; i < PROTO.length; i++) {
      PROTO[i] = new User.Builder()
        .ssn(ByteString.of(new byte[]{1, 2, 3, 4, 5, (byte) i}))
        .firstname(ByteString.of(("name" + i).getBytes()))
        .lastname(ByteString.of(("lastname" + i).getBytes()))
        .email(ByteString.of(("email" + i + "@email.com").getBytes()))
        .mobile(Long.valueOf(i))
        .build();
    }
  }

  public static UserVal[] VALS = new UserVal[10];

  static {
    for (int i = 0; i < VALS.length; i++) {
      VALS[i] = new UserValBuilder()
        .withSsn(new byte[]{1, 2, 3, 4, 5, (byte) i})
        .withFirstname(("name" + i).getBytes())
        .withLastname(("lastname" + i).getBytes())
        .withEmail(("email" + i + "@email.com").getBytes())
        .withMobile(Long.valueOf(i)).build();
    }
  }

  public static uk.co.real_logic.sbe.codec.java.DirectBuffer[] SBE =
    new uk.co.real_logic.sbe.codec.java.DirectBuffer[10];

  static {
    for (int i = 0; i < SBE.length; i++) {
      uk.co.real_logic.sbe.codec.java.DirectBuffer buffer = new uk.co.real_logic.sbe.codec.java.DirectBuffer(new byte[64]);
      SBE[i] = buffer;
      generated.sbe.User user = new generated.sbe.User();
      user.wrapForEncode(buffer, 0);
      byte[] firstname = ("name" + i).getBytes();
      byte[] lastname = ("lastname" + i).getBytes();
      byte[] email = ("email" + i + "@email.com").getBytes();
      user.mobile(i);
      user.putFirstname(firstname, 0, firstname.length);
      user.putLastname(lastname, 0, lastname.length);
      user.putEmail(email, 0, email.length);
    }
  }

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

  public void writeSmallKeyValue() {
    writeRanges(i -> {
      DirectBuffer val = new DirectBuffer(new byte[5]);
      val.putInt(0, i);
      return val.byteArray();
    });
  }

  public void writeBigKeyValue() {
    final byte[] bytes = new byte[1024];
    writeRanges(i -> bytes);
  }

  public void writeProto() {
    writeRanges(i -> PROTO[i % PROTO.length].toByteArray());
  }

  public void writeValsRanges() {
    writeRanges(i -> VALS[i % VALS.length].toByteArray());
  }

  public void writeSbeRanges() {
    writeRanges(i -> SBE[i % SBE.length].array());
  }

  private void writeRanges(Function<Integer, byte[]> value) {
    if (db.scan().count().toBlocking().first() != 0) {
      return;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        DirectBuffer key = new DirectBuffer(new byte[5]);
        for (int j = 0; j < keyRanges.length; j++) {
          for (int i = 0; i < 100_000; i++) {
            byte[] bytes = value.apply(i);
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
  }
}
