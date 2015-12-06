package org.deephacks.rxlmdb;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.internal.frame.ThreadLocalFramePool;

import java.nio.ByteBuffer;

class Payloads {
  static final ThreadLocalFramePool POOL = new ThreadLocalFramePool();

  public static final Payload EMPTY_PAYLOAD = new Payload() {
    @Override
    public ByteBuffer getData() {
      return Frame.NULL_BYTEBUFFER;
    }

    @Override
    public ByteBuffer getMetadata() {
      return Frame.NULL_BYTEBUFFER;
    }
  };

  public static Payload create(final ByteBuffer data, ByteBuffer meta) {

    return new Payload() {
      public ByteBuffer getData() {
        return data;
      }

      @Override
      public ByteBuffer getMetadata() {
        return meta;
      }
    };
  }

  public static Payload create(final ByteBuffer data) {
    return new Payload() {
      public ByteBuffer getData() {
        return data;
      }

      @Override
      public ByteBuffer getMetadata() {
        return Frame.NULL_BYTEBUFFER;
      }
    };
  }
}

