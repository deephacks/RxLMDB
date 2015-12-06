package org.deephacks.rxlmdb;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

class ScanPayload implements Payload {
  private final ByteBuffer meta;
  private final MutableDirectBuffer metaBuf;
  private boolean released = false;

  public ScanPayload() {
    this.metaBuf = Payloads.POOL.acquireMutableDirectBuffer(4);
    this.metaBuf.putInt(0, OpType.SCAN.id);
    this.meta = metaBuf.byteBuffer();
  }

  @Override
  public ByteBuffer getData() {
    return Frame.NULL_BYTEBUFFER;
  }

  @Override
  public ByteBuffer getMetadata() {
    return meta;
  }

  public void release() {
    if (!released) {
      Payloads.POOL.release(metaBuf);
      released = true;
    }
  }
}
