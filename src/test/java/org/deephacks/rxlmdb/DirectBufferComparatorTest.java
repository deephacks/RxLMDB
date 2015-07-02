package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DirectBufferComparatorTest {
  @Test
  public void testLessThan8bytes() {
    assertThat(compare(key(0, 0), stop()), is(0));
    assertThat(compare(key(0, 0), stop(0, 0)), is(0));
    assertThat(compare(key(0, 0), stop(0   )), is(0));
    assertThat(compare(key(0),    stop(0, 0)), is(-1));
    assertThat(compare(key(0),    stop(1, 0)), is(-1));
    assertThat(compare(key(0),    stop(1   )), is(-1));
    assertThat(compare(key(1, 0), stop(0)),    is(1));
    assertThat(compare(key(0, 1), stop(1   )), is(-1));
  }

  @Test
  public void testMoreThan8bytes() {
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(0,0,0,0,0,0,0,0)), is(0));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(1,0,0,0,0,0,0,0)), is(-1));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(0,0,0,0,0,0,0)), is(0));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(0,1,0,0,0,0,0,0)), is(-1));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(0,0,0,0,0,0,0,0)), is(0));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(0,0,0,0,0,0,0,0)), is(0));
    assertThat(compare(key(1,0,0,0,0,0,0,0), stop(1,0,0,0,0,0,0)), is(0));
    assertThat(compare(key(0,0,0,0,0,0,0,0), stop(1,0,0,0,0,0,0)), is(-1));
    assertThat(compare(key(0,1,0,0,0,0,0,0), stop(0,0,0,0,0,0,0,0)), is(1));
  }

  DirectBuffer key(int... bytes) {
    return b(bytes);
  }

  DirectBuffer stop(int... bytes) {
    return b(bytes);
  }

  DirectBuffer b(int... bytes) {
    byte[] array = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      array[i] = (byte) bytes[i];
    }
    return new DirectBuffer(array);
  }

  public static int compare(DirectBuffer key, DirectBuffer stop) {
    int result = DirectBufferComparator.compareTo(key, stop);
    String debug = String.format("%s %s %s",
      Arrays.toString(key.byteArray()),
      Arrays.toString(stop.byteArray()),
      result
    );
    // System.out.println(debug);
    return result;
  }
}
