package org.deephacks.rxlmdb;

import generated.User;
import okio.ByteString;

public class ProtoRows {
  public static User[] users = new User[10];

  static {
    for (int i = 0; i < users.length; i++) {
      users[i] = new User.Builder()
        .ssn(ByteString.of(new byte[]{1, 2, 3, 4, 5, (byte) i}))
        .firstname(ByteString.of(("name" + i).getBytes()))
        .lastname(ByteString.of(("lastname" + i).getBytes()))
        .email(ByteString.of(("email" + i + "@email.com").getBytes()))
        .mobile(Long.valueOf(i))
        .build();
    }
  }
}
