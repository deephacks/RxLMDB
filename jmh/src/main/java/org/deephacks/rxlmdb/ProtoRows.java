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

  public static Val_UserVal[] uservals = new Val_UserVal[10];

  static {
    for (int i = 0; i < uservals.length; i++) {
      uservals[i] = (Val_UserVal) new UserValBuilder()
        .withSsn(new byte[]{1, 2, 3, 4, 5, (byte) i})
        .withFirstname(("name" + i).getBytes())
        .withLastname(("lastname" + i).getBytes())
        .withEmail(("email" + i + "@email.com").getBytes())
        .withMobile(Long.valueOf(i)).build();
    }
  }
}
