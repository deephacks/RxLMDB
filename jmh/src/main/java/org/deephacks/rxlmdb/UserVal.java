package org.deephacks.rxlmdb;

import org.deephacks.vals.Id;
import org.deephacks.vals.Val;

@Val
public interface UserVal {
  @Id(1) byte[] getSsn();
  @Id(2) byte[] getFirstname();
  @Id(3) byte[] getLastname();
  @Id(4) byte[] getEmail();
  @Id(5) long getMobile();
}
