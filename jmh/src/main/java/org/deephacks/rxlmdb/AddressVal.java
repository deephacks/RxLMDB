package org.deephacks.rxlmdb;

import org.deephacks.vals.Encodable;
import org.deephacks.vals.Id;
import org.deephacks.vals.Val;

@Val
public interface AddressVal extends Encodable {
  @Id(1) byte[] getStreetname();
  @Id(2) int getZipcode();
  @Id(3) int getAreaCode();
  @Id(4) Country getCountry();
  @Id(5) long getTelephone();
}
