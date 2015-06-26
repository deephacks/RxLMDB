package org.deephacks.rxlmdb;

public enum ByteUnit {

  /**
   * BYTE
   */
  BYTE(0, 'b') {
    public long toBytes(final long amount) { return amount; }
    public long toKiloBytes(final long amount) { return safeShift(amount, KILO.offset - BYTE.offset); }
    public long toMegaBytes(final long amount) { return safeShift(amount, MEGA.offset - BYTE.offset); }
    public long toGigaBytes(final long amount) { return safeShift(amount, GIGA.offset - BYTE.offset); }
  },

  KILO(BYTE.offset + ByteUnit.OFFSET, 'k') {
    public long toBytes(final long amount) { return safeShift(amount, BYTE.offset - KILO.offset); }
    public long toKiloBytes(final long amount) { return amount; }
    public long toMegaBytes(final long amount) { return safeShift(amount, MEGA.offset - KILO.offset); }
    public long toGigaBytes(final long amount) { return safeShift(amount, GIGA.offset - KILO.offset); }
  },

  MEGA(KILO.offset + ByteUnit.OFFSET, 'm') {
    public long toBytes(final long amount) { return safeShift(amount, BYTE.offset - MEGA.offset); }
    public long toKiloBytes(final long amount) { return safeShift(amount, KILO.offset - MEGA.offset); }
    public long toMegaBytes(final long amount) { return amount; }
    public long toGigaBytes(final long amount) { return safeShift(amount, GIGA.offset - MEGA.offset); }
  },

  GIGA(MEGA.offset + ByteUnit.OFFSET, 'g') {
    public long toBytes(final long amount) { return safeShift(amount, BYTE.offset - GIGA.offset); }
    public long toKiloBytes(final long amount) { return safeShift(amount, KILO.offset - GIGA.offset); }
    public long toMegaBytes(final long amount) { return safeShift(amount, MEGA.offset - GIGA.offset); }
    public long toGigaBytes(final long amount) { return amount; }
  };

  private static final int OFFSET = 10;

  private final int offset;
  private final char unit;

  private ByteUnit(final int offset, final char unit) {
    this.offset = offset;
    this.unit = unit;
  }

  /**
   * Retrieves the unit character for the MemoryUnit
   * @return the unit character
   */
  public char getUnit() {
    return unit;
  }

  /**
   * returns the amount in bytes
   * @param amount of the unit
   * @return value in bytes
   */
  public abstract long toBytes(long amount);
  /**
   * returns the amount in kilobytes
   * @param amount of the unit
   * @return value in kilobytes
   */
  public abstract long toKiloBytes(long amount);
  /**
   * returns the amount in megabytes
   * @param amount of the unit
   * @return value in megabytes
   */
  public abstract long toMegaBytes(long amount);
  /**
   * returns the amount in gigabytes
   * @param amount of the unit
   * @return value in gigabytes
   */
  public abstract long toGigaBytes(long amount);

  /**
   * Human readable value, with the added unit character as a suffix
   * @param amount the amount to print
   * @return the String representation
   */
  public String toString(final long amount) {
    return amount + Character.toString(this.unit);
  }

  private static long safeShift(final long unit, final long shift) {
    if (shift > 0) {
      return unit >>> shift;
    } else if (shift <= -1 * Long.numberOfLeadingZeros(unit)) {
      return Long.MAX_VALUE;
    } else {
      return unit << -shift;
    }
  }
}
