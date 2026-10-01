package com.example.sportsbook.feed.provider;

public final class ProviderVersion {
	static final long EPOCH_FACTOR = 1_000_000_000_000_000L;
	private ProviderVersion() {}
	/**
	 * @throws IllegalArgumentException if epoch is negative or sequence is outside {@code [0, 10^15)}
	 * @throws ArithmeticException      if the epoch is too large for the version to fit in a long
	 */
	public static long compose(long epoch, long sequence) {
		checkIfValid(epoch);
		checkIfValid(sequence);
		requireSequenceInRange(sequence);
		return Math.addExact(Math.multiplyExact(epoch, EPOCH_FACTOR),  sequence);
	}
	public static long epochOf(long version) {
		checkIfValid(version);
		return version / EPOCH_FACTOR;
    }
   public static long sequenceOf(long version){
	   checkIfValid(version);
	 return version % EPOCH_FACTOR;
   }
   private static void checkIfValid(long num){
	   if(num < 0) throw new IllegalArgumentException("num cannot be negative");
   }
	private static void requireSequenceInRange(long sequence) {
		if(sequence>=EPOCH_FACTOR) throw new IllegalArgumentException("sequence must be below " + EPOCH_FACTOR + ", got " + sequence);
	}
}
