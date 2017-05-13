package org.wowcoders.beringei.compression;

import java.nio.LongBuffer;

/**
 * 
 * @author vmukumar
 *
 */
public class LongBufferBitReader implements BitStream {
	public static long []masksRead = new long[64];
	public static long []masksSet = new long[64];

	static {
		for (int i= 0; i < 64; i++) {
			long mask = 1L << i;
			masksRead[63-i] = mask;
			masksSet[i] = mask;
		}
	}

	private final LongBuffer buf;

	private final long numOfBits;
	private int pos;

	private int totalInBuf = -1;
	private int lastReadIdx = -1;
	private long readLong = 0;
	private long readLast = 0;

	/**
	 * 
	 * @param buf
	 */
	public LongBufferBitReader(LongBuffer buf) {
		this.buf = buf;
		totalInBuf = buf.remaining();
		this.numOfBits = (totalInBuf * 64);

		this.pos = 0;
	}

	/**
	 * 
	 * @param buf
	 * @param remaining
	 */

	public LongBufferBitReader(LongBuffer buf, long remaining) {
		this.buf = buf;
		totalInBuf = buf.remaining();
		this.numOfBits = ((totalInBuf + 1) * 64);
		this.pos = 0;

		readLast = remaining;
	}

	/**
	 * 
	 * @return
	 */
	public boolean getBit() {
		if (pos >= numOfBits)
			throw new IllegalStateException();

		int i = pos / 64;
		int a = pos % 64;
		++pos;

		if (lastReadIdx != i) {
			if (i >= totalInBuf) {
				readLong = readLast;
			} else {
				readLong = buf.get(i);
			}
			lastReadIdx = i;
		}

		//System.out.println(readLong);

		return ((readLong & masksRead[a]) != 0);
	}

	/**
	 * 
	 * @param bits
	 * @return
	 */
	public long getBits(int bits) {

		if (bits > 64)
			throw new IllegalArgumentException();

		if ((bits + pos) > numOfBits) 
			throw new IllegalArgumentException();
		if (bits == 0)
			return 0;

		long r = 0;
		for (int i = 0; i < bits; ++i) {
			if (getBit()) {
				//r |= 1L << (bits - i - 1);
				r |= masksSet[bits - i - 1];
			}
		}
		return r;
	}
}