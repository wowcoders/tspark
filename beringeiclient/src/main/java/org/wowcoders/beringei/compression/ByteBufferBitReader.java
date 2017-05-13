package org.wowcoders.beringei.compression;

import java.nio.ByteBuffer;

/**
 * 
 * @author vmukumar
 *
 */
public class ByteBufferBitReader implements BitStream {
	public static int []masksRead = {
			128,  64, 32, 16, 8, 4, 2, 1
	};

	public static long []masksSet = new long[64];

	static {
		for (int i= 0; i < 64; i++) {
			long mask = 1L << i;
			masksSet[i] = mask;
		}
	}


	private final ByteBuffer buf;

	private final long numOfBits;
	private int pos;

	private int lastReadIdx = -1;
	private byte readByte = 0;

	/**
	 * 
	 * @param buf
	 */
	public ByteBufferBitReader(ByteBuffer buf) {
		this.buf = buf;

		int totalBytes = buf.remaining();
		this.numOfBits = totalBytes * 8;

		this.pos = 0;
	}

	/**
	 * 
	 * @return
	 */
	public boolean getBit() {
		if (pos >= numOfBits)
			throw new IllegalStateException();

		int i = pos / 8;
		int a = pos % 8;
		++pos;

		if (lastReadIdx != i) {
			readByte = buf.get(i);
			lastReadIdx = i;
		}

		return ((readByte & masksRead[a]) != 0);
	}

	/**
	 * 
	 * @param bits
	 * @return
	 */
	public long getBits(int bits) {
		if (bits > 64)
			throw new IllegalArgumentException();
		if (bits + pos > numOfBits)
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