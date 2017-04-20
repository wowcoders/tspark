package org.wowcoders.beringei.compression;

import java.nio.ByteBuffer;

public  class BitStream {

	private final byte[] bytes;
	private final int numOfBits;
	private int pos;

	public BitStream(ByteBuffer buf) {
		bytes = new byte[buf.remaining()];
		int p = buf.position();
		buf.get(bytes);
		buf.position(p);
		this.numOfBits = bytes.length * 8;
		this.pos = 0;
	}

	public long getBit() {
		if (pos >= numOfBits)
			throw new IllegalStateException();

		int i = pos / 8;
		int a = pos % 8 + 1;
		++pos;
		return (bytes[i] >> (8 - a)) & 0x1;
	}

	public long getBits(int bits) {
		if (bits > 64)
			throw new IllegalArgumentException();
		if (bits + pos > numOfBits)
			throw new IllegalArgumentException();
		if (bits == 0)
			return 0;

		long r = 0;
		for (int i = 0; i < bits; ++i) {
			r |= (getBit() << (bits - i - 1));
		}
		return r;
	}

	public long peekBits(int bits) {
		int p = pos;
		long r = getBits(bits);
		pos = p;
		return r;
	}

	public int getRemaining() {
		return numOfBits - pos;
	}
}