package org.wowcoders.beringei.compression;

/**
 * 
 * @author vmukumar
 *
 */
public interface BitStream {
	/**
	 * to read a bit
	 * @return 
	 */
	public long getBit();

	/**
	 * reads given number of bits and returns as long
	 * @param bits number of bits to read
	 * @return
	 */
	public long getBits(int bits);
}