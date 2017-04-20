package org.wowcoders.tspark.utils;

import com.facebook.util.digest.MurmurHash;

public class Hash {
	static MurmurHash mh = MurmurHash.createRepeatableHasher();
	public static String bytesToHex(byte[] bytes) {
	    final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	    char[] hexChars = new char[bytes.length * 2];
	    int v;
	    for ( int j = 0; j < bytes.length; j++ ) {
	        v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	public static String hash(String str) {
		byte [] hashByets = mh.hash(str.getBytes());
		return bytesToHex(hashByets);
	}
	
	public static long hashCode(String str) {
		return mh.hashToLong(str.getBytes());
	}
}