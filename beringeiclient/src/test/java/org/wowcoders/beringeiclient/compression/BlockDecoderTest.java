package org.wowcoders.beringeiclient.compression;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.wowcoders.beringei.compression.BlockDecoder;

import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.Key;
import com.facebook.beringei.thriftclient.TimeSeriesBlock;

public class BlockDecoderTest {
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	@Test
	public void decodeEmptyBlock() {
		Key key = new Key();
		key.key = "testkey";
		key.shardId = 100;
		TimeSeriesBlock tsd = new TimeSeriesBlock();
		byte [] b =new byte[100];
		tsd.setData(b);
		tsd.setCount(0);
		BlockDecoder bd = new BlockDecoder(key, tsd);
		
		List<DataPoint> dps = bd.readDps();

		assertEquals(0, dps.size());
	}
	
	@Test
	public void decode2DataPointsBlock() {
		String hex="B20A83A305080A5C167814D0292468C0";
		Key key = new Key();
		key.key = "testkey";
		key.shardId = 100;
		TimeSeriesBlock tsd = new TimeSeriesBlock();
		byte [] b = hexStringToByteArray(hex);
		tsd.setData(b);
		tsd.setCount(4);
		BlockDecoder bd = new BlockDecoder(key, tsd);
		
		List<DataPoint> dps = bd.readDps();
		
		assertEquals(4, dps.size());

		//1493516753, 75.0
		DataPoint dp = dps.get(0);
		assertEquals(key, dp.key);
		assertEquals(1493516753, dp.value.unixTime);
		assertEquals(75.0, dp.value.value, 0);

		//1493516754, 79.0
		dp = dps.get(1);
		assertEquals(key, dp.key);
		assertEquals(1493516754, dp.value.unixTime);
		assertEquals(79.0, dp.value.value, 0);
		
		//1493516755 95.0
		dp = dps.get(2);
		assertEquals(key, dp.key);
		assertEquals(1493516755, dp.value.unixTime);
		assertEquals(95.0, dp.value.value, 0);
		
		//1493516756 31.0
		dp = dps.get(3);
		assertEquals(key, dp.key);
		assertEquals(1493516756, dp.value.unixTime);
		assertEquals(31.0, dp.value.value, 0);
	}
}