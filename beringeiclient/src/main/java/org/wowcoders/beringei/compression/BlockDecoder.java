package org.wowcoders.beringei.compression;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.facebook.beringei.thriftclient.DataPoint;
import com.facebook.beringei.thriftclient.Key;
import com.facebook.beringei.thriftclient.TimeSeriesBlock;
import com.facebook.beringei.thriftclient.TimeValuePair;

public class BlockDecoder {
	static class TimestampEncodings {
		int bitsForValue;
		int controlValue;
		int controlValueBitLength;

		public TimestampEncodings(int bitsForValue, int controlValue, int controlValueBitLength) {
			this.bitsForValue = bitsForValue;
			this.controlValue = controlValue;
			this.controlValueBitLength = controlValueBitLength;
		}
	} 

	@SuppressWarnings("serial")
	static  List<TimestampEncodings> timestampEncodings = new ArrayList<TimestampEncodings>() {{
		add(new TimestampEncodings(7, 2, 2));
		add(new TimestampEncodings(9, 6, 3));
		add(new TimestampEncodings(12, 14, 4));
		add(new TimestampEncodings(32, 15, 4));
	}};

	Key key;
	BitStream bs;
	
	int dataPoints = 0;

	long prevTimestamp = 0;
	long prevValue = 0;

	long defaultDelta = 60;
	long prevDelta = defaultDelta;

	long previousTrailingZeros = 0;
	long previousLeadingZeros = 0;

	public BlockDecoder(Key key, TimeSeriesBlock tsb) {
		dataPoints = tsb.getCount();
		byte b[]=tsb.getData();
		ByteBuffer buffer = ByteBuffer.wrap(b);
		
		this.bs = new BitStream(buffer);
		this.key = key;
	}

	public List <DataPoint> readDps() {
		List <DataPoint> dps = new ArrayList<DataPoint>();
		for(int i = 0; i < dataPoints; i++) {
			long ts = readTimeStamp();
			double val = readValue();
			TimeValuePair tvp = new TimeValuePair();
			tvp.setUnixTime(ts);
			tvp.setValue(val);
			
			DataPoint dp = new DataPoint();
			dp.setKey(key);
			dp.setValue(tvp);
			
			// System.out.println("**"+key.getKey()+" "+ ts+" "+val);
			
			dps.add(dp);
		}
		return dps;
	}

	public long readTimeStamp() {
		if (prevTimestamp == 0) {
			prevTimestamp = bs.getBits(31);
		} else {
			int bits = 0;
			int limit = 4;

			while (bits < limit) {
				int bit = (int)bs.getBits(1);
				if (bit == 0) {
					break;
				}

				bits++;
			}

			int type = bits;
			if (type > 0) {
				int index = type - 1;
				TimestampEncodings te = timestampEncodings.get(index);
				long decodedValue = bs.getBits(te.bitsForValue);
				decodedValue -= (1l << (te.bitsForValue - 1));
				if (decodedValue >= 0) {
					decodedValue++;
				}
				prevDelta += decodedValue;
			}
			prevTimestamp += prevDelta;
		}

		return prevTimestamp;
	}

	public double readValue() {
		long nonzero = bs.getBits(1);
		if (nonzero != 0) {
			long usePreviousBlockInformation = bs.getBits(1);
			long xorValue = -1;

			if (usePreviousBlockInformation != 0) {
				xorValue = bs.getBits((int)(64 - previousLeadingZeros - previousTrailingZeros));
				xorValue <<= previousTrailingZeros;
			} else {
				long leadingZeros = bs.getBits(5);
				// System.out.println("leadingZeros" + leadingZeros);
				int blockSize  = (int) bs.getBits(6) + 1;
				// System.out.println("blockSize" + blockSize);

				previousTrailingZeros = 64 - blockSize - leadingZeros;
				// System.out.println("previousTrailingZeros" + previousTrailingZeros);
				xorValue = bs.getBits(blockSize);
				xorValue <<= previousTrailingZeros;

				previousLeadingZeros = leadingZeros;
			}
			prevValue = xorValue^prevValue;
		}
		return Double.longBitsToDouble(prevValue);
	}
}