package com.uniclick.databank.mapreduce.redis;



import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
/**
 * �?��性hash�?算出hash的核心算�?
 * @author ping jie
 *
 */

public class HashFunction {
	//private MessageDigest md5 = null;
	private java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();  
	public long hash(String key) {
		/*if (md5 == null) {
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException("no md5 algorythm found");
			}
		}
		md5.reset();
		md5.update(key.getBytes());
		byte[] bKey = md5.digest();
		long res = ((long) (bKey[3] & 0xFF) << 24)
				| ((long) (bKey[2] & 0xFF) << 16)
				| ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
		return res;*/
		crc32.reset();
        crc32.update(key.getBytes());  
        return crc32.getValue();
	}
}
