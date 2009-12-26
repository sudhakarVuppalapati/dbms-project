package myDB;

public final class MyHashFunctions {
	
	/**
	 * Based on Robert Jenkins' bit Mix Function, version in Java developed
	 * by Thomas Wang (http://www.concentric.net/~Ttwang/tech/inthash.htm) 
	 * Copyright (c) 2007 January 
	 * @param key the input key, as 32-bit integer
	 * @return the hash value
	 */
	public static int hash32shiftmult(int key)
	{
	  int c2=0x27d4eb2d; // a prime or an odd constant
	  key = (key ^ 61) ^ (key >>> 16);
	  key = key + (key << 3);
	  key = key ^ (key >>> 4);
	  key = key * c2;
	  key = key ^ (key >>> 16);
	  return key;
	}
}
