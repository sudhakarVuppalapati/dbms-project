package secondmilestone;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Experiment {

	public static boolean f;
	public static void main(String[] args) {
		int bitmask = 0x000F;
		int val = 0x2222;
		System.out.println(val & bitmask);  // prints "2"
		int val1 = 26;
		System.out.println(new Integer(val1).hashCode());
		
		float fval = 2.3f;
		System.out.println(new Float(fval).hashCode());
				
		float fval1 = 2.3f;
		System.out.println(new Float(fval1).hashCode() + " AND " + Float.floatToIntBits(fval1));
		
		double dval1 = 2.3d;
		System.out.println(new Double(dval1).hashCode() + " XAND " + Double.doubleToLongBits(dval1));
		
		String str1 = "hello world";
		System.out.println(str1.hashCode());
		
		String str2 = "Hello World";
		System.out.println(str2.hashCode());
		
		System.out.println(f);
		
		int[] a = new int[]{1,3,43,16};
		
		Object b = a;
		
		int[] c = (int[])b;
		
		c[3] = 12;
		
		System.out.println(a[3]);
		
		int w = 3;
		print(w++);
		System.out.println("Now: " + w);
		
		int[] arr1 = new int[] {1,12,3,9,10,27,42, 100, -1};
		
		System.arraycopy(arr1, 3, arr1, 2, 2);
		System.out.println("Result: ");
		for (int j = 0; j < arr1.length; j++) {
			System.out.println(arr1[j]);
		}
	
		System.out.println("@@@@@@@@@@@@@@");
		
		String hello = "index1$0$blahbalh";
		byte[] bytes = hello.getBytes();
		System.out.println(bytes.length);
		
		String ter = new String(bytes);
		System.out.println(ter);
		
		System.out.println("----------------");
		
		Random rand1 = new Random();
		
		for (int i = 0; i < 3; i++) {
			System.out.println(rand1.nextInt());
		}
		
		for (int i = 0; i < 3; i++) {
			System.out.println(rand1.nextInt());
		}
	}
	
	private static void print(int a) {
		System.out.println("When called: " + a);
	}

}
