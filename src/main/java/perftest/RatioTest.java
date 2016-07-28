package main.java.perftest;

import org.apache.log4j.Logger;

public class RatioTest {
	public static Logger logger = Logger.getLogger(RatioTest.class);
	
	
	public static void main(final String args[]){
		
		PerfUtil.ratioReadKeys();
		new Thread(){
			public void run(){
				WriteTest.main(args);
			}
		}.start();
		
		new Thread(){
			public void run(){
				ReadTest.main(args);
			}
		}.start();
	}
}
