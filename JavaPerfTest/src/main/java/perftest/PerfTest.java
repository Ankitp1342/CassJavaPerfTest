package main.java.perftest;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class PerfTest {
	public static Logger logger = Logger.getLogger(PerfTest.class);

	public static CountDownLatch doneSignal;
	
	public static void main(String args[]){
		String mode = args!=null && args.length>0? args[0] : "mixed";
		
		if("write".equalsIgnoreCase(mode)){
			doneSignal = new CountDownLatch(1);
			logger.info("Executing write-only test");
			WriteTest.main(args);
		}else if("read".equalsIgnoreCase(mode)){
			doneSignal = new CountDownLatch(1);
			logger.info("Executing read-only test");
			PerfUtil.readKeys();
			ReadTest.main(args);
		}else if("ratio".equalsIgnoreCase(mode)){
			doneSignal = new CountDownLatch(2);
			logger.info("Executing ratio test");
			RatioTest.main(args);
		}else if("mixed".equalsIgnoreCase(mode)){
			doneSignal = new CountDownLatch(1);
			logger.info("Executing mixed test");
			ReadWriteDelTest.main(args);
		}else{
			doneSignal = new CountDownLatch(2);
			logger.info("Executing write-read test");
			WriteTest.main(args);
			ReadTest.main(args);
		}
		try{
			doneSignal.await();
		}catch(Exception e){
			logger.error("Error",e);
		}
		
		logger.info("Done with all tests");
		System.exit(0);
	}
	
	
	
	
	
	
}
