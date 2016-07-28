package main.java.perftest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.codahale.metrics.MetricRegistry;

import main.java.perftest.session.PerfTestSession;

public class ReadTest {

	
	public static Logger logger = Logger.getLogger(ReadTest.class);
	
	public static final CountDownLatch startSignal;
    public static final CountDownLatch doneSignal;
    public static final Integer readThreads;
    public static final Integer readTestDurationSeconds;
    public static final ExecutorService service;
    public static final Long readEndTime;
    public static final String readKeyFilePath;
    private static AtomicLong count = new AtomicLong(0);
    static{
    	 Integer writeThreadz = Integer.parseInt(System.getProperty("readThreads"));
    	 if(writeThreadz==null || writeThreadz<0){
    		 writeThreadz = 1;
    	 }
    	 readThreads = writeThreadz;
    	 
    	 Integer testDuration = Integer.parseInt(System.getProperty("readTestDurationSeconds"));
    	 if(testDuration==null || testDuration<0){
    		 testDuration = 60;
    	 }
    	 readTestDurationSeconds = testDuration;
    	 
    	 startSignal = new CountDownLatch(1);
		 doneSignal = new CountDownLatch(readThreads);
		 
		 service = Executors.newFixedThreadPool(readThreads);
		 readEndTime = System.currentTimeMillis() + (readTestDurationSeconds*1000l);
		 
		 readKeyFilePath = System.getProperty("readKeyFilePath");
    }
    
    
   public static void main(String args[]){
	   long time = System.currentTimeMillis();
	   PerfTestSession.getInstance().initSelectMetrics();
    	execute();
		startSignal.countDown();
		try{
			doneSignal.await();
		}catch(Exception e){
			logger.error("Error",e);
		}
		time = System.currentTimeMillis()-time;
		logger.info("Total time to execute test: " + time + " Total Reads: "+ count);
		logger.info("Estimated op/sec: " + ((double)count.get()/((double)time/1000.0)));
		logger.error("Done with all Read tests.");
		PerfUtil.reporter.report();
		PerfUtil.metrics.remove(MetricRegistry.name(PerfTestSession.class, "selectPartitionMetrics"));
		PerfTest.doneSignal.countDown();
		
		
	}
   
   private static void execute(){
		 for(int i=0;i<readThreads;i++){
			 service.execute(new ReadRunnable());
		 }
   }
   
	private static class ReadRunnable implements Runnable{
		public void run(){
			while(true){
				try{
					startSignal.await();
					Integer key = PerfUtil.cacheKeys.poll(1, TimeUnit.MILLISECONDS);
					if(key!=null){
						PerfTestSession.getInstance().select(key);
						count.incrementAndGet();
					}
					
				}catch(Exception e){
					logger.error("Error " , e);
				}finally{
					if(System.currentTimeMillis()>readEndTime || PerfUtil.cacheKeys.isEmpty()){
						doneSignal.countDown();
						logger.info("Read Test Ended. Remaining: " + doneSignal.getCount());
						break;
					}
				}
			}
		}
	}
}
