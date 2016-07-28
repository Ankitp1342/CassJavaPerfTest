package main.java.perftest;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.codahale.metrics.MetricRegistry;

import main.java.perftest.bean.ClientBean;
import main.java.perftest.session.PerfTestSession;

public class WriteTest {
	public static Logger logger = Logger.getLogger(WriteTest.class);
	
	private static Random random = new Random();
	
	public static final CountDownLatch startSignal;
    public static final CountDownLatch doneSignal;
    public static final Integer writeThreads;
    public static final Integer writeTestDurationSeconds;
    public static final ExecutorService service;
    public static final Long writeEndTime;
    private static AtomicLong count = new AtomicLong(0);
    private static AtomicLong errorCount = new AtomicLong(0);
    static{
    	 Integer writeThreadz = Integer.parseInt(System.getProperty("writeThreads"));
    	 if(writeThreadz==null || writeThreadz<0){
    		 writeThreadz = 1;
    	 }
    	 writeThreads = writeThreadz;
    	 
    	 Integer testDuration = Integer.parseInt(System.getProperty("writeTestDurationSeconds"));
    	 if(testDuration==null || testDuration<0){
    		 testDuration = 60;
    	 }
    	 writeTestDurationSeconds = testDuration;
    	 
    	 startSignal = new CountDownLatch(1);
		 doneSignal = new CountDownLatch(writeThreads);
		 
		 service = Executors.newFixedThreadPool(writeThreads);
		 writeEndTime = System.currentTimeMillis() + (writeTestDurationSeconds*1000l);
    }
	
    
    public static void main(String args[]){
    	long time = System.currentTimeMillis();
		PerfTestSession.getInstance().initInsertMetrics();
    	execute();
		startSignal.countDown();
		try{
			doneSignal.await();
		}catch(Exception e){
			logger.error("Error",e);
		}
		time = System.currentTimeMillis()-time;
		logger.info("Total time to execute test: " + time + " Total Writes: "+ count);
		logger.info("Estimated op/sec: " + ((double)count.get()/((double)time/1000.0)));
		logger.info("Total Errors: " + errorCount.get() + " ErrorRate: "+ (errorCount.get()/(double)count.get()));
		logger.error("Done with all Write tests.");
		
		try{
			PerfUtil.writer.close();
		}catch(Exception e){
			logger.error("Error",e);
		}
		PerfUtil.reporter.report();
		PerfUtil.metrics.remove(MetricRegistry.name(PerfTestSession.class, "inserts"));
		PerfTest.doneSignal.countDown();
		
		
	}
    
	private static void execute(){
		 for(int i=0;i<writeThreads;i++){
			 service.execute(new WriteRunnable());
		 }
	}
	
	
	private static class WriteRunnable implements Runnable{
		public WriteRunnable(){
			
		}
		public void run(){
			while(true){
				try{
					startSignal.await();
					ClientBean object = WriteTest.generatePayload();
					boolean success = PerfTestSession.getInstance().insert(object);
					if(!success){
						errorCount.incrementAndGet();
					}
					count.incrementAndGet();
					PerfUtil.writeKey(object.getA());
				}catch(Exception e){
					logger.error("Error " , e);
				}finally{
					if(System.currentTimeMillis()>writeEndTime){
						doneSignal.countDown();
						logger.info("Write Test Ended. Remaining: " + doneSignal.getCount());
						break;
					}
				}
			}
		}
	}
	
	public static ClientBean generatePayload(){
		
		ClientBean availibility = new ClientBean();
		availibility.setA(UUID.randomUUID().toString().hashCode());
		availibility.setB(UUID.randomUUID().toString());
		availibility.setC(UUID.randomUUID().toString());
		availibility.setD(new Date(System.currentTimeMillis()));
		availibility.setE(UUID.randomUUID().toString().hashCode());
		availibility.setF(UUID.randomUUID().toString().hashCode());
		availibility.setG(UUID.randomUUID().toString());
		return availibility;
	}

}
