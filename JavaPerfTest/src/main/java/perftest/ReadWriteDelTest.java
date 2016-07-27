package main.java.perftest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import main.java.perftest.bean.ClientBean;
import main.java.perftest.session.PerfTestSession;

public class ReadWriteDelTest  {
	public static Logger logger = Logger.getLogger(ReadWriteDelTest.class);
	
	private static Random random = new Random();
	
	public static final CountDownLatch startSignal;
    public static final CountDownLatch doneSignal;
    public static final Integer mixedThreads;
    public static final Integer mixedTestDurationSeconds;
    public static final ExecutorService service;
    public static final Long mixedEndTime;
    private static AtomicLong insertCount = new AtomicLong(0);
    private static AtomicLong deleteCount = new AtomicLong(0);
    private static AtomicLong readCount = new AtomicLong(0);
    
    private static AtomicLong readTime = new AtomicLong(0);
    private static AtomicLong insertTime = new AtomicLong(0);
    private static AtomicLong deleteTime = new AtomicLong(0);
    
    private static Integer insertsPerThread;
    private static Integer deletesPerThread;
    private static Integer readsPerThread;
    private static final List<Operation> operations;
    private static enum Operation{
    	INSERT,DELETE,READ
    }
    static{
    	 Integer writeThreadz = Integer.parseInt(System.getProperty("mixedThreads"));
    	 if(writeThreadz==null || writeThreadz<0){
    		 writeThreadz = 1;
    	 }
    	 mixedThreads = writeThreadz;
    	 
    	 Integer testDuration = Integer.parseInt(System.getProperty("mixedTestDurationSeconds"));
    	 if(testDuration==null || testDuration<0){
    		 testDuration = 60;
    	 }
    	 mixedTestDurationSeconds = testDuration;
    	 
    	 startSignal = new CountDownLatch(1);
		 doneSignal = new CountDownLatch(mixedThreads);
		 
		 service = Executors.newFixedThreadPool(mixedThreads);
		 mixedEndTime = System.currentTimeMillis() + (mixedTestDurationSeconds*1000l);
		 
		 
		 insertsPerThread = 0;
		 deletesPerThread = 0;
		 readsPerThread = 0;
		 
		 try{
			 insertsPerThread = Integer.parseInt(System.getProperty("insertsPerThread"));
		 }catch(Exception e){}

		 try{
			 deletesPerThread = Integer.parseInt(System.getProperty("deletesPerThread"));
		 }catch(Exception e){}

		 try{
			 readsPerThread = Integer.parseInt(System.getProperty("readsPerThread"));
		 }catch(Exception e){}
		 
		 
		 logger.info("InsertsPerThread: " + insertsPerThread);
		 logger.info("DeletsPerThread: " + insertsPerThread);
		 logger.info("ReadsPerThread: " + insertsPerThread);
		 
		 operations = new ArrayList<Operation>();
		 
		 for(int i=0;i<insertsPerThread;i++){
			 operations.add(Operation.INSERT);
		 }
		 for(int i=0;i<deletesPerThread;i++){
			 operations.add(Operation.DELETE);
		 }
		 for(int i=0;i<readsPerThread;i++){
			 operations.add(Operation.READ);
		 }
		 
    }
	
    
    public static void main(String args[]){
    	PerfUtil.ratioReadKeys();
    	PerfUtil.readDeletes();
    	long time = System.currentTimeMillis();
		PerfTestSession.getInstance().initInsertMetrics();
		PerfTestSession.getInstance().initDeleteMetrics();
		PerfTestSession.getInstance().initSelectMetrics();
    	execute();
		startSignal.countDown();
		try{
			doneSignal.await();
		}catch(Exception e){
			logger.error("Error",e);
		}
		time = System.currentTimeMillis()-time;
		long totalOps = insertCount.get()+deleteCount.get()+readCount.get();
		long totalTime = insertTime.get()+deleteTime.get()+readTime.get();
		logger.info("Total Absolute time to execute mixed test: " + time);
		logger.info("Total Estimated op/sec: " + ((double)totalOps/((double)time/1000.0)));
		logger.info("Total Relative time to execute mixed test: " + totalTime + " Total Operations: "+ totalOps  + " Total Threads: " + mixedThreads);
		logger.info("Normalized Total Relative op/sec: " + ((double)totalOps/(((double)totalTime/(double)mixedThreads)/1000.0)));
		
		
		
		
		logger.info("Total normalized time to execute Writes: " + ((double)insertTime.get()/(double)mixedThreads) + " Total Writes: "+ insertCount);
		logger.info("Normalized Total Write op/sec: " + ((double)insertCount.get()/((double)insertTime.get()/1000.0)));
		
		logger.info("Total normalized time to execute Deletes: " + ((double)deleteTime.get()/(double)mixedThreads) + " Total Deletes: "+ deleteCount);
		logger.info("Normalized Total Delete op/sec: " + ((double)deleteCount.get()/(((double)deleteTime.get()/(double)mixedThreads)/1000.0)));
		
		logger.info("Total normalized time to execute Reads: " + ((double)readTime.get()/(double)mixedThreads) + " Total Reads: "+ readCount);
		logger.info("Normalized Total Read op/sec: " + ((double)readCount.get()/(((double)readTime.get()/(double)mixedThreads)/1000.0)));
		
		logger.error("Done with all Mixed tests.");
		
		try{
			PerfUtil.writer.close();
		}catch(Exception e){
			logger.error("Error",e);
		}
		PerfUtil.reporter.report();
		PerfTest.doneSignal.countDown();
		
		
	}
    
	private static void execute(){
		 for(int i=0;i<mixedThreads;i++){
			 service.execute(new MixedRunnable());
		 }
	}
	
	
	private static class MixedRunnable implements Runnable{
		private int opCount=0;
		
		public void run(){
			while(true){
				try{
					startSignal.await();
					
					if(operations.get(opCount%operations.size())==Operation.INSERT){
						ClientBean object = WriteTest.generatePayload();
						long time = System.currentTimeMillis();
						PerfTestSession.getInstance().insert(object);
						insertTime.addAndGet(System.currentTimeMillis()-time);
						insertCount.incrementAndGet();
						PerfUtil.writeKey(object.getA());
						
					}else if(operations.get(opCount%operations.size())==Operation.DELETE){
						Integer object = PerfUtil.toDeleteObjects.poll(1, TimeUnit.MILLISECONDS);
						if(object!=null){
							long time = System.currentTimeMillis();
							List deletes = PerfTestSession.getInstance().delete(object);
							deleteTime.addAndGet(System.currentTimeMillis()-time);
							if(deletes!=null){
								deleteCount.addAndGet(deletes.size());
							}else{
								deleteCount.incrementAndGet();
							}
						}
					}else if(operations.get(opCount%operations.size())==Operation.READ){
						Integer key = PerfUtil.cacheKeys.poll(1, TimeUnit.MILLISECONDS);
						if(key!=null){
							long time = System.currentTimeMillis();
							PerfTestSession.getInstance().select(key);
							readTime.addAndGet(System.currentTimeMillis()-time);
							readCount.incrementAndGet();
							
						}
					}
					
				}catch(Exception e){
					logger.error("Error " , e);
				}finally{
					opCount++;
					if(System.currentTimeMillis()>mixedEndTime){
						doneSignal.countDown();
						logger.info("Mixed Test Ended. Remaining: " + doneSignal.getCount());
						break;
					}
				}
			}
		}
	}

}