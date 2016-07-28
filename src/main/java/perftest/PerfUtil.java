package main.java.perftest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;


public class PerfUtil {

	public static Logger logger = Logger.getLogger(PerfTest.class);
	public static final MetricRegistry metrics;
//	public static final Slf4jReporter reporter;
	public static final CsvReporter reporter;
	public static BufferedWriter writer;
    public static final BlockingQueue<Integer> cacheKeys = new LinkedBlockingQueue<Integer>();
    public static final BlockingQueue<Integer> toDeleteObjects = new LinkedBlockingQueue<Integer>();
    
	public static final ConsistencyLevel readConsistency;
	public static final ConsistencyLevel writeConsistency;
	static{
		
		metrics = new MetricRegistry();
		reporter = CsvReporter.forRegistry(metrics)
                .formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(new File(System.getProperty("metricsFolderPath")));

		
		Integer reportTime = Integer.parseInt(System.getProperty("reportTimeSeconds"));
		if(reportTime==null || reportTime<=0){
			reportTime = 30;
		} 
		reporter.start(reportTime, TimeUnit.SECONDS);

		
		Charset charset = Charset.forName("US-ASCII");
		try{
			writer = Files.newBufferedWriter(Paths.get(System.getProperty("keyFilePath")), charset, StandardOpenOption.APPEND);
		}catch(Exception e){
			logger.error("Error" , e);
			System.exit(1);
		}
			
		if("local_one".equalsIgnoreCase(System.getProperty("readConsistency"))){
			readConsistency = ConsistencyLevel.LOCAL_ONE;
		}else{
			readConsistency = ConsistencyLevel.LOCAL_QUORUM;
		}
		
		if("local_one".equalsIgnoreCase(System.getProperty("writeConsistency"))){
			writeConsistency = ConsistencyLevel.LOCAL_ONE;
		}else{
			writeConsistency = ConsistencyLevel.LOCAL_QUORUM;
		}
		
		logger.info("READ CONSISTENCY: " + readConsistency);
		logger.info("WRITE CONSISTENCY: " + writeConsistency);
	}
	public static void ratioReadKeys(){
		
		Charset charset = Charset.forName("US-ASCII");
		try {
			BufferedReader reader = Files.newBufferedReader(Paths.get(System.getProperty("randomKeyFilePath")), charset);
		    String line = null;
		    int count =0;
		    List<Integer> keys= new ArrayList<Integer>();
		    while ((line = reader.readLine()) != null) {
		    	try{
		    		if(count%5==2){
		    			keys.add(Integer.parseInt(line));
		    		}else{
		    			keys.add(Integer.parseInt(line)+1);
		    		}
		    		count++;
		    	}catch(Exception e){
		    		
		    	}
		    }
		    
		    Collections.shuffle(keys);
		    Collections.shuffle(keys);
		    Collections.shuffle(keys);
		    
		    cacheKeys.addAll(keys);
		    
		} catch (IOException x) {
		   logger.error("IOException: %s%n", x);
		}
		
		logger.info("Read keys: " + cacheKeys.size());
		
	}

	public static void readDeletes(){
		
		Charset charset = Charset.forName("US-ASCII");
		try {
			BufferedReader reader = Files.newBufferedReader(Paths.get(System.getProperty("deleteKeyFilePath")), charset);
		    String line = null;
		    int count =0;
		    List<Integer> keys= new ArrayList<Integer>();
		    while ((line = reader.readLine()) != null) {
		    	try{
		    		if(count%5==2){
		    			keys.add(Integer.parseInt(line));
		    		}else{
		    			keys.add(Integer.parseInt(line)+1);
		    		}
		    		count++;
		    	}catch(Exception e){
		    		
		    	}
		    }
		    
		    Collections.shuffle(keys);
		    Collections.shuffle(keys);
		    Collections.shuffle(keys);
		    
		    toDeleteObjects.addAll(keys);
		    
		} catch (IOException x) {
		   logger.error("IOException: %s%n", x);
		}
		
		logger.info("Read toDeleteObject: " + toDeleteObjects.size());
		
	}
	
	public static void readKeys(){
		
		Charset charset = Charset.forName("US-ASCII");
		try {
			BufferedReader reader = Files.newBufferedReader(Paths.get(System.getProperty("keyFilePath")), charset);
		    String line = null;
		    
		    while ((line = reader.readLine()) != null) {
		    	try{
		        cacheKeys.add(Integer.parseInt(line));
		    	}catch(Exception e){
		    		
		    	}
		    }
		} catch (IOException x) {
		   logger.error("IOException: %s%n", x);
		}
		
		logger.info("Read keys: " + cacheKeys.size());
		
	}
	public static void writeKey(Integer key){
		
		cacheKeys.add(key);
		try{
		    writer.write(key.toString());
		    writer.write("\n");
		} catch (IOException x) {
		    logger.error("IOException: %s%n", x);
		}
	}
}
