package main.java.perftest.session;

import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Mapper.Option;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

import main.java.perftest.PerfUtil;
import main.java.perftest.bean.ClientBean;

public class PerfTestSession {

	public static Logger logger = Logger.getLogger(PerfTestSession.class);
	private static PerfTestSession perfTestSession;
	private final Mapper<ClientBean> mapper;
	private final Session session;
	private PreparedStatement selectPrimaryKey;
	private PreparedStatement selectPartition;
	public Timer insertMetrics;
	public Timer selectPartitionMetrics;
	public Timer deleteMetrics;
	private Integer minTTL;
	private Integer maxTTL;
	private Random random = new Random();
	
	public static PerfTestSession getInstance(){
		
		if(perfTestSession==null){
			synchronized(PerfTestSession.class){
				if(perfTestSession==null){
					perfTestSession = new PerfTestSession();
				}
			}
		}
		return perfTestSession;
	}
	private PerfTestSession(){
		Cluster cluster = Cluster.builder().addContactPoints(System.getProperty("hosts")).build();
		
		session = cluster.connect();
		mapper = new MappingManager(session).mapper(ClientBean.class);
		
		mapper.setDefaultGetOptions(Option.consistencyLevel(PerfUtil.readConsistency));
		mapper.setDefaultSaveOptions(Option.consistencyLevel(PerfUtil.writeConsistency));

		selectPartition = session.prepare(
			      "select * from test.table where a=?");
		
		minTTL = 0;
		maxTTL = 0;
		try{
			minTTL = Integer.parseInt(System.getProperty("minTTL"));
			maxTTL = Integer.parseInt(System.getProperty("maxTTL"));
		}catch(Exception e){}
//		
		logger.info("minTTL: " + minTTL + " maxTTL: " + maxTTL);
		
		
	}

	public void initSelectMetrics(){
		selectPartitionMetrics = PerfUtil.metrics.timer(MetricRegistry.name(PerfTestSession.class, "selectPartitionMetrics"));
	}
	
	public void initInsertMetrics(){
		insertMetrics = PerfUtil.metrics.timer(MetricRegistry.name(PerfTestSession.class, "inserts"));
	}
	public void initDeleteMetrics(){
		deleteMetrics = PerfUtil.metrics.timer(MetricRegistry.name(PerfTestSession.class, "deletes"));
	}
	
	public List<ClientBean> select(Object... values){
		BoundStatement boundStatement = new BoundStatement(selectPrimaryKey);
		boundStatement.setConsistencyLevel(PerfUtil.readConsistency);
		ResultSetFuture future = session.executeAsync(boundStatement.bind(values));
		ResultSet resultSet = future.getUninterruptibly();
		Result<ClientBean> results = mapper.map(resultSet);
		List<ClientBean> products = results.all();
		for(ClientBean product:products){
			System.out.println(product);
		}
		return products;
	}
	
	public void select(Integer hc){
		final Timer.Context context = selectPartitionMetrics.time();
		try{
			BoundStatement boundStatement = new BoundStatement(selectPartition);
			boundStatement.setConsistencyLevel(PerfUtil.readConsistency);
			
			ResultSetFuture future = session.executeAsync(boundStatement.bind(hc));
			ResultSet resultSet = future.getUninterruptibly();	
		}finally{
			context.stop();
		}
		
	}

	public boolean insert(ClientBean UDAvailibility){
		final Timer.Context context = insertMetrics.time();
		try{
			if((minTTL<=0 && maxTTL <=0) || maxTTL<=0 || minTTL>maxTTL){
				mapper.save(UDAvailibility,Option.consistencyLevel(PerfUtil.writeConsistency));
			}else{
				mapper.save(UDAvailibility,Option.consistencyLevel(PerfUtil.writeConsistency),Option.ttl(random.nextInt(maxTTL-minTTL) + minTTL));
			}
			
		}catch(Exception e){
			logger.error("Error",e);
			return false;
		}finally{
			context.stop();
		}
		return true;
	}
	
	public List<ClientBean> delete(Integer hc){
		final Timer.Context context = deleteMetrics.time();
		try{
			List<ClientBean> deletes = selectDelete(hc);
			
			if(deletes!=null && !deletes.isEmpty()){
				for(ClientBean delete: deletes){
					
				
					if((minTTL<=0 && maxTTL <=0) || maxTTL<=0 || minTTL>maxTTL){
						mapper.delete(delete,Option.consistencyLevel(PerfUtil.writeConsistency));
					}else{
						mapper.delete(delete,Option.consistencyLevel(PerfUtil.writeConsistency),Option.ttl(random.nextInt(maxTTL-minTTL) + minTTL));
					}
				}
			}
			
			return deletes;
			
		}catch(Exception e){
			logger.error("Error",e);
		}finally{
			context.stop();
		}
		return null;
	}
	
	public List<ClientBean> selectDelete(Integer hc){
		BoundStatement boundStatement = new BoundStatement(selectPartition);
		boundStatement.setConsistencyLevel(PerfUtil.readConsistency);
		
		ResultSetFuture future = session.executeAsync(boundStatement.bind(hc));
		ResultSet resultSet = future.getUninterruptibly();
		Result<ClientBean> results = mapper.map(resultSet);
		List<ClientBean> products = results.all();
		return products;
	}
	
	
}
