package org.wowcoders.beringeiclient.configurations;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.beringeiclient.utils.Pair;
import org.wowcoders.beringeiclient.configurations.Configuration;

//TODO - Use the Standard Configuration Builder
public class Configuration {
	final static Logger logger = LoggerFactory.getLogger(Configuration.class);

	private static Properties prop = null;
	
	public static void init(String configFile) {
		InputStream is = null;
		try {
			prop = new Properties();
			if (configFile != null) {
				is = new FileInputStream(configFile);
			} else {
				is = Configuration.class.getResourceAsStream("/beringeiclient-demo.properties");
			}
			prop.load(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Configuration(){

	}

	static class ShardConfig {
		boolean writeOnMultiDataCenter = false;
		boolean writeOnAllHosts = false;

		public ShardConfig(Properties prop) {
			writeOnMultiDataCenter = Boolean.parseBoolean(prop.getProperty("shards.multi-datacenter-write", "false"));
			writeOnAllHosts = Boolean.parseBoolean(prop.getProperty("shards.write-on-all-hosts-in-same-datacenter", "false"));
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("write on multidatacenter enabled:" + writeOnMultiDataCenter).append("; ");
			sb.append("write on all hosts enabled:" + writeOnMultiDataCenter).append("; ");
			return sb.toString();
		}
		
		public boolean isWriteOnMultiDataCenter() {
			return writeOnMultiDataCenter;
		}

		public void setWriteOnMultiDataCenter(boolean writeOnMultiDataCenter) {
			this.writeOnMultiDataCenter = writeOnMultiDataCenter;
		}

		public boolean isWriteOnAllHosts() {
			return writeOnAllHosts;
		}

		public void setWriteOnAllHosts(boolean writeOnAllHosts) {
			this.writeOnAllHosts = writeOnAllHosts;
		}
	}

	public static class TagsCacheConfig {
		long ttl = 604800000;
		Pair<String, Integer> listenAddress = null;

		List<Pair<String, Integer>> cluster = new ArrayList<Pair<String, Integer>>();

		public TagsCacheConfig(Properties prop) {
			ttl = Long.parseLong(prop.getProperty("atomix.cache-tags.ttl-ms"));

			String listen = prop.getProperty("atomix.cache-tags.listen");
			if (listen != null) {
				String []addrport = listen.split(":");
				listenAddress = new Pair<String, Integer>(addrport[0], Integer.parseInt(addrport[1]));
			}

			String clusterStr = prop.getProperty("atomix.cache-tags.cluster");
			if (clusterStr != null) {
				String []clusterArr = clusterStr.split(",");
				for(String _cluster: clusterArr) {
					String []ip_port = _cluster.split(":");
					Pair<String, Integer> caddress = new Pair<String, Integer>(ip_port[0], Integer.parseInt(ip_port[1]));
					cluster.add(caddress);
				}
			}
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("ttl:" + ttl).append("; ");
			if (listenAddress != null) {
				sb.append("(Address:"+listenAddress.first)
				.append("; ").append("port:"+listenAddress.second).append("); ");
			}

			if (cluster != null) {
				cluster.stream().forEach(addr-> {
					sb.append("(Address:"+addr.first)
					.append("; ").append("port:"+addr.second).append("); ");
				});
			}
			return sb.toString();
		}
		
		public long getTtl() {
			return ttl;
		}

		public void setTtl(long ttl) {
			this.ttl = ttl;
		}

		public Pair<String, Integer> getListenAddress() {
			return listenAddress;
		}

		public void setListenAddress(Pair<String, Integer> listenAddress) {
			this.listenAddress = listenAddress;
		}

		public List<Pair<String, Integer>> getCluster() {
			return cluster;
		}

		public void setCluster(List<Pair<String, Integer>> cluster) {
			this.cluster = cluster;
		}
	}
	
	public static class ClientsConfig {
		long connectTimeout = 600;
		long readTimeout = 60000;
		long writeTimeout = 60000;
		
		int idleConnectionsPerShards = 1;

		long timeoutToCloseIdleConnection = 40000;
		
		int connectionCountPerShards = 2;
		
		int shardCounts = 100;
		
		String[] datacenters = null;
		
		HashMap<String, HashSet<Pair<String, Integer>>> datacenterHostListMap = new HashMap<String, HashSet<Pair<String, Integer>>>();
		
		int flushInterval = 10000;
		int flushAfterCount = 1000;
		int readThreads = 10;
		int writeThreads = 10;
		public ClientsConfig(Properties prop) {
			connectTimeout = Long.parseLong(prop.getProperty("clients.connect-timeout-ms"));
			readTimeout = Long.parseLong(prop.getProperty("clients.read-timeout-ms"));
			writeTimeout = Long.parseLong(prop.getProperty("clients.write-timeout-ms"));
			
			idleConnectionsPerShards = Integer.parseInt(prop.getProperty("clients.active-idle-connections-per-shards"));
			timeoutToCloseIdleConnection = Long.parseLong(prop.getProperty("clients.timeout-to-close-idle-connections-in-seconds"));
			connectionCountPerShards = Integer.parseInt(prop.getProperty("clients.connection-count-per-shards"));
			
			shardCounts = Integer.parseInt(prop.getProperty("clients.cluster-shards-count", "100"));
			datacenters = prop.getProperty("clients.datacenters", "").split(",");
			
			for (String datacenter: datacenters) {
				String endpointsStr = prop.getProperty("clients." + datacenter + ".cluster-endpoints", "");
				String []endpoints = endpointsStr.split(",");
				HashSet<Pair<String, Integer>> endpointSet = datacenterHostListMap.get(datacenter);
				if (endpointSet == null) {
					endpointSet = new HashSet<Pair<String, Integer>>();
					datacenterHostListMap.put(datacenter, endpointSet);
				}
				for(String endpoint: endpoints) {
					String []ip_port = endpoint.split(":");
					Pair<String, Integer> caddress = new Pair<String, Integer>(ip_port[0], Integer.parseInt(ip_port[1]));
					endpointSet.add(caddress);
				}
			}
			
			flushInterval = Integer.parseInt(prop.getProperty("clients.flush.when-metric-count-in-queue"));
			flushAfterCount = Integer.parseInt(prop.getProperty("clients.flush.interval"));
			readThreads = Integer.parseInt(prop.getProperty("clients.read-threads"));
			writeThreads = Integer.parseInt(prop.getProperty("clients.write-threads"));
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append("connectTimeout:"+connectTimeout).append("; ")
			.append("readTimeout:"+readTimeout).append("; ")
			.append("writeTimeout:"+writeTimeout).append("; ");
		
			sb.append("idleConnectionsPerShards:"+idleConnectionsPerShards).append("; ")
			.append("timeoutToCloseIdleConnection:"+timeoutToCloseIdleConnection).append("; ")
			.append("connectionCountPerShards:"+connectionCountPerShards).append("; ");
			
			sb.append("shardCnt:"+shardCounts).append("; ")
			.append("datacenters:"+Arrays.toString(datacenters)).append("; ");
			
			return sb.toString();
		}
		
		public long getConnectTimeout() {
			return connectTimeout;
		}

		public void setConnectTimeout(long connectTimeout) {
			this.connectTimeout = connectTimeout;
		}

		public long getReadTimeout() {
			return readTimeout;
		}

		public void setReadTimeout(long readTimeout) {
			this.readTimeout = readTimeout;
		}

		public long getWriteTimeout() {
			return writeTimeout;
		}

		public void setWriteTimeout(long writeTimeout) {
			this.writeTimeout = writeTimeout;
		}

		public int getIdleConnectionsPerShards() {
			return idleConnectionsPerShards;
		}

		public void setIdleConnectionsPerShards(int idleConnectionsPerShards) {
			this.idleConnectionsPerShards = idleConnectionsPerShards;
		}

		public long getTimeoutToCloseIdleConnection() {
			return timeoutToCloseIdleConnection;
		}

		public void setTimeoutToCloseIdleConnection(long timeoutToCloseIdleConnection) {
			this.timeoutToCloseIdleConnection = timeoutToCloseIdleConnection;
		}

		public int getConnectionCountPerShards() {
			return connectionCountPerShards;
		}

		public void setConnectionCountPerShards(int connectionCountPerShards) {
			this.connectionCountPerShards = connectionCountPerShards;
		}
		
		public int getShardCounts() {
			return shardCounts;
		}

		public void setShardCounts(int shardCounts) {
			this.shardCounts = shardCounts;
		}

		public String[] getDatacenters() {
			return datacenters;
		}

		public void setDatacenters(String[] datacenters) {
			this.datacenters = datacenters;
		}

		public HashMap<String, HashSet<Pair<String, Integer>>> getDatacenterHostListMap() {
			return datacenterHostListMap;
		}

		public void setDatacenterHostListMap(HashMap<String, HashSet<Pair<String, Integer>>> datacenterHostListMap) {
			this.datacenterHostListMap = datacenterHostListMap;
		}
		
		public int getFlushInterval() {
			return flushInterval;
		}

		public void setFlushInterval(int flushInterval) {
			this.flushInterval = flushInterval;
		}

		public int getFlushAfterCount() {
			return flushAfterCount;
		}

		public void setFlushAfterCount(int flushAfterCount) {
			this.flushAfterCount = flushAfterCount;
		}

		public int getReadThreads() {
			return readThreads;
		}

		public void setReadThreads(int readThreads) {
			this.readThreads = readThreads;
		}

		public int getWriteThreads() {
			return writeThreads;
		}

		public void setWriteThreads(int writeThreads) {
			this.writeThreads = writeThreads;
		}
	}
	
	/*******************************/
	ShardConfig shardConfig = null;
	ClientsConfig clientConfig = null;
	
	public void load() {
		shardConfig = new ShardConfig(prop);
		clientConfig = new ClientsConfig(prop);
	}
	
	/*******************************/
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("shard config: " + shardConfig).append("\n");
		sb.append("clients cache config: " + clientConfig).append("\n");
		
		return sb.toString();
	}
	
	private static Configuration instance = new Configuration();
	
	public static void loadConfig() {
		instance.load();
		logger.info(instance.toString());
	}

	public ShardConfig getShardConfig() {
		return shardConfig;
	}

	public ClientsConfig getClientConfig() {
		return clientConfig;
	}

	public static Configuration getInstnace() {
		if (instance.shardConfig == null) {
			synchronized(instance) {
				loadConfig();
			}
		}
		return instance;
	}
	/*******************************/
	
	public static void main(String a[]) {
		loadConfig();
	}
}