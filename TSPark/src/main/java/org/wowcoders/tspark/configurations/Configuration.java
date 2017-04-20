package org.wowcoders.tspark.configurations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.tspark.utils.Pair;

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
				is = Configuration.class.getResourceAsStream("/tspark-demo.properties");
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

	public static class APIConfig {
		boolean enabled = false;

		boolean enabledTLS = false;
		Pair<String, Integer> listenAddress = null;

		int threadsCnt = 2;

		public APIConfig(Properties prop) {
			enabled = Boolean.parseBoolean(prop.getProperty("api.enabled"));

			if (enabled) {
				enabledTLS =  Boolean.parseBoolean(prop.getProperty("api.enable-tls"));
				String listen = prop.getProperty("api.listen");
				String []addrport = listen.split(":");
				listenAddress = new Pair<String, Integer>(addrport[0], Integer.parseInt(addrport[1]));
				
				threadsCnt = Integer.parseInt(prop.getProperty("api.threads-cnt"));
			}
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("enabled:" + enabled).append("; ");
			if (enabled) {
				sb.append("tls:"+enabledTLS)
				.append("; ");
				sb.append("(Address:"+listenAddress.first)
				.append("; ").append("port:"+listenAddress.second).append("); ");;
			}
			return sb.toString();
		}

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public boolean isEnabledTLS() {
			return enabledTLS;
		}

		public void setEnabledTLS(boolean enabledTLS) {
			this.enabledTLS = enabledTLS;
		}

		public Pair<String, Integer> getListenAddress() {
			return listenAddress;
		}

		public void setListenAddress(Pair<String, Integer> listenAddress) {
			this.listenAddress = listenAddress;
		}
		
		public int getThreadsCnt() {
			return threadsCnt;
		}

		public void setThreadsCnt(int threadsCnt) {
			this.threadsCnt = threadsCnt;
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
	
	
	/*******************************/
	
	APIConfig apiConfig = null;
	TagsCacheConfig tagsCacheConfig = null;
	
	public void load() {
		apiConfig = new APIConfig(prop);
		tagsCacheConfig = new TagsCacheConfig(prop);
	}
	
	/*******************************/
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("api config: " + apiConfig).append("\n");
		sb.append("tag cache config: " + tagsCacheConfig).append("\n");
		
		return sb.toString();
	}
	
	private static Configuration instance = new Configuration();
	
	public static void loadConfig() {
		instance.load();
		logger.info(instance.toString());
	}
	
	public APIConfig getApiConfig() {
		return apiConfig;
	}

	public TagsCacheConfig getTagsCacheConfig() {
		return tagsCacheConfig;
	}
	
	public static Configuration getInstnace() {
		if (instance.apiConfig == null) {
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