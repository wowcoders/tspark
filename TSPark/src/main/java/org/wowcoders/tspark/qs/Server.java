package org.wowcoders.tspark.qs;

import java.io.FileInputStream;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.nio.*;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.wowcoders.beringeiclient.BeringeiClient;
import org.wowcoders.tspark.TS;
import org.wowcoders.tspark.configurations.Configuration;
import org.wowcoders.tspark.models.Topo;
import org.wowcoders.tspark.tags.MetaDataCollector;

public class Server {
	static org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server();
	static BeringeiClient client = null;
	static TaggedTS taggedTS = null;

	static TS tsCli = null;

	private static void setAccessControlAllowOrigin(ServletContextHandler context) {
		FilterHolder holder = new FilterHolder(CrossOriginFilter.class);
		holder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
		//holder.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "http://127.0.0.1:4200");
		holder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,POST,PUT,HEAD");
		holder.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "X-Requested-With,Content-Type,Accept,Origin");
		holder.setName("cross-origin");
		FilterMapping fm = new FilterMapping();
		fm.setFilterName("cross-origin");
		fm.setPathSpec("*");
		context.getServletHandler().addFilter(holder, fm);
	}

	private static void listenQS() throws Exception {
		Configuration cfg = Configuration.getInstnace();
		
		if (cfg.getApiConfig().isEnabled()) {
			SelectChannelConnector connector1 = new SelectChannelConnector();
			connector1.setHost(cfg.getApiConfig().getListenAddress().first);
			connector1.setPort(cfg.getApiConfig().getListenAddress().second);
			connector1.setThreadPool(new org.eclipse.jetty.util.thread.QueuedThreadPool(cfg.getApiConfig().getThreadsCnt()));
			connector1.setName("topo-query");
	
			server.setConnectors(new Connector[]{ connector1 });
	
	
			ServletContextHandler context=new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
			server.setHandler(context);
	
			setAccessControlAllowOrigin(context);
			
			GrafanaTSDB2RequestHandler.addServeletHander(context, taggedTS);
			
			HttpIngesterHandler.addServeletHander(context, taggedTS, tsCli);
			
			server.start();
		}
	}

	public static void main(String []args) throws Exception {
		
		String configFileName = null;
		Options options = new Options();
		Option optThreads = new Option("c", "config-file", true, "a config file with config value.");
		options.addOption(optThreads);

		CommandLineParser parser = new PosixParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			formatter.printHelp("Server", options);
		}

		String _propertyFile = cmd.getOptionValue("c");

		if (_propertyFile != null) {
			configFileName = _propertyFile;
		}
		
		org.wowcoders.beringeiclient.configurations.Configuration.init(configFileName);
		Configuration.init(configFileName);
		
		@SuppressWarnings("unused")
		Configuration cfg = Configuration.getInstnace();
		
		MetaDataCollector.init();

		client = new BeringeiClient();
		taggedTS = new TaggedTS(client);
		tsCli = new TS(client);

		new Thread() {// second metric emitter
			public void run() {
				while(true) {
					int hosts = 5;

					long ms = System.currentTimeMillis() / 1000;

					for(int host = 1; host <= hosts; host++) {
						Topo topo = new Topo();
						topo.add("pool", "login");
						topo.add("datacenter", "aws-location1");
						topo.add("host", "loginhost" + host);
						int count = (int) (Math.random() *100);
						tsCli.addTSCnt("logincount", topo, ms, count);
					}

					tsCli.flush();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		}.start();

		listenQS();
		server.join();
	}
}