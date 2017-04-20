package org.wowcoders.tspark.qs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowcoders.tspark.models.Aggregators;
import org.wowcoders.tspark.models.TSDBQueries;
import org.wowcoders.tspark.models.TSDBReq;
import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.models.Topo;
import org.wowcoders.tspark.tags.AtomixDistributedStore;
import org.wowcoders.tspark.utils.Pair;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import io.atomix.collections.DistributedMap;

public class GrafanaTSDB2RequestHandler {
	final static Logger logger = LoggerFactory.getLogger(GrafanaTSDB2RequestHandler.class);
	private static TaggedTS taggedTS = null;
	@SuppressWarnings("serial")
	public static class Tsdb2QueryAggServlet extends HttpServlet
	{
		static final String []agg = Aggregators.getAggregators();
		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
		{
			Gson gson = new Gson();
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("application/json; charset=utf-8");
			OutputStream os = response.getOutputStream();
			JsonWriter jw = new JsonWriter(new OutputStreamWriter(os, "UTF-8"));
			gson.toJson(agg, String[].class, jw);
			jw.flush();
			jw.close();
			os.close();
		}
	}

	@SuppressWarnings("serial")
	public static class Tsdb2QuerySuggestServlet extends HttpServlet
	{
		static final String []tagv = {"slc", "login", "loginhost1000"};
		static final String []tagk = {"pool", "colo", "host"};
		static final String []metrics = {"logincount"};
		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
		{
			String limitStr = request.getParameter("max"); 
			
			int _limit = 100;
			if (limitStr != null) {
				_limit = Integer.parseInt(limitStr);
			}
			
			String query = request.getParameter("q"); 
			String type = request.getParameter("type"); 

			DistributedMap<Object, Object> map = null;
			if (type.equals("tagv")) {
				map = AtomixDistributedStore.tagv;
			} else if (type.equals("tagk")){
				map = AtomixDistributedStore.tagk;
			} else if (type.equals("metrics")) {
				map = AtomixDistributedStore.metrics;
			}

			Set<Object> _keys = map.keySet().join();
			List <String> list = _keys.stream().parallel().map(strObj->(String)strObj)
					.filter(str->str.contains(query))
					.sorted().limit(_limit).collect(Collectors.toList());
			try {
				sendResponse(response, list);
			} catch (IOException e) {
				e.printStackTrace();
			}

			sendResponse(response, list);
		}

		void sendResponse(HttpServletResponse response, List <String> list) throws IOException {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("application/json; charset=utf-8");

			Gson gson = new Gson();

			OutputStream os = response.getOutputStream();
			JsonWriter jw = new JsonWriter(new OutputStreamWriter(os, "UTF-8"));

			gson.toJson(list.toArray(), String[].class, jw);

			jw.flush();
			jw.close();
			os.close();
			os.close();
		}
	}


	@SuppressWarnings("serial")
	public static class Tsdb2QueryServlet extends HttpServlet
	{
		//Gson gson = new Gson();
		//TSDB tsdb = Client.getTsdb(environment);

		//ExtensionRegistry protobufExtensionRegistry = tsdb.getStringOrdinalConverter().getRegistry();
		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
		{
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println("Use Post");
		}

		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
		{
			Gson gson = new Gson();
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("application/json");
			response.setHeader("Content-Encoding", "gzip");
			TSDBReq q = gson.fromJson(request.getReader(), TSDBReq.class);

			try {
				OutputStream os = response.getOutputStream();
				GZIPOutputStream gzip = new GZIPOutputStream(os);
				JsonWriter jw = new JsonWriter(new OutputStreamWriter(gzip, "UTF-8"));
				jw.beginArray();
				long _start = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
				List<TSKey> tsArr = new ArrayList<TSKey>();
				long end = q.getEnd();
				if (end == 0) {
					end = System.currentTimeMillis();
				}
				final long endms = end;
				long start = q.getStart();

				List <TSDBQueries> tsdbQueries = q.getQuery();
				for(TSDBQueries tsdbQuery: tsdbQueries)
				{
					TSDBQueries tq = tsdbQuery;

					Topo topo = new Topo();
					HashMap <String, String> tagsIn = tq.getTags();
					tagsIn.entrySet().stream().forEach(entry-> {
						topo.add(entry.getKey(), entry.getValue());
					});

					Aggregators.fromString(tq.getAggregator());

					TSKey tsKey = new TSKey(Aggregators.fromString(tq.getAggregator()), tq.getMetric(), topo);
					// tsAvg._hash();
					tsArr.add(tsKey);

					logger.info("====ts access input begin====");
					logger.info("start:" + q.getStart() + ", end:" + end);
					logger.info("agg:metric -> " + tq.getAggregator()  + ":" + tq.getMetric());
					logger.info("====ts access input end====");
				}

				List<CompletableFuture<List<BeringeiQSResponse>>> cfs = new ArrayList<CompletableFuture<List<BeringeiQSResponse>>>();

				for(TSKey _tsKey: tsArr) {
					CompletableFuture<List<BeringeiQSResponse>> cf = taggedTS.getData(q.getStart()/1000, end/1000, _tsKey);
					cfs.add(cf);
				}

				logger.info("Waiting multiple metrics results..");

				//List<DataPoint> dps = cf1.join();
				//System.out.println(dps);

				CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).thenAccept(x -> {
					cfs.stream().forEach(metric -> {
						List<BeringeiQSResponse> series = metric.join();
						series.stream().forEach(dps-> {
							TSKey tsKey = dps.getMeta();
							StringBuilder sb = new StringBuilder();
							try {
								jw.beginObject();

								Iterator<Pair<Long, Double>> idps = dps.getDps().iterator();

								jw.name("metric");
								jw.value(tsKey.getMetric());

								jw.name("aggregator");
								jw.value(tsKey.getAggregator().getAggregator());

								jw.name("tags");
								jw.beginObject();
								List<Pair<String, String>> tags = tsKey.getTags();
								for(Pair<String, String> tag : tags) {
									jw.name(tag.first);
									jw.value(tag.second);
								}
								jw.endObject();

								jw.name("dps");
								jw.beginObject();
								while(idps.hasNext()) {
									Pair<Long, Double> dp = idps.next(); 
									long ms = dp.first * 1000;
									// System.out.println(ms+" start:"+ start + ", end:" + endms);
									if (start <= ms && ms <= endms) {
										sb.setLength(0);
										sb.append(dp.first);
										jw.name(sb.toString());
										jw.value(dp.second);

									}
								}
								jw.endObject();
								jw.endObject();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						});
					});
					long _end = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
					logger.info("Overall TimeTaken:" + (_end - _start) + "(micros)");
					try {
						jw.endArray();
						jw.flush();
						gzip.finish();
						jw.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//gson.toJson(q, TSDBReq.class, jw);

				}).exceptionally(tw->{
					return null;
				}).join();

			} catch(Exception e) {
				//				slf4jLogger.info(StringUtils.traceToString(e));
			}
		}
	}
	
	public static void addServeletHander(ServletContextHandler context, TaggedTS taggedTS) {
		GrafanaTSDB2RequestHandler.taggedTS = taggedTS;
		context.addServlet(new ServletHolder(new Tsdb2QueryServlet()), "/tsdb/api/query");
		context.addServlet(new ServletHolder(new Tsdb2QueryAggServlet()), "/tsdb/api/aggregators");
		context.addServlet(new ServletHolder(new Tsdb2QuerySuggestServlet()), "/tsdb/api/suggest");
	}
}