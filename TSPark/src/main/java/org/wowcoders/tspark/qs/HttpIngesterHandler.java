package org.wowcoders.tspark.qs;

import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.wowcoders.tspark.TS;
import org.wowcoders.tspark.models.Aggregators;
import org.wowcoders.tspark.models.TSKey;
import org.wowcoders.tspark.models.Topo;

import com.google.gson.stream.JsonReader;

public class HttpIngesterHandler {
	@SuppressWarnings("unused")
	private static TaggedTS taggedTS = null;
	
	private static TS tsCli = null;
	
	public static void addServeletHander(ServletContextHandler context, TaggedTS taggedTS, TS tsCli) {
		HttpIngesterHandler.taggedTS = taggedTS;
		HttpIngesterHandler.tsCli = tsCli;
		
		context.addServlet(new ServletHolder(new InternalsUpload()), "/internals/api/datapointsupload");
	}
	
	@SuppressWarnings("serial")
	public static class InternalsUpload extends HttpServlet
	{
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
		{
			response.setStatus(HttpServletResponse.SC_OK);

			Topo topo = new Topo();
			TSKey tsKey = new TSKey(topo);

			JsonReader jr = new JsonReader(new InputStreamReader(request.getInputStream(), "UTF-8"));
			jr.beginArray();
			while (jr.hasNext()) {
				jr.beginObject();
				String metric = null;
				String aggregator = null;
				while (jr.hasNext()) {
					String name = jr.nextName();
					if (name.equals("aggregator")) {
						aggregator = jr.nextString();
						System.out.println(aggregator);
					} else if (name.equals("metric")) {
						metric = jr.nextString();
						System.out.println(metric);
					} else if (name.equals("tags")) {
						jr.beginObject();
						tsKey.setMetric(metric);
						tsKey.setAggregator(Aggregators.fromString(aggregator));
						tsKey.clear();
						while (jr.hasNext()) {
							String tagk = jr.nextName();
							String tagv = jr.nextString();
							System.out.println(tagk+" "+tagv);
							topo.add(tagk, tagv);
						}
						jr.endObject();	
					} else if (name.equals("dps")) {
						jr.beginObject();
						while (jr.hasNext()) {
							String ts = jr.nextName();
							long ut = Long.parseLong(ts);
							double value = jr.nextDouble();
							System.out.println(ut+" "+value);
							tsCli.pushTS(tsKey, ut, value);
						}
						jr.endObject();
					} else {
						jr.skipValue();
					}
				}
				jr.endObject();
			}
			jr.endArray();
			jr.close();
		}
	}
}