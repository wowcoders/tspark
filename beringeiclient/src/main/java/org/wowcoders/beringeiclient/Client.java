package org.wowcoders.beringeiclient;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.wowcoders.beringeiclient.utils.Pair;

import  org.apache.thrift.transport.TTransportException;

public class Client {
	/*TODO create BeringeiClient builder to add configurations*/
	public static int numberOfBeringeiClientInstance = 100;
	public static int numberOfBeringeiAsyncMessagesPerClientInstance = 1;  /*TODO need to find a way to multiplex */
	public static int executerThreads = 5; // 0 to use the current thread
	
	Thrift pooledThriftClient = null;

	public Client(Pair<String, Integer> hostport) throws TTransportException, IOException {
		if (executerThreads > 0) {
			ExecutorService executor = Executors.newFixedThreadPool(executerThreads);
			pooledThriftClient = new Thrift(executor,
					numberOfBeringeiClientInstance,
					numberOfBeringeiAsyncMessagesPerClientInstance,  
					hostport.first,
					hostport.second);
		} else {
			pooledThriftClient = new Thrift(
					numberOfBeringeiClientInstance,
					numberOfBeringeiAsyncMessagesPerClientInstance,
					hostport.first,
					hostport.second);
		}
	}
	
	Thrift getClient() {
		return pooledThriftClient;
	}
}