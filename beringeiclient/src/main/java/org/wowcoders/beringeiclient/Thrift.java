package org.wowcoders.beringeiclient;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.facebook.beringei.thriftclient.BeringeiService.AsyncClient;
import com.facebook.beringei.thriftclient.BeringeiService.AsyncClient.Factory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class Thrift {

    // This is the request
    public static abstract class ThriftRequest {
    	Thrift thrift;
    	AsyncClient cli;
    	
        private void go(final Thrift thrift, final AsyncClient cli) {
        	this.thrift = thrift;
        	this.cli = cli;
            on(cli);
            //thrift.ret(cli);  // TODO < fix : this line is causing for : Client is currently executing another method>
        }
        
        protected void taskDone() {
        	thrift.ret(cli); // TODO < fix : this line is causing for : Client is currently executing another method>
        }

        public abstract void on(AsyncClient cli);
    }

    // Holds all of our Async Clients
    private final ConcurrentLinkedQueue<AsyncClient> instances = new ConcurrentLinkedQueue<AsyncClient>();
    // Holds all of our postponed requests
    private final ConcurrentLinkedQueue<ThriftRequest> requests = new ConcurrentLinkedQueue<ThriftRequest>();
    // Holds our executor, if any
    private Executor exe = null;

    /**
     * This factory runs in thread bounce mode, meaning that if you call it from
     * many threads, execution bounces between calling threads depending on when
     * execution is needed.
     * @throws TTransportException 
     */
    public Thrift(
            final int clients,
            final int clients_per_message_processing_thread,
            final String host,
            final int port) throws IOException, TTransportException {

        // We only need one protocol factory
        TProtocolFactory proto_fac = new TProtocolFactory() {
			private static final long serialVersionUID = 1L;

			@Override
			public TProtocol getProtocol(TTransport trans) {
				return new TBinaryProtocol( new TFramedTransport(trans) );
			}
        };
        // Create our clients
        Factory fac = null;
        for (int i = 0; i < clients; i++) {

            if (fac == null || i % clients_per_message_processing_thread == 0) {
            	TAsyncClientManager asm = new TAsyncClientManager();
                fac = new AsyncClient.Factory(asm, proto_fac);
            }
            TNonblockingSocket tnbs = new TNonblockingSocket(host, port);
            instances.add(fac.getAsyncClient(tnbs));
        }
    }

    /**
     * This factory runs callbacks in whatever mode the executor is setup for,
     * not on calling threads.
     * @throws TTransportException 
     */
    public Thrift(Executor exe,
                  final int clients,
                  final int clients_per_message_processing_thread,
                  final String host,
                  final int port) throws IOException, TTransportException {
        this(clients, clients_per_message_processing_thread, host, port);
        this.exe = exe;
    }

    // Call this to grab an instance
    public void req(final ThriftRequest req) {
        final AsyncClient cli;
        synchronized (instances) {
            cli = instances.poll();
        }
        if (cli != null) {
            if (exe != null) {
                // Executor mode
                exe.execute(new Runnable() {

                    @Override
                    public void run() {
                        req.go(Thrift.this, cli);
                    }

                });
            } else {
                // Thread bounce mode
                req.go(this, cli);
            }
            return;
        }
        // No clients immediately available
        requests.add(req);
    }

    private void ret(final AsyncClient cli) {
        final ThriftRequest req;
        synchronized (requests) {
            req = requests.poll();

        }

        if (req != null) {
            if (exe != null) {
                // Executor mode
                exe.execute(new Runnable() {

                    @Override
                    public void run() {
                        req.go(Thrift.this, cli);
                    }
                });
            } else {
                // Thread bounce mode
                req.go(this, cli);
            }
            return;
        }
        instances.add(cli);
    }
}