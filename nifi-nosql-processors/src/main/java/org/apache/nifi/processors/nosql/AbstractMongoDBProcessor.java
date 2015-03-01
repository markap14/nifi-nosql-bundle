package org.apache.nifi.processors.nosql;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public abstract class AbstractMongoDBProcessor extends AbstractProcessor {

	public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
		.name("Hostname")
		.description("The hostname of the MongoDB instance to connect to")
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.required(true)
		.build();
	public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
		.name("Port")
		.description("The port that MongoDB is listening on")
		.defaultValue("27017")
		.addValidator(StandardValidators.PORT_VALIDATOR)
		.required(true)
		.build();
	public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
		.name("Timeout")
		.description("Timeout used for communicating with MongoDB server")
		.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
		.required(true)
		.defaultValue("30 secs")
		.build();
	public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
		.name("Database Name")
		.description("The name of the MongoDB database to connect to")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(true)
		.build();
	
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles will be routed to 'success' if they are processed as expected").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles will be routed to 'failure' if they are unable to be processed for any reason").build();
	
	private volatile MongoClient client;
	
	
	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		return relationships;
	}
	
	@OnScheduled
	public void onScheduled(final ProcessContext context) throws UnknownHostException {
		final String hostname = context.getProperty(HOSTNAME).getValue();
		final int port = context.getProperty(PORT).asInteger().intValue();
		final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
		
		final MongoClientOptions opts = new MongoClientOptions.Builder()
			.connectTimeout(timeout)
			.socketTimeout(timeout)
			.socketKeepAlive(true)
			.alwaysUseMBeans(false)
			.build();
		client = new MongoClient(new ServerAddress(hostname, port), opts);
	}
	
	
	@OnStopped
	public void onStopped() {
		if ( client != null ) {
			client.close();
			client = null;
		}
	}
	
	protected MongoClient getClient() {
		final MongoClient c = client;
		if ( c == null ) {
			throw new IllegalStateException("Client has not yet been initialized");
		}
		return c;
	}
}
