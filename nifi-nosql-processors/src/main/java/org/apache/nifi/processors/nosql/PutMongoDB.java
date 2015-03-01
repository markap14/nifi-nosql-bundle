package org.apache.nifi.processors.nosql;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.nosql.util.WriteConcernEnum;
import org.apache.nifi.stream.io.StreamUtils;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

public class PutMongoDB extends AbstractMongoDBProcessor {

	public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
		.name("Collection Name")
		.description("The name of the Collection in the MongoDB database to add the data to")
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.required(true)
		.expressionLanguageSupported(true)
		.build();
	public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
		.name("Write Concern")
		.description("The Write Concern to use when putting objects to MongoDB. This value can affect performance of the processor as well as the reliability. See the MongoDB documentation for more information.")
		.allowableValues(WriteConcernEnum.values())
		.defaultValue(WriteConcernEnum.ACKNOWLEDGED.name())
		.required(true)
		.build();

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> props = new ArrayList<>();
		props.add(HOSTNAME);
		props.add(PORT);
		props.add(DATABASE_NAME);
		props.add(COLLECTION_NAME);
		props.add(WRITE_CONCERN);
		props.add(TIMEOUT);
		return props;
	}

	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}
		
		final long startNanos = System.nanoTime();
		final String dbName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		final MongoClient client = getClient();
		final DB db = client.getDB(dbName);
		
		final WriteConcern writeConcern = WriteConcernEnum.valueOf(context.getProperty(WRITE_CONCERN).getValue()).getWriteConcern();
		final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
		
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream rawIn) throws IOException {
				try (final InputStream in = new BufferedInputStream(rawIn)) {
					StreamUtils.copy(in, baos);
				}
			}
		});
		
		final String json = new String(baos.toByteArray(), StandardCharsets.UTF_8);
		final DBObject dbObject;
		try {
			dbObject = (DBObject) JSON.parse(json);
		} catch (final JSONParseException jpe) {
			getLogger().error("Failed to send {} to MongoDB because it is not valid JSON; routing to failure", new Object[] {flowFile});
			flowFile = session.putAttribute(flowFile, "mongodb.failure.reason", "not valid json");
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
		
		try {
			db.getCollection(collectionName).insert(dbObject, writeConcern);
			final long nanos = System.nanoTime() - startNanos;
			final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
			getLogger().info("Successfully inserted {} into the {} collection of database {}", new Object[] {flowFile, collectionName, dbName});
			session.getProvenanceReporter().send(flowFile, "mongodb://" + context.getProperty(HOSTNAME).getValue() + ":" + context.getProperty(PORT).getValue() + "/" + dbName + "/" + collectionName, millis);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (final MongoException me) {
			getLogger().error("Failed to insert {} into {} collection of database {} due to {}", new Object[] {flowFile, collectionName, dbName, me});
			session.transfer(flowFile, REL_FAILURE);
		}
	}

}
