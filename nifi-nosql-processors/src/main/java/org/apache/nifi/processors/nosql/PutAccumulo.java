/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.nosql;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

public class PutAccumulo extends AbstractProcessor {

	public static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
		.name("Instance Name")
		.description("The name of the Accumulo instance")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();

	public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
	    .name("ZooKeeper Connection String")
	    .description("The Connection String to use in order to connect to ZooKeeper. This is often a comma-separated list of <host>:<port> combinations. For example, host1:2181,host2:2181,host3:2188")
	    .required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
	    .build();
	public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
		.name("Username")
		.description("The user name for connecting to Accumulo")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
		.name("Password")
		.description("The password for connecting to Accumulo")
		.required(true)
		.sensitive(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
		.name("Table")
		.description("The table to write the data to")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
		.name("Row ID")
		.description("The Row ID for the data being inserted into Accumulo")
		.required(true)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
		.name("Column Family")
		.description("The Column Family for the data being inserted into Accumulo")
		.required(true)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor COLUMN_QUALIFIER = new PropertyDescriptor.Builder()
		.name("Column Qualifier")
		.description("The Column Qualifier for the data being inserted into Accumulo")
		.required(true)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor VISIBILITY = new PropertyDescriptor.Builder()
		.name("Column Visibility")
		.description("The Column Visibility for the data being inserted into Accumulo")
		.required(false)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
		.name("Max Buffer Size")
		.description("Specifies the maximum amount of data that any one task should buffer before sending to Accumulo")
		.required(true)
		.defaultValue("10 MB")
		.addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
		.build();
	public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
		.name("Communcations Timeout")
		.description("The amount of time to wait for a response before failing the data transfer")
		.required(true)
		.defaultValue("30 secs")
		.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
		.build();
	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
		.name("Batch Size")
		.description("Maximum number of FlowFiles to send in a single batch")
		.required(true)
		.defaultValue("100")
		.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
		.build();
	
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("FlowFiles that are successfully sent to Accumulo will be routed to this relationship")
		.build();
	public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("Any FlowFile that cannot be sent to Accumulo will be routed to this relationship")
		.build();
	
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(INSTANCE_NAME);
		properties.add(ZOOKEEPER_CONNECTION_STRING);
		properties.add(USER);
		properties.add(PASSWORD);
		properties.add(TABLE);
		properties.add(ROW_ID);
		properties.add(COLUMN_FAMILY);
		properties.add(COLUMN_QUALIFIER);
		properties.add(VISIBILITY);
		properties.add(BUFFER_SIZE);
		properties.add(TIMEOUT);
		return properties;
	}
	
	
	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		return relationships;
	}
	
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}
		
		final StopWatch stopWatch = new StopWatch(true);
		final String instanceName = context.getProperty(INSTANCE_NAME).getValue();
		final String zookeeperString = context.getProperty(ZOOKEEPER_CONNECTION_STRING).getValue();
		final Instance instance = new ZooKeeperInstance(instanceName, zookeeperString);

		final String username = context.getProperty(USER).getValue();
		final String password = context.getProperty(PASSWORD).getValue();
		final String table = context.getProperty(TABLE).getValue();
		
		final Connector connector;
		try {
			connector = instance.getConnector(username, new PasswordToken(password));
		} catch (final AccumuloSecurityException e) {
			getLogger().error("Failed to connect to Accumulo due to {}", e);
			session.transfer(flowFile, REL_FAILURE);
			return;
		} catch (final AccumuloException e) {
			getLogger().error("Failed to create connection to Accumulo due to {}", e);
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
		
		final BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxWriteThreads(1);
		config.setMaxMemory(context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).longValue());
		config.setTimeout(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
		
		final BatchWriter writer;
		try {
			writer = connector.createBatchWriter(table, config);
		} catch (final TableNotFoundException e) {
			getLogger().error("Failed to send data to Accumulo because table '" + table + "' does not exist");
			session.transfer(flowFile, REL_FAILURE);
			return;
		}

		final Set<FlowFile> flowFilesSent = new HashSet<>();
		final int maxBatchSize = context.getProperty(BATCH_SIZE).asInteger();
		final Charset charset = StandardCharsets.UTF_8;
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int sentCount = 0;
		long bytesSent = 0L;
		
		final Map<FlowFile, String> transitUris = new HashMap<>();
		
		try {
			try {
				while ( flowFile != null ) {
					try {
						final String rowId = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
						final String columnFamily = context.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue();
						final String columnQualifier = context.getProperty(COLUMN_QUALIFIER).evaluateAttributeExpressions(flowFile).getValue();
						
						// read the contents of the FlowFile into memory
						session.exportTo(flowFile, baos);
						final byte[] value = baos.toByteArray();
						baos.reset();
						
						final Mutation mutation = new Mutation(rowId);
						mutation.put(columnFamily.getBytes(charset), columnQualifier.getBytes(charset), value);
						
						writer.addMutation(mutation);
						transitUris.put(flowFile, "accumulo://" + table + "/" + rowId + "/" + columnFamily + "/" + columnQualifier);
						bytesSent += flowFile.getSize();
					} finally {
						flowFilesSent.add(flowFile);
					}
	
					if ( ++sentCount < maxBatchSize ) {
						flowFile = session.get();
					} else { 
						flowFile = null;
					}
				}
				
				writer.flush();
			} finally {
				writer.close();
			}
		} catch (final MutationsRejectedException mre) {
			getLogger().error("Failed to send batch of FlowFiles to Accumulo due to {}", new Object[] {flowFile, mre});
			session.transfer(flowFilesSent, REL_FAILURE);
			return;
		}
		
		session.transfer(flowFilesSent, REL_SUCCESS);
		stopWatch.stop();
		getLogger().info("Successfully transferred {} FlowFiles ({} bytes) to Accumulo in {} at a rate of {}", new Object[] {flowFilesSent.size(), bytesSent, stopWatch.getDuration(), stopWatch.calculateDataRate(bytesSent)});
		for ( final FlowFile sent : flowFilesSent ) {
			session.getProvenanceReporter().send(sent, transitUris.get(sent));
		}
	}

}
