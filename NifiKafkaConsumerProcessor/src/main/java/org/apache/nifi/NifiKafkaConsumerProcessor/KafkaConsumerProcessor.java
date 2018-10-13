package org.apache.nifi.NifiKafkaConsumerProcessor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;
//import com.sun.xml.internal.bind.v2.TODO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.imageio.ImageIO;

import java.io.PrintWriter;
import java.sql.Timestamp;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * This sends each message to the downstream processor
 *
 */
@Tags({ "KafkaConsumerProcessor" })
@CapabilityDescription("Subscribes to a given kafka topic and consumes messages from it.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class KafkaConsumerProcessor extends AbstractProcessor {

	public static final PropertyDescriptor CONSUMER_GROUP_NAME = new PropertyDescriptor.Builder().name("group_name")
			.displayName("Consumer group name").description("Consumer group name of the subscriber").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_ADDRESS = new PropertyDescriptor.Builder().name("kafka ip address")
			.displayName("kafka ip address").description("kafka ip address").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_PORT = new PropertyDescriptor.Builder().name("kafka port")
			.displayName("kafka port").description("kafka port").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder().name("topic")
			.displayName("Topic").description("topic name").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor ENTRY_EXIT = new PropertyDescriptor.Builder().name("entry_exit")
			.displayName("Entry/Exit").description("Give the input in small letters").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File")
			.description("File path to wherever the experiment logs are written")
			.required(true)
			.addValidator(Validator.VALID)
			.defaultValue("~/experiment.log")
			.build();
	
	public static final PropertyDescriptor CONSUME_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Consume Log File")
			.description("File path to wherever the kafka consume logs are written")
			.required(true)
			.addValidator(Validator.VALID)
			.defaultValue("~/experiment.log")
			.build();

	PrintWriter expLogStream;

	private void openFile(String filename) {
		if (expLogStream == null)
			try {
				expLogStream = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	//this logstream will be used to create a mapping of 
	//the record received from Kafka which contains the key
	// and the flowfile_id so that we can see the latency btw
	//the production of record and consumption here
	PrintWriter expLogStreamHoldUp;
	
//	private static final String consumer_file = "/consume_time.log";
	
	private void openHoldUpFile(String filename) {
		if (expLogStreamHoldUp == null)
			try {
				expLogStreamHoldUp = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	
	
	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private String topic;
	private String groupName;
	private String kafkaIp;
	private String kafkaPort;
	byte[] imageInByte;
	Properties props;
	KafkaConsumer<String, byte[]> consumer;
	public static int i=0;
	
	private AtomicReference<Integer> flowFileId = new AtomicReference<Integer>(0);
	
	String entryOrExit;
	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(CONSUMER_GROUP_NAME);
		descriptors.add(KAFKA_BROKER_ADDRESS);
		descriptors.add(KAFKA_BROKER_PORT);
		descriptors.add(TOPIC);
		descriptors.add(ENTRY_EXIT);
		descriptors.add(EXPERIMENT_LOG_FILE);
		descriptors.add(CONSUME_LOG_FILE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(success);
		this.relationships = Collections.unmodifiableSet(relationships);

	}	 

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		try {
			topic = context.getProperty(TOPIC).getValue();
			groupName = context.getProperty(CONSUMER_GROUP_NAME).getValue();
			entryOrExit = context.getProperty(ENTRY_EXIT).getValue();
			kafkaIp = context.getProperty(KAFKA_BROKER_ADDRESS).getValue();
			kafkaPort = context.getProperty(KAFKA_BROKER_PORT).getValue();
			props = new Properties();
			String conn = kafkaIp +":"+kafkaPort;
			props.put("bootstrap.servers", conn);
			props.put("group.id", groupName);
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("session.timeout.ms", "30000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
			props.put("auto.offset.reset", "earliest");
			consumer = new KafkaConsumer<String, byte[]>(props);
			consumer.subscribe(Arrays.asList(topic));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@OnStopped
	public void onStopped(){
		try{
			if(consumer != null)
				consumer.close();
		}catch(Exception e){
			System.out.println("Error in closing connection during onStopped method");
		}	
	}


	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException  {
		
		ConsumerRecords<String, byte[]> records = consumer.poll(100);
		PrintWriter writer;

		if (!records.isEmpty()) {
			for (ConsumerRecord<String, byte[]> record : records) {
				imageInByte = record.value();
				FlowFile flowFile = session.create();
				//previous use has ++i for the flowfileId
				flowFile = session.putAttribute(flowFile, ExperimentAttributes.FLOWFILE_ID.key(), Long.toString(flowFileId.getAndSet(flowFileId.get()+1)));
				openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
				openHoldUpFile(context.getProperty(CONSUME_LOG_FILE).getValue());
				expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						KafkaConsumerProcessor.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
				expLogStreamHoldUp.println(String.format("%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						record.key()));
				flowFile = session.putAttribute(flowFile, "entry_exit", entryOrExit);
				flowFile = session.putAttribute(flowFile, "msgId", Long.toString(record.offset()));
				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						out.write(imageInByte);
					}
				});
				session.transfer(flowFile, success);
				expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						KafkaConsumerProcessor.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
			    expLogStream.flush();
			    expLogStreamHoldUp.flush();
			}
		}
		
		
	}
}
