package org.apache.nifi.NifiKafkaConsumerProcessor;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This batches multiple elements and sends the batch to the downstream
 * processor
 *
 */
@Tags({ "KafkaConsumerProcessor" })
@CapabilityDescription("Subscribes to a given kafka topic and consumes messages from it.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class KafkaConsumerProcessorBatchElements extends AbstractProcessor {

	public static final PropertyDescriptor CONSUMER_GROUP_NAME = new PropertyDescriptor.Builder().name("group_name")
			.displayName("Consumer group name").description("Consumer group name of the subscriber").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_ADDRESS = new PropertyDescriptor.Builder()
			.name("kafka ip address").displayName("kafka ip address").description("kafka ip address").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_PORT = new PropertyDescriptor.Builder().name("kafka port")
			.displayName("kafka port").description("kafka port").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder().name("topic").displayName("Topic")
			.description("topic name").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor ENTRY_EXIT = new PropertyDescriptor.Builder().name("entry_exit")
			.displayName("Entry/Exit").description("Give the input in small letters").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File").description("File path to wherever the experiment logs are written")
			.required(true).addValidator(Validator.VALID).defaultValue("~/experiment.log").build();

	public static final PropertyDescriptor CONSUME_LOG_FILE = new PropertyDescriptor.Builder().name("Consume Log File")
			.description("File path to wherever the kafka consume logs are written").required(true)
			.addValidator(Validator.VALID).defaultValue("~/experiment.log").build();

	PrintWriter expLogStream;

	private void openFile(String filename) {
		if (expLogStream == null)
			try {
				expLogStream = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	// this logstream will be used to create a mapping of
	// the record received from Kafka which contains the key
	// and the flowfile_id so that we can see the latency btw
	// the production of record and consumption here
	PrintWriter expLogStreamHoldUp;

	// private static final String consumer_file = "/consume_time.log";

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
	public static int i = 0;

	private AtomicReference<Integer> flowFileId = new AtomicReference<Integer>(0);

	String entryOrExit;

	// we are using batches of size 10
	private int batchSize = 10;
	// current batch number
	private int currentBatchNum = 0;
	
	private Map<Integer, List<byte[]>> batchContent = new HashMap<>();
	
	private Map<Integer, StringBuilder> batchElementIds = new HashMap<>();
	
	private Map<Integer, StringBuilder> batchMsgIds = new HashMap<>();
	
	private Map<Integer, StringBuilder> batchImgByteLengths = new HashMap<>();

	private ReentrantLock lock = new ReentrantLock();

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
			String conn = kafkaIp + ":" + kafkaPort;
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
	public void onStopped() {
		try {
			if (consumer != null)
				consumer.close();
		} catch (Exception e) {
			System.out.println("Error in closing connection during onStopped method");
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		ConsumerRecords<String, byte[]> records = consumer.poll(100);
		PrintWriter writer;

		if (!records.isEmpty()) {
			for (ConsumerRecord<String, byte[]> record : records) {
				imageInByte = record.value();
				openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
				openHoldUpFile(context.getProperty(CONSUME_LOG_FILE).getValue());
				String elementId = Long.toString(flowFileId.getAndSet(flowFileId.get() + 1));
				expLogStream.println(
						String.format("%s,%s,%s,%s,%s", elementId,
								KafkaConsumerProcessorBatchElements.class,
								(new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY", currentBatchNum));
				expLogStreamHoldUp.println(String.format("%s,%s",
						elementId, record.key()));

				int resultantBatch = addEntryToBatch(imageInByte, record.offset(), elementId);
				FlowFile batchResultFlowFile = null;
				if (resultantBatch != -1)
					batchResultFlowFile = doTaskLogicWithBatching(session, resultantBatch, elementId);
				/*flowFile = session.putAttribute(flowFile, "entry_exit", entryOrExit);
				flowFile = session.putAttribute(flowFile, "msgId", Long.toString(record.offset()));
				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						out.write(imageInByte);
					}
				});
				session.transfer(flowFile, success);*/
				if(batchResultFlowFile != null)
					session.transfer(batchResultFlowFile);
				expLogStream.println(
						String.format("%s,%s,%s,%s,%s", elementId,
								KafkaConsumerProcessorBatchElements.class,
								(new Timestamp(System.currentTimeMillis())).getTime(), "EXIT", currentBatchNum));
				expLogStream.flush();
				expLogStreamHoldUp.flush();
			}
		}
	}

	private int addEntryToBatch(byte[] image, long offset, String elementId) {
		lock.lock();
		int result = -1;
		if (!batchContent.containsKey(currentBatchNum)) {
			batchContent.put(currentBatchNum, new ArrayList<>());
			batchElementIds.put(currentBatchNum, new StringBuilder());
			batchMsgIds.put(currentBatchNum, new StringBuilder());
			batchImgByteLengths.put(currentBatchNum, new StringBuilder());
		}
		if (batchContent.get(currentBatchNum).size() >= batchSize) {
			result = currentBatchNum;
			currentBatchNum += 1;
			batchContent.put(currentBatchNum, new ArrayList<>());
			batchElementIds.put(currentBatchNum, new StringBuilder());
			batchMsgIds.put(currentBatchNum, new StringBuilder());
			batchImgByteLengths.put(currentBatchNum, new StringBuilder());
		}
		batchContent.get(currentBatchNum).add(image);
		batchElementIds.get(currentBatchNum).append(elementId).append(",");
		batchMsgIds.get(currentBatchNum).append(Long.toString(offset)).append(",");
		batchImgByteLengths.get(currentBatchNum).append(Long.toString(image.length)).append(",");
		lock.unlock();
		return result;
	}

	public FlowFile doTaskLogicWithBatching(ProcessSession session, int batchNum, String elementId) {
		FlowFile flowFile = session.create();
		flowFile = session.putAttribute(flowFile, ExperimentAttributes.FLOWFILE_ID.key(),
				elementId);
		flowFile = session.putAttribute(flowFile, "entry_exit", entryOrExit);
		batchElementIds.get(batchNum).replace(batchElementIds.get(batchNum).length()-1, 
				batchElementIds.get(batchNum).length(), "");
		batchMsgIds.get(batchNum).replace(batchMsgIds.get(batchNum).length()-1, 
				batchMsgIds.get(batchNum).length(), "");
		batchImgByteLengths.get(batchNum).replace(batchImgByteLengths.get(batchNum).length()-1, 
				batchImgByteLengths.get(batchNum).length(), "");
		flowFile = session.putAttribute(flowFile, "elementId", batchElementIds.get(batchNum).toString());
		flowFile = session.putAttribute(flowFile, "msgId", batchMsgIds.get(batchNum).toString());
		flowFile = session.putAttribute(flowFile, "imageLength", 
				batchImgByteLengths.get(batchNum).toString());
		flowFile = session.write(flowFile, new OutputStreamCallback() {

			public void process(OutputStream out) throws IOException {
				for(byte[] arr : batchContent.get(batchNum)) {
					out.write(arr);
				}
			}
		});
		
		return flowFile;
	}
	
}
