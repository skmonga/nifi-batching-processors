package org.apache.nifi.ALPRProcessor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
//import org.apache.storm.shade.org.apache.commons.io.IOUtils;

/**
 * This uses wrapper to convert microbatch of size > 1 
 * to a file and send the resulting output downstream.
 *
 */
@Tags({ "ALPRProcessor" })
@CapabilityDescription("Calls ALPR script.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ALPRScriptProcessorMultiElementBatch extends AbstractProcessor {

	public static final PropertyDescriptor SCRIPT_TYPE = new PropertyDescriptor.Builder().name("sript_type")
			.displayName("Script type").description("Script Type").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor FILEPATH = new PropertyDescriptor.Builder().name("filepath")
			.displayName("filepath").description("filepath").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor SCRIPT_NAME = new PropertyDescriptor.Builder().name("script_name")
			.displayName("Script name").description("Script name").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File")
			.description("File path to wherever the experiment logs are written")
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


	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	
	byte[] imageInByte;
	Properties props;
	PrintWriter writer;
	String scriptType;
	String scriptName;
	String fileName;
	String msgId;
	String entryOrExit;
//	String con;
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(SCRIPT_NAME);
		descriptors.add(SCRIPT_TYPE);
		descriptors.add(FILEPATH);
		descriptors.add(EXPERIMENT_LOG_FILE);
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
			scriptName = context.getProperty(SCRIPT_NAME).getValue();
			scriptType = context.getProperty(SCRIPT_TYPE).getValue();
			fileName = context.getProperty(FILEPATH).getValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException  {
		FlowFile flowfile = session.get();
		
		if(flowfile==null) {
			return;
		}
		openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						ALPRScriptProcessorMultiElementBatch.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
		
		String currentKey = flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key());

		
		entryOrExit = flowfile.getAttribute("entry_exit");
		
		
		//Moving this part into the wrapper code
		/*session.read(flowfile, new InputStreamCallback() {
			
			public void process(InputStream in) throws IOException {
				imageInByte = IOUtils.toByteArray(in);
				FileUtils.writeByteArrayToFile(new File(fileName), imageInByte);
			}
		});
		
		session.remove(flowfile);
		StringBuilder processOut = new StringBuilder();
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(scriptName + " " + fileName);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			//System.out.println("Here is the standard output of the command:\n");
	        while ((s = stdInput.readLine()) != null) {
	        	processOut.append(s);
	        }
		} catch(Exception e) {
			
		}*/
		
		MicroBatchToFileWrapper conversionWrapper = 
				new MicroBatchToFileWrapper(session, getLogger(), flowfile);
		List<FlowFile> flowFiles = new ArrayList<>();
		try {
			flowFiles = conversionWrapper.convert(scriptName, fileName);
		} catch (IOException e) {
			//need proper error handling
			getLogger().error(e.getMessage());
		}
		
		for (FlowFile f : flowFiles) {
			f = session.putAttribute(f, "entry_exit", entryOrExit);
			f = session.putAttribute(f, "type", scriptType);

			session.transfer(f, success);
			expLogStream.println(String.format("%s,%s,%s,%s", f.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
					ALPRScriptProcessorMultiElementBatch.class, (new Timestamp(System.currentTimeMillis())).getTime(),
					"EXIT"));
		}
		expLogStream.flush();
	}
	
}
