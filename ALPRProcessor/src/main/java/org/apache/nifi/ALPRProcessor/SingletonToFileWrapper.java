package org.apache.nifi.ALPRProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;

/**
 * This is a wrapper for a microbatch of single element to a file
 *
 */
public class SingletonToFileWrapper extends ConversionWrapper {

	public SingletonToFileWrapper() {

	}

	public SingletonToFileWrapper(ProcessSession ps, ComponentLog logger, FlowFile f) {
		super(ps,logger,f);
	}

	/**
	 * @param scriptName
	 *            this is the script which will be executed using second argument as its sole argument
	 * @param outputFileName
	 *            this is where we will be storing the file in the underlying file system
	 * @return
	 */
	public StringBuilder convert(String scriptName, String outputFileName) {
		session.read(inputFlowFile, new InputStreamCallback() {

			public void process(InputStream in) throws IOException {
				byte[] imageInByte = IOUtils.toByteArray(in);
				FileUtils.writeByteArrayToFile(new File(outputFileName), imageInByte);
			}
		});

		session.remove(inputFlowFile);
		StringBuilder processOut = new StringBuilder();
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(scriptName + " " + outputFileName);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line;
			while ((line = stdInput.readLine()) != null) {
				processOut.append(line);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		
		return processOut;
	}
	
}
