package org.apache.nifi.ALPRProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

/**
 * This is a wrapper for a microbatch of more than 1 elements to a file
 *
 */
public class MicroBatchToFileWrapper extends ConversionWrapper {

	public MicroBatchToFileWrapper() {
		
	}
	
	public MicroBatchToFileWrapper(ProcessSession ps, ComponentLog logger, FlowFile f) {
		super(ps,logger,f);
	}
	
	/**
	 * @param scriptName
	 *            this is the script which will be executed using second argument as its sole argument
	 * @param outputFileName
	 *            this is where we will be storing the file in the underlying file system
	 * @return
	 * @throws IOException 
	 */
	public List<FlowFile> convert(String scriptName, String outputFileName) throws IOException {
		List<FlowFile> list = new ArrayList<>();
		// there is a batch of images coming in this time
		String[] elementIds = inputFlowFile.getAttribute("elementId").split(",");
		String[] msgIds = inputFlowFile.getAttribute("msgId").split(",");
		String[] imageLengths = inputFlowFile.getAttribute("imageLength").split(",");
		
		ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
		session.read(inputFlowFile, new InputStreamCallback() {

			public void process(InputStream in) throws IOException {
				arrayWrapper.setBytes(IOUtils.toByteArray(in));
			}
		});

		session.remove(inputFlowFile);
		byte[] imageBytes = arrayWrapper.getBytes();
		int start = 0, end = 0;
		for (int i = 0; i < elementIds.length; i++) {
			end = start + Integer.parseInt(imageLengths[i]);
			byte[] currentImage = Arrays.copyOfRange(imageBytes, start, end);
			start = end;
			FileUtils.writeByteArrayToFile(new File(outputFileName), currentImage);
			StringBuilder processOut = new StringBuilder();
			try {
				Runtime rt = Runtime.getRuntime();
				Process pr = rt.exec(scriptName + " " + outputFileName);
				BufferedReader stdInput = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line = null;
				while ((line = stdInput.readLine()) != null) {
					processOut.append(line);
				}
			} catch (Exception ex) {
				logger.error(ex.getMessage());
			}
			
			FlowFile f = session.create();
			f = session.putAttribute(f, "msgId", msgIds[i]);
			f = session.putAttribute(f, ExperimentAttributes.FLOWFILE_ID.key(), elementIds[i]);
			f = session.write(f, new OutputStreamCallback() {

				public void process(OutputStream out) throws IOException {
					out.write(processOut.toString().getBytes());
				}
			});
			
			list.add(f);
		}
		
		return list;
	}
	
	private static class ByteArrayWrapper {
		byte[] bytes = new byte[0];
		
		public ByteArrayWrapper() {
			
		}

		public byte[] getBytes() {
			return bytes;
		}

		public void setBytes(byte[] bytes) {
			this.bytes = bytes;
		}
		
	}
		
}
