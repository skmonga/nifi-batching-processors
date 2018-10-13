package org.apache.nifi.ALPRProcessor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

public abstract class ConversionWrapper {

	protected ProcessSession session;
	
	protected ComponentLog logger;
	
	protected FlowFile inputFlowFile;
	
	public ConversionWrapper() {
		
	}
	
	public ConversionWrapper(ProcessSession ps, ComponentLog lgr, FlowFile flowFile) {
		this.session = ps;
		this.logger = lgr;
		this.inputFlowFile = flowFile;
	}
}
