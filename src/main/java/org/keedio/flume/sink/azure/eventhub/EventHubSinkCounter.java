package org.keedio.flume.sink.azure.eventhub;

import org.apache.flume.instrumentation.SinkCounter;

public class EventHubSinkCounter extends SinkCounter implements EventHubSinkCounterMBean {

	public EventHubSinkCounter(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

}
