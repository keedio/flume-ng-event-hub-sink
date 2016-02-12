package org.keedio.flume.sink.azure.eventhub;

public interface EventHubSinkCounterMBean {

	public void increaseCounterMessageSent();

	public void increaseCounterMessageSentError();

	public long getCounterMessageSent();

	public long getCounterMessageSentError();

	public long getCurrentThroughput();

	public long getAverageThroughput();
}
