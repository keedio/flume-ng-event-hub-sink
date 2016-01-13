package org.keedio.flume.sink.azure.eventhub;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubSender;

import static org.keedio.flume.sink.azure.eventhub.EventHubSinkConstants.*;


public class EventHubSinkUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(EventHubSinkUtil.class);
	private final String  policyName, policyKey, namespace, eventHubName, partitionId;
	private final int batchSize;
	
	public EventHubSinkUtil(Context context) {
		
		batchSize = context.getInteger(BATCH_SIZE,DEFAULT_BATCH_SIZE);
		partitionId = context.getString(PARTITION_ID);
		policyName = context.getString(POLICY_NAME);
		policyKey = context.getString(POLICY_KEY);
		namespace = context.getString(NAMESPACE);
		eventHubName = context.getString(EVENTHUB_NAME);
		checkMandatoryProperties(context);
	}
	
	private void checkMandatoryProperties(Context context) {
		if (policyName == null || policyKey == null){
			throw new ConfigurationException(POLICY_NAME + " or " + POLICY_KEY + " property not set");
		}
		if (namespace == null || eventHubName == null){
			throw new ConfigurationException(NAMESPACE + " or " + EVENTHUB_NAME + " property not set");
		}	
	}

	public EventHubSender createSender() {
		
		try {
			EventHubClient client = EventHubClient.create(policyName, policyKey, namespace, eventHubName);
			EventHubSender sender = client.createPartitionSender(partitionId);
			
			return sender;
			
		} catch (EventHubException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getBatchSize() {
		return batchSize;
	}
	
	
}
