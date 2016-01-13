package org.keedio.flume.sink.azure.eventhub;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubSender;

public class EventHubSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(EventHubSink.class);
	
	private EventHubSinkCounter counter;
	private EventHubSinkUtil eventHubSinkUtil;
	EventHubSender sender;
	
	private List<byte[]> messageList;
	
	@Override
	public Status process() throws EventDeliveryException {
		
		Status result = Status.READY;
		Channel channel = getChannel();
		Event event = null;
		Transaction transaction = null;
		
		transaction = channel.getTransaction();
	    transaction.begin();
		
		try{
			/*
			event = channel.take();
			if(event == null) {
	            result = Status.BACKOFF;
	            //counter.incrementBatchEmptyCount();
	          } else {
	        	byte[] eventBody = event.getBody();
	  			sender.send(eventBody);
	            //counter.incrementBatchUnderflowCount();
	          }
			transaction.commit();
			*/
			
			
		    long processedEvents = 0;
			
			for (; processedEvents < eventHubSinkUtil.getBatchSize(); processedEvents += 1) {
		        event = channel.take();
	
		        if (event == null) {
		          // no events available in channel
		          if(processedEvents == 0) {
		            result = Status.BACKOFF;
		            counter.incrementBatchEmptyCount();
		          } else {
		            counter.incrementBatchUnderflowCount();
		          }
		          break;
		        }
		        
		        byte[] eventBody = event.getBody();
		        messageList.add(eventBody);
		    }
			
			// publish batch and commit.
			if (processedEvents > 0) {
				long startTime = System.nanoTime();
				sender.send(messageList);
				long endTime = System.nanoTime();
				counter.addToKafkaEventSendTimer((endTime - startTime)
						/ (1000 * 1000));
				counter.addToEventDrainSuccessCount(Long.valueOf(messageList
						.size()));
			}
			

			transaction.commit();
			
		} catch (Exception ex) {
		      String errorMsg = "Failed to publish events";
		      logger.error("Failed to publish events", ex);
		      result = Status.BACKOFF;
		      if (transaction != null) {
		        try {
		          transaction.rollback();
		          //counter.incrementRollbackCount();
		        } catch (Exception e) {
		          logger.error("Transaction rollback failed", e);
		          throw Throwables.propagate(e);
		        }
		      }
		      throw new EventDeliveryException(errorMsg, ex);
		    } finally {
		      if (transaction != null) {
		        transaction.close();
		      }
		    }
		return result;	
	}

	@Override
	public synchronized void start() {
		// instantiate the producer
		//ProducerConfig config = new ProducerConfig(kafkaProps);
		//producer = new Producer<String, byte[]>(config);
		sender = eventHubSinkUtil.createSender();
		
		counter.start();
		super.start();
	}

	@Override
	public synchronized void stop() {
		try {
			sender.close();
		} catch (EventHubException e) {
			e.printStackTrace();
		}
		counter.stop();
		logger.info("EventHub Sink {} stopped. Metrics: {}", getName(), counter);
		super.stop();
	}

	@Override
	public void configure(Context context) {
		
		eventHubSinkUtil = new EventHubSinkUtil(context); 
		
		if (counter == null){
			counter = new EventHubSinkCounter(getName());
		}
	}

}
