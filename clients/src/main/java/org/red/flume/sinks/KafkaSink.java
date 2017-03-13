package org.red.flume.sinks;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jred on 2016/10/18.
 */
public class KafkaSink extends AbstractSink implements Configurable{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    private Properties kafkaProps;
    private org.apache.kafka.clients.producer.Producer producer;
    private String topic;
    private int batchSize;
    private List<ProducerRecord<String,byte[]>> messageList;
    private KafkaSinkCounter counter;



    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        try{
            long processedEvents = 0;
            transaction = channel.getTransaction();
            transaction.begin();
            messageList.clear();
            for(;processedEvents<batchSize;processedEvents += 1){
                event = channel.take();

                if(event == null){
                    break;
                }
                byte[] eventBody = event.getBody();
                Map<String,String> headers = event.getHeaders();
//                if((eventTopic = headers.get(TOPIC_HDR)) == null){
                    eventTopic = topic;
//                }
                eventKey = headers.get(KEY_HDR);

                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("{Event}"+eventTopic+":"+eventKey+":"+new String(eventBody,"UTF-8"));
                    LOGGER.debug("event #{}",processedEvents);
                }
                //create a message and add to buffer
                ProducerRecord<String,byte[]> data = new ProducerRecord<String, byte[]>(eventTopic,eventKey,eventBody);
                messageList.add(data);
            }

            //publish batch and commit
            if(processedEvents > 0){
                long startTime = System.nanoTime();
                for(ProducerRecord record : messageList){
                    producer.send(record);
                }

                long endTime = System.nanoTime();
                counter.addToKafkaEventSendTimer((endTime-startTime)/(1000*1000));
                counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
            }
            transaction.commit();
        }catch (Exception e){
            String errorMsg = "Failed to publish events";
            LOGGER.error(errorMsg,e);
            result = Status.BACKOFF;
            if(transaction != null){
                try{
                    transaction.rollback();
                    counter.incrementRollbackCount();
                }catch (Exception ex){
                    LOGGER.error("Transaction rollback failed",e);
                    throw Throwables.propagate(ex);
                }
            }
            throw new EventDeliveryException(errorMsg,e);
        }finally {
             if(transaction != null){
                 transaction.close();
             }
        }

        return result;
    }

    public void configure(Context context) {
        batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE,
                KafkaSinkConstants.DEFAULT_BATCH_SIZE);

        messageList =
                new ArrayList<ProducerRecord<String, byte[]>>(batchSize);
        LOGGER.debug("Using batch size: {}", batchSize);

        topic = context.getString(KafkaSinkConstants.TOPIC,
                KafkaSinkConstants.DEFAULT_TOPIC);
        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
            LOGGER.warn("The Property 'topic' is not set. " +
                    "Using the default topic name: " +
                    KafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            LOGGER.info("Using the static topic: " + topic +
                    " this may be over-ridden by event headers");
        }

        kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Kafka producer properties: " + kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        producer = new KafkaProducer(kafkaProps);
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        counter.stop();
        LOGGER.info("Kafka Sink {} stopped. Metrics: {}",getName(),counter);
        super.stop();
    }
}
