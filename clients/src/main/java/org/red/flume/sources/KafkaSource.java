package org.red.flume.sources;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 自定义kafka source
 * Created by jred on 2016/10/17.
 */
public class KafkaSource  extends AbstractSource implements Configurable,PollableSource{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private ConsumerConnector consumer;
    private ConsumerIterator<byte[],byte[]> it;
    private String topic;
    private int batchUpperLimit;
    private int timeUpperLimit;
    private int consumerTimeout;
    private boolean kafkaAutoCommitnabled;
    private Context context;
    private Properties kafkaProps;
    private final List<Event> eventList = new ArrayList<Event>();
    private KafkaSourceCounter counter;

    public Status process() throws EventDeliveryException {
        LOGGER.info("======process method start.");
        byte[] kafkaMessage;
        byte[] kafkaKey;
        Event event;
        Map<String,String> headers;
        long batchStartTime = System.currentTimeMillis();
        long batchEndTime = System.currentTimeMillis()+timeUpperLimit;
        try {
            boolean iterStatus = false;
            long startTime = System.nanoTime();
            while (eventList.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
                iterStatus = hasNext();
                if (iterStatus) {
                    //get next message
                    MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
                    kafkaMessage = messageAndMetadata.message();
                    kafkaKey = messageAndMetadata.key();

                    //Add headers to event (topic,timestamp and key)
                    headers = new HashMap<String, String>();
                    headers.put(KafkaSourceConstants.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
                    headers.put(KafkaSourceConstants.TOPIC, topic);
                    if (kafkaKey != null) {
                        headers.put(KafkaSourceConstants.KEY, new String(kafkaKey));
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Message: {}", new String(kafkaMessage));
                    }
                    event = EventBuilder.withBody(kafkaMessage, headers);
                    eventList.add(event);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Waited:{} ", System.currentTimeMillis() - batchStartTime);
                    LOGGER.debug("Event #: {}", eventList.size());
                }
            }
            long endTime = System.nanoTime();
            counter.addToKafkaEventGetTimer((endTime - startTime) / (1000 * 1000));
            counter.addToEventReceivedCount(Long.valueOf(eventList.size()));

            if (eventList.size() > 0) {
                getChannelProcessor().processEventBatch(eventList);
                counter.addToEventAcceptedCount(eventList.size());
                eventList.clear();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Wrote {} events to channel", eventList.size());
                }
                if (!kafkaAutoCommitnabled) {
                    long commitStartTime = System.nanoTime();
                    consumer.commitOffsets();
                    long commitEndTime = System.nanoTime();
                    counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / (1000 * 1000));
                }
            }

            if (!iterStatus) {
//                if (LOGGER.isDebugEnabled()) {
//                    counter.incrementKafkaEmptyCount();
//                    LOGGER.debug("Returning with backoff. No more data to read");
//                }
                return Status.BACKOFF;
            }
            LOGGER.info("======process method end. status = {}",Status.READY);
            return Status.READY;
        }catch (Exception e){
            LOGGER.error("KafkaSource Exception, {}",e);
            return Status.BACKOFF;
        }
    }

    public void configure(Context context) {
        this.context = context;
        batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,KafkaSourceConstants.DEFAULT_BATCH_SIZE);
        timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,KafkaSourceConstants.DEFAULT_BATCH_DURATION);
        topic = context.getString(KafkaSourceConstants.TOPIC);
        if(StringUtils.isEmpty(topic)){
            throw new ConfigurationException("Kafka topic must be specified.");
        }
        kafkaProps = KafkaSourceUtil.getKafkaProperties(context);
        consumerTimeout = Integer.valueOf(kafkaProps.getProperty(
                KafkaSourceConstants.CONSUMER_TIMEOUT
        ));
        kafkaAutoCommitnabled = Boolean.valueOf(kafkaProps.getProperty(
                KafkaSourceConstants.AUTO_COMMIT_ENABLED
        ));
        if(counter == null){
            counter = new KafkaSourceCounter(getName());
        }
    }


    @Override
    public synchronized void start() {
        LOGGER.info("Starting {} ...",this);
        try{
            //initialize a consumer. This creates the connection to Zookeeper
            consumer = KafkaSourceUtil.getConsumer(kafkaProps);
        }catch (Exception e){
            throw new FlumeException("Unable to create consumer.Check whether the Zookeeper" +
                    "Server is up and that the Flume agent can connect it .",e);
        }
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        //We always have just one topic being read by one thread
        topicCountMap.put(topic,1);

        try{
            Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap =
                    consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[],byte[]>> topicList = consumerMap.get(topic);
            KafkaStream<byte[],byte[]> stream = topicList.get(0);
            it = stream.iterator();
        }catch (Exception e){
            throw new FlumeException("Unable to get message iterator from kafka",e);
        }
        LOGGER.info("Kafka source {} started.",getName());
        counter.start();
        super.start();
    }


    @Override
    public synchronized void stop() {
        if(consumer != null){
            consumer.shutdown();
        }
        counter.stop();
        LOGGER.info("Kafka Source {} stopped. Metrics:{}",getName(),counter);
        super.stop();
    }

    /**
     * Check if there are messages waiting in Kafka,
     * waiting until timeout (10ms by default) for messages to arrive.
     * and catching the timeout exception to return a boolean
     */
    boolean hasNext() {
        try {
            it.hasNext();
            return true;
        } catch (ConsumerTimeoutException e) {
            return false;
        }
    }
}

