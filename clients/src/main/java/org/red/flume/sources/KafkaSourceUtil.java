package org.red.flume.sources;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * the util of KafkaSource
 * Created by jred on 2016/10/18.
 */
public class KafkaSourceUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceUtil.class);

    public static Properties getKafkaProperties(Context context){
        LOGGER.info("context={} ",context.toString());
        Properties properties = generateDefaultKafkaProps();
        setKafkaProps(context,properties);
        addDocumentedKafkaProps(context,properties);
        return properties;
    }

    public static ConsumerConnector getConsumer(Properties kafkaProps){
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        return consumerConnector;
    }




    /**
     * Generate Kafka consumer properties object with some defaults
     * @return
     */
    private static Properties generateDefaultKafkaProps(){
        Properties properties = new Properties();
        properties.put(KafkaSourceConstants.AUTO_COMMIT_ENABLED,KafkaSourceConstants.DEFAULT_AUTO_COMMIT);
        properties.put(KafkaSourceConstants.CONSUMER_TIMEOUT,KafkaSourceConstants.DEFAULT_CONSUMER_TIMEOUT);
        properties.put(KafkaSourceConstants.GROUP_ID,KafkaSourceConstants.DEFAULT_GROUP_ID);
        return properties;
    }

    /**
     *
     * @param context
     * @param kafkaProps
     */
    private static void setKafkaProps(Context context,Properties kafkaProps){
        Map<String,String> kafkaProperties = context.getSubProperties(KafkaSourceConstants.PROPERTY_PREFIX);
        for(Map.Entry<String,String> prop:kafkaProperties.entrySet()){
            kafkaProps.put(prop.getKey(),prop.getValue());
            if(LOGGER.isDebugEnabled()){
                LOGGER.debug("Reading a Kafka Producer propertey: key:"+prop.getKey()+",value:"+prop.getValue());
            }
        }
    }

    private static void addDocumentedKafkaProps(Context context,Properties kafkaProps) throws ConfigurationException{
        String zookeeperConnect = context.getString(KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME);
        if(zookeeperConnect == null){
            throw new ConfigurationException("ZookeeperConnect must contain at least one Zookeeper " +
                    "server");
        }
        kafkaProps.put(KafkaSourceConstants.ZOOKEEPER_CONNECT,zookeeperConnect);

        String groupID = context.getString(KafkaSourceConstants.GROUP_ID_FLUME);

        if(groupID != null){
            kafkaProps.put(KafkaSourceConstants.GROUP_ID,groupID);
        }
    }
}
