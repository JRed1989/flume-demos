package org.red.flume.sinks;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * Created by jred on 2016/9/29.
 */
public class MySink extends AbstractSink implements Configurable{

    private String myProp;

    public Status process() throws EventDeliveryException {

        Status status = null;

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try{

            transaction.commit();
            status = Status.READY;
        }catch (Throwable t) {
            transaction.rollback();

            status = Status.BACKOFF;

            if (t instanceof Error) {
                throw (Error) t;
            }
        }

        return status;
    }

    public void configure(Context context) {
        String myProp = context.getString("myProp","defaultValue");

    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
