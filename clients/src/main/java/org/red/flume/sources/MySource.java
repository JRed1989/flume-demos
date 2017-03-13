package org.red.flume.sources;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * Created by jred on 2016/9/29.
 */
public class MySource  extends AbstractSource implements Configurable,PollableSource{


    private String myProp;

    public void configure(Context context) {
        String myProp = context.getString("myProp","defaultValue");
        this.myProp = myProp;

    }

    public Status process() throws EventDeliveryException {
        Status status = null;
        return null;
    }


}
