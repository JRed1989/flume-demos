package org.red.flume.sources.database;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库数据源
 * Created by jred on 2016/11/14.
 */
public class DataBaseSource  extends AbstractSource implements Configurable,PollableSource{


    private static final Logger LOGGER =  LoggerFactory.getLogger(DataBaseSource.class);

    /**
     * 批量提交数量
     */
    private int batchUpperLimit;
    /**
     * 提交时间间隔
     */
    private int timeUpperLimit;
    /**
     * 是否需要分页，默认为否
     */
    private boolean isPageable = false;
    /**
     * 分页数据大小
     */
    private int pageSize;
    /**
     * 查询语句
     */
    private String querySql;
    /**
     * 表示数据是否更新的字段
     */
    private String updateFieldName;
    /**
     * 数据库的配置属性
     */
    private Properties dbProps;
    /**
     * 数据库类型
     */
    private String dbType;
    /**
     * 数据字符编码
     */
    private String dbCharset;


    private  final List<Event> eventList = new ArrayList<Event>();

    private DataSource dataSource;
    /**
     *上次更新时间
     */
    private long lastUpdateTime = -1;



    public Status process() throws EventDeliveryException {
        //当初始化时或者1天以后才能再次读取数据
        if(lastUpdateTime == -1 || (System.currentTimeMillis() > (lastUpdateTime + 1000*60*60*24))){
            try {
                //判断是否需要分页
                if (isPageable) {
                    long totalSize = DataBaseSourceUtil.getSize(dataSource,querySql);
                    int pageCount = DataBaseSourceUtil.computePage(totalSize, pageSize);
                    for (int i = 1; i <= pageCount; i++) {
                        try {
                            List<Map<String, Object>> list = DataBaseSourceUtil.getListByPage(dataSource,dbType, querySql, i, pageSize);
                            processEvent(list);
                        } catch (Exception e) {
                            LOGGER.error("处理数据出错。第" + i + "页数据.sql = " + querySql + "", e);
                        }
                    }

                } else {
                    processEvent(DataBaseSourceUtil.getList(dataSource,querySql));
                }
                lastUpdateTime = System.currentTimeMillis();
                return Status.READY;
            }catch (Exception e){
                return Status.BACKOFF;

            }
        }
        return Status.READY;


    }


    private void processEvent(List<Map<String,Object>> list){
        if(list != null) {
            Event event;
            long batchStartTime = System.currentTimeMillis();
            long batchEndTime = System.currentTimeMillis() + timeUpperLimit;

            for (int n = 0; n < list.size(); n++) {
                Map<String, Object> data = list.get(n);
                StringBuilder stringBuilder = new StringBuilder();
                for (Map.Entry<String, Object> map : data.entrySet()) {
                    stringBuilder.append(map.getValue() + "\t");
                }
                stringBuilder.append("\n");
                try {
                    System.out.println(stringBuilder.toString());
                    event = EventBuilder.withBody(stringBuilder.toString().getBytes(dbCharset), null);
                    eventList.add(event);
                    if (eventList.size() == batchUpperLimit && System.currentTimeMillis() > batchEndTime) {
                        getChannelProcessor().processEventBatch(eventList);
                        eventList.clear();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Wrote {} events to channel", eventList.size());
                        }
                    }

                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
            //提交为提交的数据
            if (eventList.size() > 0) {
                getChannelProcessor().processEventBatch(eventList);
                eventList.clear();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Wrote {} events to channel", eventList.size());
                }
            }

        }
    }


    public void configure(Context context) {
        dbProps = new Properties();
        DataBaseSourceUtil.setDbProperties(context,dbProps);
        batchUpperLimit = context.getInteger(DataBaseSourceConstants.BATCH_UPPER_LIMIT);
        timeUpperLimit = context.getInteger(DataBaseSourceConstants.TIME_UPPER_LIMIT);
        isPageable = context.getBoolean(DataBaseSourceConstants.IS_PAGEABLE);
        pageSize = context.getInteger(DataBaseSourceConstants.PAGE_SIZE);
        querySql = context.getString(DataBaseSourceConstants.QUERY_SQL);
        updateFieldName = context.getString(DataBaseSourceConstants.UPDATE_FIELD_NAME);
        dbType = context.getString(DataBaseSourceConstants.DB_TYPE);
        dbCharset = context.getString(DataBaseSourceConstants.DB_CHARSET);
        if(dataSource == null){
            dataSource = DataBaseSourceUtil.getDataSource(dbProps);
            System.out.println("数据库连接成功。datasource = "+dataSource);
        }
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
