package com.singularity.ee.agent.systemagent.monitors.common;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.singularity.ee.util.clock.ClockUtils;
import com.tibco.tibjms.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TibcoEMSMonitor3 extends JavaServersMonitor
{
    private volatile String tierName;
    private volatile String serverName;
    private volatile int refreshIntervalInExecutionTicks;
    private volatile int currentNumExecutionTicks = -1;
    private volatile String userid;
    private volatile String password;
    private volatile String hostname;
    private volatile String port;
    private volatile String protocol;
    private volatile Boolean showTempQueues = false;
    private volatile Boolean showSysQueues = false;

    public TibcoEMSMonitor3()
    {
        oldValueMap = Collections
                .synchronizedMap(new HashMap<String, String>());
    }

    protected void parseArgs(Map<String, String> args)
    {
        super.parseArgs(args);
        tierName = getArg(args, "tier", "Tibco EMS Server"); // if the tier is not specified
        // then create the metrics for
        // all tiers
        serverName = getArg(args, "emsservername", null);
        userid = getArg(args, "userid", "admin");
        password = getArg(args, "password", "admin");
        hostname = getArg(args, "hostname", "localhost");
        port = getArg(args, "port", "7222");
        protocol = getArg(args, "protocol", "tcp");
        showTempQueues = Boolean.valueOf(getArg(args, "showTempQueues", "false"));
        showSysQueues = Boolean.valueOf(getArg(args, "showSysQueues", "false"));

        int refreshIntervalSecs = Integer.parseInt(getArg(args, "refresh-interval", "60"));

        if (refreshIntervalSecs <= 60)
        {
            refreshIntervalInExecutionTicks = 1;
        }
        else
        {
            // Convert refresh interval to milliseconds and round up to the
            // nearest minute timeslice.
            // From that we can get the number of 60 second ticks before the
            // next refresh.
            // We do this to prevent time drift issues from preventing this task
            // from running.
            refreshIntervalInExecutionTicks = (int) (ClockUtils
                    .roundUpTimestampToNextMinute(refreshIntervalSecs * 1000) / 60000);
        }

        if (currentNumExecutionTicks == -1)
        {
            // This is the first time we've parsed the args. Assume we refresh
            // the data
            // the next time we execute the monitor.
            currentNumExecutionTicks = refreshIntervalInExecutionTicks;
        }
    }

    private TibjmsAdmin connect() throws TibjmsAdminException
    {

        String connectionUrl = String.format("%s://%s:%s", protocol, hostname, port);

        logger.debug(String.format("Connecting to %s as %s", connectionUrl, userid));
        return new TibjmsAdmin(connectionUrl, userid, password);
    }

    private void putServerValue(Map<String, String> valueMap, String key, long value)
    {
        valueMap.put(key, Long.toString(value));
    }

    private void putQueueValue(Map<String, String> valueMap, String queueName, String key, long value)
    {
        valueMap.put(queueName + "|" + key, Long.toString(value));
    }

    // collects all monitoring data for this time period from database
    private Map<String, String> putValuesIntoMap() throws Exception
    {
        Map<String, String> map = new HashMap<String, String>();

        TibjmsAdmin conn = null;
        boolean debug = logger.isDebugEnabled();
        try
        {
            if (conn == null)
            {
                conn = connect();
            }

            ServerInfo serverInfo = conn.getInfo();

            putServerValue(map, "DiskReadRate", serverInfo.getDiskReadRate());
            putServerValue(map, "DiskWriteRate", serverInfo.getDiskWriteRate());

            putServerValue(map, "InboundBytesRate", serverInfo.getInboundBytesRate());
            putServerValue(map, "InboundMessageRate", serverInfo.getInboundMessageRate());
            putServerValue(map, "OutboundBytesRate", serverInfo.getOutboundBytesRate());
            putServerValue(map, "OutboundMessageRate", serverInfo.getOutboundMessageRate());

            putServerValue(map, "ConnectionCount", serverInfo.getConnectionCount());
            putServerValue(map, "MaxConnections", serverInfo.getMaxConnections());

            putServerValue(map, "ProducerCount", serverInfo.getProducerCount());
            putServerValue(map, "ConsumerCount", serverInfo.getConsumerCount());

            putServerValue(map, "PendingMessageCount", serverInfo.getPendingMessageCount());
            putServerValue(map, "PendingMessageSize", serverInfo.getPendingMessageSize());
            putServerValue(map, "InboundMessageCount", serverInfo.getInboundMessageCount());
            putServerValue(map, "OutboundMessageCount", serverInfo.getOutboundMessageCount());


            // get most accurate time
            currentTime = System.currentTimeMillis();
            logger.debug("Retrieving Queue Information");

            QueueInfo[] queueInfos = null;
            ProducerInfo[] producerInfos = null;

            try
            {
                producerInfos = conn.getProducersStatistics();
                if (debug)
                {
                    logger.debug("Retrieving Producer Information");
                    if (producerInfos.length > 0)
                    {
                        logger.debug("Producing Information is Greater than ZERO");
                    }
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            try
            {
                queueInfos = conn.getQueuesStatistics();
                if (debug)
                {
                    logger.debug("Retrieving Queue Information");
                    if (queueInfos.length > 0)
                    {
                        logger.debug("Queue Information is Greater than ZERO");
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            if (queueInfos == null)
            {
                logger.warn("Unable to get queue statistics");
            }
            else
            {
                for (QueueInfo queueInfo : queueInfos)
                {

                    String queueName = queueInfo.getName();

                    if (queueName.startsWith("$TMP$.") && !showTempQueues)
                    {
                        logger.info("Skipping temporary queue " + queueName);
                    }
                    else if (queueName.startsWith("$sys.") && !showSysQueues)
                    {
                        logger.info("Skipping system queue " + queueName);
                    }
                    else
                    {
                        logger.info("Publishing metrics for queue " + queueName);

                        putQueueValue(map, queueName, "ConsumerCount", queueInfo.getConsumerCount());
                        putQueueValue(map, queueName, "InTransitCount", queueInfo.getInTransitMessageCount());
                        putQueueValue(map, queueName, "PendingMessageCount", queueInfo.getPendingMessageCount());
                        putQueueValue(map, queueName, "FlowControlMaxBytes", queueInfo.getFlowControlMaxBytes());
                        putQueueValue(map, queueName, "MaxMsgs", queueInfo.getMaxMsgs());
                        putQueueValue(map, queueName, "PendingMessageSize", queueInfo.getPendingMessageSize());
                        putQueueValue(map, queueName, "ReceiverCount", queueInfo.getReceiverCount());
                        putQueueValue(map, queueName, "MaxMsgs", queueInfo.getMaxMsgs());
                        putQueueValue(map, queueName, "MaxBytes", queueInfo.getMaxBytes());

                        // Inbound metrics
                        StatData inboundData = queueInfo.getInboundStatistics();
                        putQueueValue(map, queueName, "InboundByteRate", inboundData.getByteRate());
                        putQueueValue(map, queueName, "InboundMessageRate", inboundData.getMessageRate());
                        putQueueValue(map, queueName, "InboundByteCount", inboundData.getTotalBytes());
                        putQueueValue(map, queueName, "InboundMessageCount", inboundData.getTotalMessages());

                        // Outbound metrics
                        StatData outboundData = queueInfo.getOutboundStatistics();
                        putQueueValue(map, queueName, "OutboundByteRate", outboundData.getByteRate());
                        putQueueValue(map, queueName, "OutboundMessageRate", outboundData.getMessageRate());
                        putQueueValue(map, queueName, "OutboundByteCount", outboundData.getTotalBytes());
                        putQueueValue(map, queueName, "OutboundMessageCount", outboundData.getTotalMessages());
                    }

                }
            }

            logger.info("Closing connection to EMS server");
            conn.close();
        }
        catch (com.tibco.tibjms.admin.TibjmsAdminException ex)
        {
            logger.error("Error connecting to EMS server" + serverName + " "
                    + port + " " + this.hostname + " " + this.password, ex);
        }
        catch (Exception ex)
        {
            logger.error("Error getting performance data from Tibco EMS", ex);
        }
        return Collections.synchronizedMap(map);
    }

    public TaskOutput execute(Map<String, String> taskArguments, TaskExecutionContext taskContext)
            throws TaskExecutionException
    {

        logger.debug("Starting Execute Thread: " + taskArguments + " : " + taskContext);

        startExecute(taskArguments, taskContext);
        try
        {
            Thread.sleep(5000);
        }
        catch (Exception e)
        {
            logger.error("Sleep was interrupted", e);
        }

        // just for debug output
        logger.debug("Starting METRIC COLLECTION for Tibco EMS Monitor");

        Map<String, String> map;
        try
        {
            map = this.putValuesIntoMap();
            Iterator<String> keys = map.keySet().iterator();
            while (keys.hasNext())
            {
                String key = keys.next();
                String value = map.get(key);
                printMetric(key, value);
            }
        }
        catch (Exception e)
        {
            logger.error("Error uploading metrics: " + e.getMessage(), e);
        }

        logger.debug("Finished METRIC COLLECTION for Tibco EMS Monitor");

        return this.finishExecute();
    }

    private void printMetric(String name, String value)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("* * * KEY: " + name + " VALUE: " + value);
        }
        printMetric(name, value,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
    }

    protected String getMetricPrefix()
    {
        logger.debug("Tier name is " + tierName);
        if (serverName != null && serverName.trim().length() > 0)
        {
            return "Custom Metrics|" + tierName + "|" + serverName + "|";
        }
        else
        {
            return "Custom Metrics|" + tierName + "|";
        }
    }

    public static void main(String[] args) throws TaskExecutionException
    {
        new TibcoEMSMonitor3().execute(null, null);
    }

}
