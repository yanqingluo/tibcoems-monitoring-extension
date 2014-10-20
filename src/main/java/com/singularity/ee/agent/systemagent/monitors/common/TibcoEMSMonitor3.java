package com.singularity.ee.agent.systemagent.monitors.common;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.tibco.tibjms.admin.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TibcoEMSMonitor3 extends JavaServersMonitor
{
    private String tierName;
    private String serverName;
    private String username;
    private String password;
    private String hostname;
    private String port;
    private String protocol;
    private List<Pattern> queuePatternsToExclude;
    private boolean showTempQueues = false;
    private boolean showSysQueues = false;


    public TibcoEMSMonitor3()
    {
        oldValueMap = Collections
                .synchronizedMap(new HashMap<String, String>());
    }

    protected void parseArgs(Map<String, String> args)
    {
        super.parseArgs(args);

        // if the tier is not specified, then create the metrics for all tiers
        tierName = getArg(args, "tier", "Tibco EMS Server");
        serverName = getArg(args, "emsServerName", null);
        username = getArg(args, "username", "admin");
        password = getArg(args, "password", "admin");
        hostname = getArg(args, "hostname", "localhost");
        port = getArg(args, "port", "7222");
        protocol = getArg(args, "protocol", "tcp");
        showTempQueues = Boolean.valueOf(getArg(args, "showTempQueues", "false"));
        showSysQueues = Boolean.valueOf(getArg(args, "showSysQueues", "false"));

        String[] queuesToExclude = getArg(args, "queuesToExclude", "").trim().split("\\s+");
        queuePatternsToExclude = new ArrayList<Pattern>();
        for (String pattern : queuesToExclude)
        {
            queuePatternsToExclude.add(Pattern.compile(pattern));
        }
    }

    private TibjmsAdmin connect() throws TibjmsAdminException
    {
        String connectionUrl = String.format("%s://%s:%s", protocol, hostname, port);

        logger.debug(String.format("Connecting to %s as %s", connectionUrl, username));
        return new TibjmsAdmin(connectionUrl, username, password);
    }

    private void putServerValue(String key, long value)
    {
        valueMap.put(key, Long.toString(value));
    }

    private void putServerInfo(ServerInfo serverInfo)
    {
        putServerValue("DiskReadRate", serverInfo.getDiskReadRate());
        putServerValue("DiskWriteRate", serverInfo.getDiskWriteRate());

        putServerValue("InboundBytesRate", serverInfo.getInboundBytesRate());
        putServerValue("InboundMessageRate", serverInfo.getInboundMessageRate());
        putServerValue("OutboundBytesRate", serverInfo.getOutboundBytesRate());
        putServerValue("OutboundMessageRate", serverInfo.getOutboundMessageRate());

        putServerValue("ConnectionCount", serverInfo.getConnectionCount());
        putServerValue("MaxConnections", serverInfo.getMaxConnections());

        putServerValue("ProducerCount", serverInfo.getProducerCount());
        putServerValue("ConsumerCount", serverInfo.getConsumerCount());

        putServerValue("PendingMessageCount", serverInfo.getPendingMessageCount());
        putServerValue("PendingMessageSize", serverInfo.getPendingMessageSize());
        putServerValue("InboundMessageCount", serverInfo.getInboundMessageCount());
        putServerValue("OutboundMessageCount", serverInfo.getOutboundMessageCount());

        putServerValue("InboundMessagesPerMinute", getDeltaValue("InboundMessageCount"));
        putServerValue("OutboundMessagesPerMinute", getDeltaValue("OutboundMessageCount"));
    }

    private void putQueueValue(String queueName, String key, long value)
    {
        valueMap.put(queueName + "|" + key, Long.toString(value));
    }

    private void putQueueInfo(QueueInfo queueInfo)
    {
        String queueName = queueInfo.getName();

        putQueueValue(queueName, "ConsumerCount", queueInfo.getConsumerCount());
        putQueueValue(queueName, "InTransitCount", queueInfo.getInTransitMessageCount());
        putQueueValue(queueName, "PendingMessageCount", queueInfo.getPendingMessageCount());
        putQueueValue(queueName, "FlowControlMaxBytes", queueInfo.getFlowControlMaxBytes());
        putQueueValue(queueName, "MaxMsgs", queueInfo.getMaxMsgs());
        putQueueValue(queueName, "PendingMessageSize", queueInfo.getPendingMessageSize());
        putQueueValue(queueName, "ReceiverCount", queueInfo.getReceiverCount());
        putQueueValue(queueName, "MaxMsgs", queueInfo.getMaxMsgs());
        putQueueValue(queueName, "MaxBytes", queueInfo.getMaxBytes());

        // Inbound metrics
        StatData inboundData = queueInfo.getInboundStatistics();
        putQueueValue(queueName, "InboundByteRate", inboundData.getByteRate());
        putQueueValue(queueName, "InboundMessageRate", inboundData.getMessageRate());
        putQueueValue(queueName, "InboundByteCount", inboundData.getTotalBytes());
        putQueueValue(queueName, "InboundMessageCount", inboundData.getTotalMessages());

        // Outbound metrics
        StatData outboundData = queueInfo.getOutboundStatistics();
        putQueueValue(queueName, "OutboundByteRate", outboundData.getByteRate());
        putQueueValue(queueName, "OutboundMessageRate", outboundData.getMessageRate());
        putQueueValue(queueName, "OutboundByteCount", outboundData.getTotalBytes());
        putQueueValue(queueName, "OutboundMessageCount", outboundData.getTotalMessages());

        putQueueValue(queueName, "InboundMessagesPerMinute", getDeltaValue(queueName + "|InboundMessageCount"));
        putQueueValue(queueName, "OutboundMessagesPerMinute", getDeltaValue(queueName + "|OutboundMessageCount"));
        putQueueValue(queueName, "InboundBytesPerMinute", getDeltaValue(queueName + "|InboundByteCount"));
        putQueueValue(queueName, "OutboundBytesPerMinute", getDeltaValue(queueName + "|OutboundByteCount"));
    }

    private boolean shouldMonitorQueue(QueueInfo queueInfo)
    {
        String queueName = queueInfo.getName();

        if (queueName.startsWith("$TMP$.") && !showTempQueues)
        {
            logger.info("Skipping temporary queue '" + queueName + "'");
            return false;
        }
        else if (queueName.startsWith("$sys.") && !showSysQueues)
        {
            logger.info("Skipping system queue '" + queueName + "'");
            return false;
        }
        else
        {
            for (Pattern patternToExclude : queuePatternsToExclude)
            {
                Matcher matcher = patternToExclude.matcher(queueName);
                if (matcher.matches()) {
                    logger.info(String.format("Skipping queue '%s' due to pattern '%s' in queuesToExclude",
                            queueName, patternToExclude.pattern()));
                    return false;
                }
            }

            return true;
        }
    }

    // collects all monitoring data for this time period
    private void putValuesIntoMap() throws Exception
    {
        TibjmsAdmin conn = null;

        try
        {
            conn = connect();

            ServerInfo serverInfo = conn.getInfo();
            putServerInfo(serverInfo);

            // get most accurate time
            currentTime = System.currentTimeMillis();

//            logger.debug("Retrieving producer information");
//            ProducerInfo[] producerInfos = conn.getProducersStatistics();

            logger.debug("Retrieving queue information");
            QueueInfo[] queueInfos = conn.getQueuesStatistics();

            if (queueInfos == null)
            {
                logger.warn("Unable to get queue statistics");
            }
            else
            {
                for (QueueInfo queueInfo : queueInfos)
                {
                    if (shouldMonitorQueue(queueInfo))
                    {
                        logger.info("Publishing metrics for queue " + queueInfo.getName());
                        putQueueInfo(queueInfo);
                    }
                }
            }

        }
        catch (com.tibco.tibjms.admin.TibjmsAdminException ex)
        {
            logger.error("Error connecting to EMS server " + serverName + " "
                    + port + " " + this.hostname + " " + this.password, ex);
        }
        catch (Exception ex)
        {
            logger.error("Error getting performance data from Tibco EMS", ex);
        }
        finally
        {
            if (conn != null) {
                logger.info("Closing connection to EMS server");
                conn.close();
            }
        }
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

        try
        {
            putValuesIntoMap();
            for (Map.Entry<String, String> entry : valueMap.entrySet())
            {
                printMetric(entry.getKey(), entry.getValue());
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
        printMetric(name, value,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
    }

    protected String getMetricPrefix()
    {
        logger.debug("tierName=" + tierName + " serverName=" + serverName);
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
        Map<String, String> params = new HashMap<String, String>();
        params.put("hostname", "192.168.56.101");
        params.put("password", null);
        params.put("queuesToExclude", "sample");
        new TibcoEMSMonitor3().execute(params, null);
    }

}
