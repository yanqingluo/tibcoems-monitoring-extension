package com.singularity.ee.agent.systemagent.monitors.common;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.tibco.tibjms.TibjmsSSL;
import com.tibco.tibjms.admin.*;
import com.tibco.tibjms.admin.ProducerInfo;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TibcoEMSMonitor3 extends JavaServersMonitor {
    private String tierName;
    private String serverName;
    private String username;
    private String password;
    private String hostname;
    private String port;
    private String protocol;
    private String serverUrl;
    private String sslVendor;
    private String sslIdentityFile;
    private String sslIdentityPassword;
    private String sslTrustedCerts;
    private String sslIssuerCerts;
    private boolean sslDebug;
    private boolean sslVerifyHost;
    private boolean sslVerifyHostName;
    private List<Pattern> queuePatternsToExclude;
    private List<Pattern> topicPatternsToExclude;
    private boolean showTempQueues;
    private boolean showSysQueues;


    public TibcoEMSMonitor3() {
        oldValueMap = Collections.synchronizedMap(new HashMap<String, String>());
    }

    protected void parseArgs(Map<String, String> args) {
        super.parseArgs(args);

        // if the tier is not specified, then create the metrics for all tiers
        tierName = getArg(args, "tier", "Tibco EMS Server");
        serverName = getArg(args, "emsServerName", null);
        username = getArg(args, "username", "admin");
        password = getArg(args, "password", "admin");
        hostname = getArg(args, "hostname", "localhost");
        port = getArg(args, "port", "7222");
        protocol = getArg(args, "protocol", "tcp");

        sslVendor = getArg(args, "sslVendor", "j2se");
        sslIdentityFile = getArg(args, "sslIdentityFile", null);
        sslIdentityPassword = getArg(args, "sslIdentityPassword", null);
        sslTrustedCerts = getArg(args, "sslTrustedCerts", null);
        sslIssuerCerts = getArg(args, "sslIssuerCerts", null);

        sslDebug = Boolean.valueOf(getArg(args, "sslDebug", "false"));
        sslVerifyHost = Boolean.valueOf(getArg(args, "sslVerifyHost", "false"));
        sslVerifyHostName = Boolean.valueOf(getArg(args, "sslVerifyHostName", "false"));

        showTempQueues = Boolean.valueOf(getArg(args, "showTempQueues", "false"));
        showSysQueues = Boolean.valueOf(getArg(args, "showSysQueues", "false"));

        queuePatternsToExclude = parsePatternArg(args, "queuesToExclude");
        topicPatternsToExclude = parsePatternArg(args, "topicsToExclude");
    }

    private List<Pattern> parsePatternArg(Map<String, String> args, String name) {
        ArrayList<Pattern> patternList = new ArrayList<Pattern>();

        String[] patterns = getArg(args, name, "").trim().split("\\s+");
        for (String pattern : patterns) {
            patternList.add(Pattern.compile(pattern));
        }

        return patternList;
    }

    @SuppressWarnings("unchecked")
    private TibjmsAdmin connect() throws TibjmsAdminException {
        Hashtable sslParams = new Hashtable();

        if (StringUtils.isNotEmpty(sslIdentityFile)) {
            sslParams.put(TibjmsSSL.IDENTITY, sslIdentityFile);
            sslParams.put(TibjmsSSL.PASSWORD, sslIdentityPassword);
        }

        if (StringUtils.isNotEmpty(sslTrustedCerts)) {
            sslParams.put(TibjmsSSL.TRUSTED_CERTIFICATES, sslTrustedCerts);
        }

        if (StringUtils.isNotEmpty(sslIssuerCerts)) {
            sslParams.put(TibjmsSSL.ISSUER_CERTIFICATES, sslIssuerCerts);
        }

        sslParams.put(TibjmsSSL.VENDOR, sslVendor);
        sslParams.put(TibjmsSSL.TRACE, sslDebug);
        sslParams.put(TibjmsSSL.ENABLE_VERIFY_HOST, sslVerifyHost);
        sslParams.put(TibjmsSSL.ENABLE_VERIFY_HOST_NAME, sslVerifyHostName);

        serverUrl = String.format("%s://%s:%s", protocol, hostname, port);

        logger.debug(String.format("Connecting to %s as %s", serverUrl, username));
        return new TibjmsAdmin(serverUrl, username, password, sslParams);
    }

    private void putServerValue(String key, long value) {
        valueMap.put(key, Long.toString(value));
    }

    private void putServerInfo(ServerInfo serverInfo) {
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

    private void putDestinationValue(String prefix, String key, long value) {
        valueMap.put(prefix + "|" + key, Long.toString(value));
    }

    private void putDestinationInfo(String prefix, DestinationInfo destInfo) {
        putDestinationValue(prefix, "ConsumerCount", destInfo.getConsumerCount());
        putDestinationValue(prefix, "PendingMessageCount", destInfo.getPendingMessageCount());
        putDestinationValue(prefix, "PendingMessageSize", destInfo.getPendingMessageSize());
        putDestinationValue(prefix, "FlowControlMaxBytes", destInfo.getFlowControlMaxBytes());
        putDestinationValue(prefix, "MaxMsgs", destInfo.getMaxMsgs());
        putDestinationValue(prefix, "MaxBytes", destInfo.getMaxBytes());

        // Inbound metrics
        StatData inboundData = destInfo.getInboundStatistics();
        putDestinationValue(prefix, "InboundByteRate", inboundData.getByteRate());
        putDestinationValue(prefix, "InboundMessageRate", inboundData.getMessageRate());
        putDestinationValue(prefix, "InboundByteCount", inboundData.getTotalBytes());
        putDestinationValue(prefix, "InboundMessageCount", inboundData.getTotalMessages());

        // Outbound metrics
        StatData outboundData = destInfo.getOutboundStatistics();
        putDestinationValue(prefix, "OutboundByteRate", outboundData.getByteRate());
        putDestinationValue(prefix, "OutboundMessageRate", outboundData.getMessageRate());
        putDestinationValue(prefix, "OutboundByteCount", outboundData.getTotalBytes());
        putDestinationValue(prefix, "OutboundMessageCount", outboundData.getTotalMessages());

        putDestinationValue(prefix, "InboundMessagesPerMinute", getDeltaValue(prefix + "|InboundMessageCount"));
        putDestinationValue(prefix, "OutboundMessagesPerMinute", getDeltaValue(prefix + "|OutboundMessageCount"));
        putDestinationValue(prefix, "InboundBytesPerMinute", getDeltaValue(prefix + "|InboundByteCount"));
        putDestinationValue(prefix, "OutboundBytesPerMinute", getDeltaValue(prefix + "|OutboundByteCount"));
    }

    private void putQueueInfo(QueueInfo queueInfo) {
        String prefix = "Queues|" + queueInfo.getName();

        putDestinationInfo(prefix, queueInfo);
        putDestinationValue(prefix, "InTransitCount", queueInfo.getInTransitMessageCount());
        putDestinationValue(prefix, "ReceiverCount", queueInfo.getReceiverCount());
    }

    private void putTopicInfo(TopicInfo topicInfo) {
        String prefix = "Topics|" + topicInfo.getName();

        putDestinationInfo(prefix, topicInfo);
        putDestinationValue(prefix, "ConsumerCount", topicInfo.getConsumerCount());
        putDestinationValue(prefix, "SubscriberCount", topicInfo.getSubscriberCount());
        putDestinationValue(prefix, "ActiveDurableCount", topicInfo.getActiveDurableCount());
        putDestinationValue(prefix, "DurableCount", topicInfo.getDurableCount());
    }

    private void putProducerInfos(TibjmsAdmin conn) throws TibjmsAdminException {
        logger.debug("Retrieving producer information");
        ProducerInfo[] producerInfos = conn.getProducersStatistics();

        if (producerInfos == null) {
            logger.warn("Unable to get producer statistics");
        } else {
            for (ProducerInfo producerInfo : producerInfos) {
                logger.info("Publishing metrics for producer " + Long.toString(producerInfo.getID()));
                putProducerInfo(producerInfo);
            }
        }

    }

    private void putProducerInfo(ProducerInfo producerInfo) throws TibjmsAdminException {

    }

    private void putQueueInfos(TibjmsAdmin conn) throws TibjmsAdminException {
        logger.debug("Retrieving queue information");
        QueueInfo[] queueInfos = conn.getQueuesStatistics();

        if (queueInfos == null) {
            logger.warn("Unable to get queue statistics");
        } else {
            for (QueueInfo queueInfo : queueInfos) {
                if (shouldMonitorDestination(queueInfo, queuePatternsToExclude)) {
                    logger.info("Publishing metrics for queue " + queueInfo.getName());
                    putQueueInfo(queueInfo);
                }
            }
        }
    }


    private void putTopicInfos(TibjmsAdmin conn) throws TibjmsAdminException {
        logger.debug("Retrieving topic information");
        TopicInfo[] topicInfos = conn.getTopicsStatistics();

        if (topicInfos == null) {
            logger.warn("Unable to get topic statistics");
        } else {
            for (TopicInfo topicInfo : topicInfos) {
                if (shouldMonitorDestination(topicInfo, topicPatternsToExclude)) {
                    logger.info("Publishing metrics for topic " + topicInfo.getName());
                    putTopicInfo(topicInfo);
                }
            }
        }
    }

    private boolean shouldMonitorDestination(DestinationInfo destInfo, List<Pattern> patternsToExclude) {
        String destName = destInfo.getName();

        if (destName.startsWith("$TMP$.") && !showTempQueues) {
            logger.info("Skipping temporary destination '" + destName + "'");
            return false;
        } else if (destName.startsWith("$sys.") && !showSysQueues) {
            logger.info("Skipping system destination '" + destName + "'");
            return false;
        } else {
            for (Pattern patternToExclude : patternsToExclude) {
                Matcher matcher = patternToExclude.matcher(destName);
                if (matcher.matches()) {
                    logger.info(String.format("Skipping queue '%s' due to excluded pattern '%s'",
                            destName, patternToExclude.pattern()));
                    return false;
                }
            }

            return true;
        }
    }

    // collects all monitoring data for this time period
    private void putValuesIntoMap() throws Exception {
        TibjmsAdmin conn = null;

        try {
            conn = connect();

            ServerInfo serverInfo = conn.getInfo();
            putServerInfo(serverInfo);

            logger.debug("Retrieving producer information");
            ProducerInfo[] producerInfos = conn.getProducersStatistics();

            putQueueInfos(conn);
            putTopicInfos(conn);
        } catch (com.tibco.tibjms.admin.TibjmsAdminException ex) {
            logger.error("Error connecting to EMS server " + this.serverUrl, ex);
        } catch (Exception ex) {
            logger.error("Error getting performance data from Tibco EMS", ex);
        } finally {
            if (conn != null) {
                logger.info("Closing connection to EMS server");
                conn.close();
            }
        }
    }

    public TaskOutput execute(Map<String, String> taskArguments, TaskExecutionContext taskContext)
            throws TaskExecutionException {
        logger.debug("Starting Execute Thread: " + taskArguments + " : " + taskContext);

        startExecute(taskArguments, taskContext);
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            logger.error("Sleep was interrupted", e);
        }

        // just for debug output
        logger.debug("Starting METRIC COLLECTION for Tibco EMS Monitor");

        try {
            putValuesIntoMap();
            for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                printMetric(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("Error uploading metrics: " + e.getMessage(), e);
        }

        logger.debug("Finished METRIC COLLECTION for Tibco EMS Monitor");

        return this.finishExecute();
    }

    private void printMetric(String name, String value) {
        printMetric(name, value,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
    }

    protected String getMetricPrefix() {
        logger.debug("tierName=" + tierName + " serverName=" + serverName);
        if (serverName != null && serverName.trim().length() > 0) {
            return "Custom Metrics|" + tierName + "|" + serverName + "|";
        } else {
            return "Custom Metrics|" + tierName + "|";
        }
    }

    public static void main(String[] args) throws TaskExecutionException {
        Map<String, String> params = new HashMap<String, String>();
        params.put("hostname", "localhost");
        params.put("port", "7243");
        params.put("protocol", "ssl");
        params.put("password", null);
        params.put("queuesToExclude", "sample");
        new TibcoEMSMonitor3().execute(params, null);
    }

}
