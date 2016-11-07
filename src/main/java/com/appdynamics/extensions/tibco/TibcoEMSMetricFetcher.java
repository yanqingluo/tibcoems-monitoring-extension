package com.appdynamics.extensions.tibco;


import com.appdynamics.TaskInputArgs;
import com.appdynamics.extensions.conf.MonitorConfiguration;
import com.appdynamics.extensions.crypto.CryptoUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.tibco.tibjms.TibjmsSSL;
import com.tibco.tibjms.admin.ConnectionInfo;
import com.tibco.tibjms.admin.ConsumerInfo;
import com.tibco.tibjms.admin.DestinationInfo;
import com.tibco.tibjms.admin.DurableInfo;
import com.tibco.tibjms.admin.ProducerInfo;
import com.tibco.tibjms.admin.QueueInfo;
import com.tibco.tibjms.admin.RouteInfo;
import com.tibco.tibjms.admin.ServerInfo;
import com.tibco.tibjms.admin.StatData;
import com.tibco.tibjms.admin.TibjmsAdmin;
import com.tibco.tibjms.admin.TibjmsAdminException;
import com.tibco.tibjms.admin.TopicInfo;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Satish Muddam
 */
public class TibcoEMSMetricFetcher implements Runnable {

    private static final Logger logger = Logger.getLogger(TibcoEMSMetricFetcher.class);

    private MonitorConfiguration configuration;
    private Map emsServer;
    private Map<String, Map<String, String>> cachedStats;
    protected volatile Map<String, String> valueMap;
    private Map<String, String> oldValuesMap;

    public TibcoEMSMetricFetcher(MonitorConfiguration configuration, Map emsServer, Map<String, Map<String, String>> cachedStats) {
        this.configuration = configuration;
        this.emsServer = emsServer;
        this.cachedStats = cachedStats;

        String displayName = (String) emsServer.get("displayName");
        oldValuesMap = cachedStats.get(displayName);

        if (oldValuesMap == null) {
            oldValuesMap = new HashMap<String, String>();
        }
        valueMap = new HashMap<String, String>();
    }

    public void run() {

        String displayName = (String) emsServer.get("displayName");
        String host = (String) emsServer.get("host");
        int port = (Integer) emsServer.get("port");
        String protocol = (String) emsServer.get("protocol");
        String user = (String) emsServer.get("user");
        String password = (String) emsServer.get("password");
        String encryptedPassword = (String) emsServer.get("encryptedPassword");
        String encryptionKey = (String) emsServer.get("encryptionKey");
        boolean showTemp = (Boolean) emsServer.get("showTemp");
        boolean showSystem = (Boolean) emsServer.get("showSystem");
        List<String> excludeQueues = (List) emsServer.get("excludeQueues");
        List<String> excludeTopics = (List) emsServer.get("excludeTopics");

        List<Pattern> excludeQueuePatterns = buildPattern(excludeQueues);
        List<Pattern> excludeTopicPatterns = buildPattern(excludeTopics);

        String emsURL = String.format("%s://%s:%s", protocol, host, port);
        logger.debug(String.format("Connecting to %s as %s", emsURL, user));

        String plainPassword = getPassword(password, encryptedPassword, encryptionKey);

        Hashtable sslParams = new Hashtable();

        if (protocol.equals("ssl")) {
            String sslIdentityFile = (String) emsServer.get("sslIdentityFile");
            String sslIdentityPassword = (String) emsServer.get("sslIdentityPassword");
            String sslTrustedCerts = (String) emsServer.get("sslTrustedCerts");


            if (!Strings.isNullOrEmpty(sslIdentityFile)) {
                sslParams.put(TibjmsSSL.IDENTITY, sslIdentityFile);
            }


            if (!Strings.isNullOrEmpty(sslIdentityPassword)) {
                sslParams.put(TibjmsSSL.PASSWORD, sslIdentityPassword);
            }
            if (!Strings.isNullOrEmpty(sslTrustedCerts)) {
                sslParams.put(TibjmsSSL.TRUSTED_CERTIFICATES, sslTrustedCerts);
            }

            String sslDebug = (String) emsServer.get("sslDebug");
            String sslVerifyHost = (String) emsServer.get("sslVerifyHost");
            String sslVerifyHostName = (String) emsServer.get("sslVerifyHostName");
            String sslVendor = (String) emsServer.get("sslVendor");

            if (!Strings.isNullOrEmpty(sslVendor)) {
                sslParams.put(TibjmsSSL.VENDOR, sslVendor);
            }
            if (!Strings.isNullOrEmpty(sslDebug)) {
                sslParams.put(TibjmsSSL.TRACE, sslDebug);
            }
            if (!Strings.isNullOrEmpty(sslVerifyHost)) {
                sslParams.put(TibjmsSSL.ENABLE_VERIFY_HOST, sslVerifyHost);
            }
            if (!Strings.isNullOrEmpty(sslVerifyHostName)) {
                sslParams.put(TibjmsSSL.ENABLE_VERIFY_HOST_NAME, sslVerifyHostName);
            }
        }

        try {
            TibjmsAdmin tibjmsAdmin = new TibjmsAdmin(emsURL, user, plainPassword, sslParams);
            collectMetrics(tibjmsAdmin, displayName, showSystem, showTemp, excludeQueuePatterns, excludeTopicPatterns);
        } catch (TibjmsAdminException e) {
            logger.error("Error while collecting metrics from Tibco EMS server [ " + displayName + " ]", e);
        }

    }

    private List<Pattern> buildPattern(List<String> patternStrings) {
        List<Pattern> patternList = new ArrayList<Pattern>();

        if (patternStrings != null) {
            for (String pattern : patternStrings) {
                patternList.add(Pattern.compile(pattern));
            }
        }

        return patternList;
    }

    private void collectMetrics(TibjmsAdmin tibjmsAdmin, String displayName, boolean showSystem, boolean showTemp, List<Pattern> excludeQueuePatterns, List<Pattern> excludeTopicPatterns) throws TibjmsAdminException {
        try {
            ServerInfo serverInfo = tibjmsAdmin.getInfo();
            collectServerInfo(serverInfo);

            collectConnectionInfo(tibjmsAdmin.getConnections());
            collectDurableInfo(tibjmsAdmin.getDurables());
            collectRoutesInfo(tibjmsAdmin.getRoutes());

            collectConsumerInfo(tibjmsAdmin.getConsumers(), excludeQueuePatterns, excludeTopicPatterns, showSystem, showTemp);
            collectProducerInfo(tibjmsAdmin.getProducersStatistics(), excludeQueuePatterns, excludeTopicPatterns, showSystem, showTemp);

            putQueueInfos(tibjmsAdmin, excludeQueuePatterns, showSystem, showTemp);
            putTopicInfos(tibjmsAdmin, excludeTopicPatterns, showSystem, showTemp);

            printMetrics(displayName);

        } catch (TibjmsAdminException e) {
            logger.error("Error while collecting metrics", e);
            throw e;
        }
    }


    private void printMetrics(String displayName) {
        String metricPrefix = configuration.getMetricPrefix();
        for (Map.Entry<String, String> metric : valueMap.entrySet()) {
            configuration.getMetricWriter().printMetric(metricPrefix + "|" + displayName + "|" + metric.getKey(), metric.getValue(), MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                    MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                    MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);
        }
    }

    private void putQueueInfos(TibjmsAdmin conn, List<Pattern> excludeQueuePatterns, boolean showSystem, boolean showTemp) throws TibjmsAdminException {
        logger.debug("Retrieving queue information");
        QueueInfo[] queueInfos = conn.getQueuesStatistics();

        if (queueInfos == null) {
            logger.warn("Unable to get queue statistics");
        } else {
            for (QueueInfo queueInfo : queueInfos) {
                if (shouldMonitorDestination(queueInfo.getName(), excludeQueuePatterns, showSystem, showTemp)) {
                    logger.info("Publishing metrics for queue " + queueInfo.getName());
                    putQueueInfo(queueInfo);
                }
            }
        }
    }

    private void putQueueInfo(QueueInfo queueInfo) {

        String prefix = "Queues|" + queueInfo.getName();

        putDestinationInfo(prefix, queueInfo);
        putDestinationValue(prefix, "InTransitCount", queueInfo.getInTransitMessageCount());
        putDestinationValue(prefix, "ReceiverCount", queueInfo.getReceiverCount());
        putDestinationValue(prefix, "MaxRedelivery", queueInfo.getMaxRedelivery());
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

    private void putDestinationValue(String prefix, String key, long value) {


        valueMap.put(prefix + "|" + key, Long.toString(value));
    }

    private void putTopicInfos(TibjmsAdmin conn, List<Pattern> excludeTopicPatterns, boolean showSystem, boolean showTemp) throws TibjmsAdminException {
        logger.debug("Retrieving topic information");
        TopicInfo[] topicInfos = conn.getTopicsStatistics();

        if (topicInfos == null) {
            logger.warn("Unable to get topic statistics");
        } else {
            for (TopicInfo topicInfo : topicInfos) {
                if (shouldMonitorDestination(topicInfo.getName(), excludeTopicPatterns, showSystem, showTemp)) {
                    logger.info("Publishing metrics for topic " + topicInfo.getName());
                    putTopicInfo(topicInfo);
                }
            }
        }
    }

    private void putTopicInfo(TopicInfo topicInfo) {
        String prefix = "Topics|" + topicInfo.getName();

        putDestinationInfo(prefix, topicInfo);
        putDestinationValue(prefix, "ConsumerCount", topicInfo.getConsumerCount());
        putDestinationValue(prefix, "SubscriberCount", topicInfo.getSubscriberCount());
        putDestinationValue(prefix, "ActiveDurableCount", topicInfo.getActiveDurableCount());
        putDestinationValue(prefix, "DurableCount", topicInfo.getDurableCount());
    }

    private boolean shouldMonitorDestination(String destName, List<Pattern> patternsToExclude, boolean showSystem, boolean showTemp) {

        if (destName.startsWith("$TMP$.") && !showTemp) {
            logger.debug("Skipping temporary destination '" + destName + "'");
            return false;
        } else if (destName.startsWith("$sys.") && !showSystem) {
            logger.debug("Skipping system destination '" + destName + "'");
            return false;
        } else {
            for (Pattern patternToExclude : patternsToExclude) {
                Matcher matcher = patternToExclude.matcher(destName);
                if (matcher.matches()) {
                    logger.debug(String.format("Skipping queue '%s' due to excluded pattern '%s'",
                            destName, patternToExclude.pattern()));
                    return false;
                }
            }

            return true;
        }
    }

    private void putServerValue(String key, Number value) {

        if (value == null) {
            logger.warn("Found null value for key [" + key + "]. Ignoring this metric");
        }

        valueMap.put(key, value.toString());
    }

    private void collectProducerInfo(ProducerInfo[] producers, List<Pattern> excludeQueuePatterns, List<Pattern> excludeTopicPatterns, boolean showSystem, boolean showTemp) {

        if (producers == null || producers.length <= 0) {
            logger.info("No producers found to get the producers metrics");
        }

        for (ProducerInfo producerInfo : producers) {
            String prefix = "Producers|" + producerInfo.getID() + "|";
            int destinationType = producerInfo.getDestinationType();
            String destinationName = producerInfo.getDestinationName();

            if (destinationType == 2) {
                boolean monitor = shouldMonitorDestination(destinationName, excludeTopicPatterns, showSystem, showTemp);
                if (!monitor) { //Skipping this destination as configured
                    return;
                }
            } else {
                boolean monitor = shouldMonitorDestination(destinationName, excludeQueuePatterns, showSystem, showTemp);
                if (!monitor) { //Skipping this destination as configured
                    return;
                }
            }

            if (destinationType == 2) {
                prefix += "topic|";
            } else {
                prefix += "queue|";
            }
            prefix += destinationName;

            putDestinationValue(prefix, "ConnectionID", producerInfo.getConnectionID());
            putDestinationValue(prefix, "SessionID", producerInfo.getSessionID());
            putDestinationValue(prefix, "SessionID", producerInfo.getCreateTime());
            StatData statistics = producerInfo.getStatistics();
            if (statistics != null) {
                putDestinationValue(prefix, "TotalMessages", statistics.getTotalMessages());
                putDestinationValue(prefix, "TotalBytes", statistics.getTotalBytes());
                putDestinationValue(prefix, "MessageRate", statistics.getMessageRate());
            }

        }

    }

    private void collectConsumerInfo(ConsumerInfo[] consumers, List<Pattern> excludeQueuePatterns, List<Pattern> excludeTopicPatterns, boolean showSystem, boolean showTemp) {
        if (consumers == null || consumers.length <= 0) {
            logger.info("No consumers found to get the consumers metrics");
        }

        for (ConsumerInfo consumerInfo : consumers) {
            String prefix = "Consumers|" + consumerInfo.getID() + "|";
            int destinationType = consumerInfo.getDestinationType();
            String destinationName = consumerInfo.getDestinationName();

            if (destinationType == 2) {
                boolean monitor = shouldMonitorDestination(destinationName, excludeTopicPatterns, showSystem, showTemp);
                if (!monitor) { //Skipping this destination as configured
                    return;
                }
            } else {
                boolean monitor = shouldMonitorDestination(destinationName, excludeQueuePatterns, showSystem, showTemp);
                if (!monitor) { //Skipping this destination as configured
                    return;
                }
            }

            if (destinationType == 2) {
                prefix += "topic|";
            } else {
                prefix += "queue|";
            }
            prefix += destinationName;

            putDestinationValue(prefix, "ConnectionID", consumerInfo.getConnectionID());
            putDestinationValue(prefix, "SessionID", consumerInfo.getSessionID());
            putDestinationValue(prefix, "SessionID", consumerInfo.getCreateTime());
            StatData statistics = consumerInfo.getStatistics();
            if (statistics != null) {
                putDestinationValue(prefix, "TotalMessages", statistics.getTotalMessages());
                putDestinationValue(prefix, "TotalBytes", statistics.getTotalBytes());
                putDestinationValue(prefix, "MessageRate", statistics.getMessageRate());
            }
        }

    }

    private void collectRoutesInfo(RouteInfo[] routes) {
        if (routes == null || routes.length <= 0) {
            logger.info("No routes found to get the routes metrics");
        }

        for (RouteInfo routeInfo : routes) {
            String prefix = "Routes|" + routeInfo.getName();
            putDestinationValue(prefix, "InboundMessageRate", routeInfo.getInboundStatistics().getMessageRate());
            putDestinationValue(prefix, "InboundTotalMessages", routeInfo.getInboundStatistics().getTotalMessages());
            putDestinationValue(prefix, "OutboundMessageRate", routeInfo.getOutboundStatistics().getMessageRate());
            putDestinationValue(prefix, "OutboundTotalMessages", routeInfo.getOutboundStatistics().getTotalMessages());
        }
    }

    private void collectDurableInfo(DurableInfo[] durables) {
        if (durables == null || durables.length <= 0) {
            logger.info("No durables found to get the durables metrics");
        }

        for (DurableInfo durableInfo : durables) {
            String prefix = "Durables|" + durableInfo.getDurableName();
            putDestinationValue(prefix, "PendingMessageCount", durableInfo.getPendingMessageCount());
            putDestinationValue(prefix, "PendingMessageSize", durableInfo.getPendingMessageSize());
        }
    }

    private void collectConnectionInfo(ConnectionInfo[] connections) {

        if (connections == null || connections.length <= 0) {
            logger.info("No Connections found to get the connection metrics");
        }

        for (ConnectionInfo connectionInfo : connections) {
            String prefix = "Connections|" + connectionInfo.getID() + "|" + connectionInfo.getHost() + "|" + connectionInfo.getType();
            putDestinationValue(prefix, "SessionCount", connectionInfo.getSessionCount());
            putDestinationValue(prefix, "ConsumerCount", connectionInfo.getConsumerCount());
            putDestinationValue(prefix, "ProducerCount", connectionInfo.getProducerCount());
            putDestinationValue(prefix, "StartTime", connectionInfo.getStartTime());
            putDestinationValue(prefix, "UpTime", connectionInfo.getUpTime());
        }

    }

    private void collectServerInfo(ServerInfo serverInfo) {

        putServerValue("DiskReadRate", serverInfo.getDiskReadRate());
        putServerValue("DiskWriteRate", serverInfo.getDiskWriteRate());


        putServerValue("State", serverInfo.getState());
        putServerValue("MsgMemory", serverInfo.getMsgMem());
        putServerValue("MaxMsgMemory", serverInfo.getMaxMsgMemory());
        putServerValue("MemoryPooled", serverInfo.getMsgMemPooled());

        putServerValue("SyncDBSize", serverInfo.getSyncDBSize());
        putServerValue("AsyncDBSize", serverInfo.getAsyncDBSize());
        putServerValue("QueueCount", serverInfo.getQueueCount());
        putServerValue("TopicCount", serverInfo.getTopicCount());
        putServerValue("DurableCount", serverInfo.getDurableCount());


        putServerValue("InboundBytesRate", serverInfo.getInboundBytesRate());
        putServerValue("InboundMessageRate", serverInfo.getInboundMessageRate());
        putServerValue("OutboundBytesRate", serverInfo.getOutboundBytesRate());
        putServerValue("OutboundMessageRate", serverInfo.getOutboundMessageRate());

        putServerValue("ConnectionCount", serverInfo.getConnectionCount());
        putServerValue("MaxConnections", serverInfo.getMaxConnections());

        putServerValue("ProducerCount", serverInfo.getProducerCount());
        putServerValue("ConsumerCount", serverInfo.getConsumerCount());
        putServerValue("SessionCount", serverInfo.getSessionCount());
        putServerValue("StartTime", serverInfo.getStartTime());
        putServerValue("UpTime", serverInfo.getUpTime());

        putServerValue("PendingMessageCount", serverInfo.getPendingMessageCount());
        putServerValue("PendingMessageSize", serverInfo.getPendingMessageSize());
        putServerValue("InboundMessageCount", serverInfo.getInboundMessageCount());
        putServerValue("OutboundMessageCount", serverInfo.getOutboundMessageCount());

        putServerValue("InboundMessagesPerMinute", getDeltaValue("InboundMessageCount"));
        putServerValue("OutboundMessagesPerMinute", getDeltaValue("OutboundMessageCount"));

    }

    protected long getDeltaValue(String key) {
        long delta = 0;

        String resultStr = valueMap.get(key);
        String oldResultStr = null;

        if (resultStr != null) {
            long result = Long.valueOf(resultStr);

            oldResultStr = oldValuesMap.get(key);
            if (oldResultStr != null) {
                long oldResult = Long.valueOf(oldResultStr);
                delta = result - oldResult;
            }
        }

        logger.info(String.format("key='%s' old=%s new=%s diff=%d",
                key, oldResultStr, resultStr, delta));

        return delta;
    }

    private String getPassword(String password, String encryptedPassword, String encryptionKey) {

        if (!Strings.isNullOrEmpty(password)) {
            return password;
        }

        try {
            if (Strings.isNullOrEmpty(encryptedPassword)) {
                return "";
            }
            Map<String, String> args = Maps.newHashMap();
            args.put(TaskInputArgs.PASSWORD_ENCRYPTED, encryptedPassword);
            args.put(TaskInputArgs.ENCRYPTION_KEY, encryptionKey);
            return CryptoUtil.getPassword(args);
        } catch (IllegalArgumentException e) {
            String msg = "Encryption Key not specified. Please set the value in config.yml.";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
}
