package com.appdynamics.extensions.tibcoems;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import com.singularity.ee.util.clock.ClockUtils;
import com.tibco.tibjms.admin.QueueInfo;
import com.tibco.tibjms.admin.ServerInfo;
import com.tibco.tibjms.admin.StatData;
import com.tibco.tibjms.admin.TibjmsAdmin;

/**
 * 
 * @author Appdynamics
 * 
 */
public class TibcoEMSMonitor extends JavaExtensionHelper {

	private volatile String tierName;
	private volatile String serverName;
	private volatile int refreshIntervalInExecutionTicks;
	private volatile int currentNumExecutionTicks = -1;
	private volatile String userid;
	private volatile String password;
	private volatile String hostname;
	private volatile String port;

	public TibcoEMSMonitor() {
		String msg = "Using Monitor Version ["+getImplementationVersion()+"]";
		logger.info(msg);
		System.out.println(msg);
		oldValueMap = Collections
				.synchronizedMap(new HashMap<String, String>());
	}

	protected void parseArgs(Map<String, String> args) {
		super.parseArgs(args);
		// if no tier defined then create metrics for all tiers
		tierName = getArg(args, "tier", null);
		userid = getArg(args, "userid", null);
		password = getArg(args, "password", null);
		hostname = getArg(args, "hostname", null);
		port = getArg(args, "port", "7222");
		serverName = getArg(args, "emsservername", null);

		int refreshIntervalSecs = Integer.parseInt(getArg(args,
				"refresh-interval", "60"));

		if (refreshIntervalSecs <= 60) {
			refreshIntervalInExecutionTicks = 1;
		} else {
			// Convert refresh interval to milliseconds and round up to the
			// nearest minute timeslice.
			// From that we can get the number of 60 second ticks before the
			// next refresh.
			// We do this to prevent time drift issues from preventing this task
			// from running.
			refreshIntervalInExecutionTicks = (int) (ClockUtils
					.roundUpTimestampToNextMinute(refreshIntervalSecs * 1000) / 60000);
		}

		if (currentNumExecutionTicks == -1) {
			// This is the first time we've parsed the args. Assume we refresh
			// the data
			// the next time we execute the monitor.
			currentNumExecutionTicks = refreshIntervalInExecutionTicks;
		}
	}

	private TibjmsAdmin connect(){
		TibjmsAdmin tibcoAdmin = null;
		try{
			tibcoAdmin = new TibjmsAdmin("tcp://" + hostname + ":"
					+ port, userid, password);
			//need improvement
		}catch(Throwable e){
			logger.debug("Issue while connecting with EMS server...",e);
		}
		return tibcoAdmin;
	}

	// collects all monitoring data for this time period from database
	private Map<String, String> putValuesIntoMap() throws Exception {
		logger.debug("Started adding metrics");
		Map<String, String> columnName2Value = new HashMap<String, String>();
		TibjmsAdmin conn = null;
		boolean debug = logger.isDebugEnabled();
		try {
			conn = connect();
			if(conn == null){
				logger.debug("Connection Failed! " + conn);
			}
			ServerInfo serverInfo = conn.getInfo();
			columnName2Value.put("ConnectionCount",
					new Integer(serverInfo.getConnectionCount()-1).toString());
			columnName2Value.put("MaxConnections",
					new Integer(serverInfo.getMaxConnections()).toString());
			columnName2Value.put("ProducerCount",
					new Long(serverInfo.getProducerCount()).toString());
			columnName2Value.put("ConsumerCount",
					new Long(serverInfo.getConsumerCount()).toString());
			columnName2Value.put("PendingMessageCount",
					new Long(serverInfo.getPendingMessageCount()).toString());
			columnName2Value.put("PendingMessageSize",
					new Long(serverInfo.getPendingMessageSize()).toString());

			columnName2Value.put("InboundMessageCount",
					new Long(serverInfo.getInboundMessageCount()).toString());
			columnName2Value.put("InboundMessageRate",
					new Long(serverInfo.getInboundMessageRate()).toString());
			columnName2Value.put("InboundByteRate",
					new Long(serverInfo.getInboundBytesRate()).toString());

			columnName2Value.put("OutboundMessageCount",
					new Long(serverInfo.getOutboundMessageCount()).toString());
			columnName2Value.put("OutboundMessageRate",
					new Long(serverInfo.getOutboundMessageRate()).toString());
			columnName2Value.put("OutboundByteRate",
					new Long(serverInfo.getOutboundBytesRate()).toString());




			// get most accurate time
			currentTime = System.currentTimeMillis();
			logger.debug("Retrieving Queue Information");
			QueueInfo[] queueInformation = conn.getQueuesStatistics();
			for (int i = 0; i < queueInformation.length; i++) {
				QueueInfo queueInfo = queueInformation[i];
				if(!queueInfo.getName().contains("$TMP$")){// skip $TMP$ queues

					columnName2Value.put(queueInfo.getName() + "|ConsumerCount",
							new Integer(queueInfo.getConsumerCount()).toString());
					columnName2Value.put(queueInfo.getName() + "|ReceiverCount",
							new Long(queueInfo.getReceiverCount()).toString());
					columnName2Value.put(queueInfo.getName()+ "|DeliveredMessageCount", 
							new Long(queueInfo.getDeliveredMessageCount()).toString());
					columnName2Value.put(queueInfo.getName()+ "|PendingMessageCount", 
							new Long(queueInfo.getPendingMessageCount()).toString());
					columnName2Value.put(queueInfo.getName()+ "|InTransitMessageCount", 
							new Long(queueInfo.getInTransitMessageCount()).toString());
					columnName2Value.put(queueInfo.getName()+ "|FlowControlMaxBytes", 
							new Long(queueInfo.getFlowControlMaxBytes()).toString());
					columnName2Value.put(queueInfo.getName()+ "|PendingMessageSize", 
							new Long(queueInfo.getPendingMessageSize()).toString());
					columnName2Value.put(queueInfo.getName() + "|MaxMsgs",
							new Long(queueInfo.getMaxMsgs()).toString());
					columnName2Value.put(queueInfo.getName() + "|MaxBytes",
							new Long(queueInfo.getMaxBytes()).toString());

					StatData inboundStatData = queueInfo.getInboundStatistics();
					if(inboundStatData != null){
						columnName2Value.put(queueInfo.getName() + "|InboundByteRate",
								new Long(inboundStatData.getByteRate()).toString());
						columnName2Value.put(queueInfo.getName() + "|InboundMessageRate",
								new Long(inboundStatData.getMessageRate()).toString());
						columnName2Value.put(queueInfo.getName() + "|InboundByteCount",
								new Long(inboundStatData.getTotalBytes()).toString());
						columnName2Value.put(queueInfo.getName() + "|InboundMessageCount",
								new Long(inboundStatData.getTotalMessages()).toString());
					}

					StatData outboundStatData = queueInfo.getOutboundStatistics();
					if(outboundStatData != null){
						columnName2Value.put(queueInfo.getName() + "|OutboundByteRate",
								new Long(outboundStatData.getByteRate()).toString());
						columnName2Value.put(queueInfo.getName() + "|OutboundMessageRate",
								new Long(outboundStatData.getMessageRate()).toString());
						columnName2Value.put(queueInfo.getName() + "|OutboundByteCount",
								new Long(outboundStatData.getTotalBytes()).toString());
						columnName2Value.put(queueInfo.getName() + "|OutboundMessageCount",
								new Long(outboundStatData.getTotalMessages()).toString());
					}


					// Compare Pending Message Size against the Max Message Size.
					if (debug) {
						logger.debug("Queue Name " + queueInfo.getName());
						logger.debug("Consumer Count "
								+ queueInfo.getConsumerCount());
						logger.debug("Delivered Message Count "
								+ queueInfo.getDeliveredMessageCount());
						logger.debug("Flow Control Max Bytes "
								+ queueInfo.getFlowControlMaxBytes());
						logger.debug("Pending Message Count "
								+ queueInfo.getPendingMessageCount());
						logger.debug("FlowControlMaxBytes"
								+ queueInfo.getFlowControlMaxBytes());
						logger.debug("MaxMessages " + queueInfo.getMaxMsgs());
						logger.debug("ReceiverCount "
								+ queueInfo.getReceiverCount());
					}
				}

			}
			logger.info("Closing Connection to Server");
			conn.close();

		} catch (com.tibco.tibjms.admin.TibjmsAdminException ex) {
			logger.error("Error connecting to EMS Server" + serverName + " "
					+ port + " " + this.hostname + " " + this.password, ex);

		} catch (Exception ex) {
			logger.error("Error getting performance data from Tibco EMS.", ex);
			throw ex;

		} finally {
			logger.info("Closing connection");
			conn.close();
		}
		return Collections.synchronizedMap(columnName2Value);
	}

	public TaskOutput execute(Map<String, String> taskArguments,
			TaskExecutionContext taskContext) throws TaskExecutionException {

		startExecute(taskArguments, taskContext);

		logger.debug("Starting METRIC COLLECTION for Tibco EMS  Monitor");

		Map<String, String> map;
		try {
			map = putValuesIntoMap();
			Iterator<String> keys = map.keySet().iterator();
			while (keys.hasNext()) {
				String key = (String) keys.next();
				String value = map.get(key);
				printMetric(key, value);
				if (logger.isDebugEnabled()) {
					logger.debug("Key :" + key + " : " + value);
				}

			}
		} catch (Exception e) {
			logger.error("Error collectng EMS metrics ", e);
		}

		return this.finishExecute();
	}

	private void printMetric(String name, String value) {
		printMetric(name, value,
				MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
				MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
				MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE);

	}

	protected String getMetricPrefix() {
		if (tierName != null) {
			return "Server|Component:" + tierName + "|" + serverName + "|";
		} else {
			return "Custom Metrics|Tibco EMS Server|";
		}
	}
	public static String getImplementationVersion(){
		return TibcoEMSMonitor.class.getPackage().getImplementationTitle();
	}
}
