package com.singularity.ee.agent.systemagent.monitors.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;

public abstract class JavaServersMonitor extends AManagedMonitor
{
    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    protected volatile Map<String, String> oldValueMap;
    protected volatile Map<String, String> valueMap;

    public abstract TaskOutput execute(Map<String, String> taskArguments, TaskExecutionContext taskContext)
            throws TaskExecutionException;

    protected void parseArgs(Map<String, String> args)
    {
    }

    // safe way to get parameter from monitor, but if null, use default
    protected String getArg(Map<String, String> args, String arg, String oldVal)
    {
        String result = args.get(arg);

        if (result == null)
            return oldVal;

        return result;
    }

    protected void printMetric(String name, String value, String aggType, String timeRollup, String clusterRollup)
    {
        String metricName = getMetricPrefix() + name;
        MetricWriter metricWriter = getMetricWriter(metricName, aggType, timeRollup, clusterRollup);
        metricWriter.printMetric(value);

        // just for debug output
        if (logger.isDebugEnabled())
        {
            logger.debug(String.format("Publishing metric: name='%s' value='%s' aggType='%s' " +
                            "timeRollup='%s' clusterRollup='%s'",
                    metricName, value, aggType, timeRollup, clusterRollup));
        }
    }

    protected String getMetricPrefix()
    {
        return "";
    }

    @SuppressWarnings("unused")
    protected void startExecute(Map<String, String> taskArguments, TaskExecutionContext taskContext)
    {
        valueMap = Collections.synchronizedMap(new HashMap<String, String>());
        parseArgs(taskArguments);
    }

    protected TaskOutput finishExecute()
    {
        oldValueMap = valueMap;

        // just for debug output
        logger.debug("Finished execution");

        return new TaskOutput("Success");
    }

    @SuppressWarnings("unused")
    protected long getValue(String key)
    {
        if (!valueMap.containsKey(key))
            return 0;

        return Long.valueOf(valueMap.get(key));
    }

    protected long getDeltaValue(String key)
    {
        long delta = 0;

        String resultStr = valueMap.get(key);
        String oldResultStr = null;

        if (resultStr != null)
        {
            long result = Long.valueOf(resultStr);

            oldResultStr = oldValueMap.get(key);
            if (oldResultStr != null)
            {
                long oldResult = Long.valueOf(oldResultStr);
                delta = result - oldResult;
            }
        }

        logger.info(String.format("key='%s' old=%s new=%s diff=%d",
                key, oldResultStr, resultStr, delta));

        return delta;
    }

}
