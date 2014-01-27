# AppDynamics TibcoEMS Monitoring Extension

This extension works only with the standalone machine agent.

##Use Case
The TibcoEMS Monitoring extension collects metrics from an Tibco EMS messaging server and uploads them to the AppDynamics Controller. 


##Installation

1. Add following jar file in lib directory.
<pre>
```
jms.jar
tibjmsapps.jar
tibjmsufo.jar
tibrvjms.jar
tibcrypt.jar
tibemsd_sec.jar
tibjms.jar
tibjmsadmin.jar
```
</pre>
2. Run "mvn clean install"
3. Download and unzip the file 'target/TibcoEMSMonitor.zip' to \<machineagent install dir\>/monitors
4. Open monitor.xml and configure the TibcoEMS arguments.
5. Add the line "statistics=enabled" to tibemsd.conf


## Metrics

### Global Instance Metrics

| Metric Name          | Description                                                           |
| :------------------- | :-------------------------------------------------------------------- |
| DiskReadRate         | Rate at which messages are being read from disk, in bytes per second  |
| DiskWriteRate        | Rate at which messages are being written to disk, in bytes per second |
| ConnectionCount      | Current number of connections                                         |
| MaxConnections       | Maximum number of connections allowed by the server                   |
| ProducerCount        | Total number of producers                                             |
| ConsumerCount        | Total number of consumers                                             |
| PendingMessageCount  | Total number of pending messages                                      |
| PendingMessageSize   | Total size of pending messages in bytes                               |
| InboundMessageCount  | Number of inbound messages                                            |
| InboundMessageRate   | Number of inbound messages per second                                 |
| InboundBytesRate     | Volume of inbound bytes per second                                    |
| OutboundMessageCount | Number of outbound messages                                           |
| OutboundMessageRate  | Number of outbound messages per second                                |
| OutboundBytesRate    | Volume of outbound bytes per second                                   |


### Per-Queue Metrics

| Metric Name           | Description |
| :-------------------- | :---------- |
| ConsumerCount         | Number of consumers for this destination |
| ReceiverCount         | Number of active receivers on this queue |
| DeliveredMessageCount | Total number of messages that have been delivered to consumer applications but have not yet been acknowledged |
| PendingMessageCount   | Total number of pending messages for this destination |
| InTransitCount        | Total number of messages that have been delivered to the queue owner but have not yet been acknowledged |
| FlowControlMaxBytes   | Volume of pending messages (in bytes) at which flow control is enabled for this destination |
| PendingMessageSize    | Total size for all pending messages for this destination |
| MaxMsgs               | Maximum number of messages that the server will store for pending messages bound for this destination |
| MaxBytes              | Maximum number of message bytes that the server will store for pending messages bound for this destination |
| InboundByteRate       | Bytes received per second |
| InboundMessageRate    | Messages received per second |
| InboundByteCount      | Total number of bytes received |
| InboundMessageCount   | Total number of messages received |
| OutboundByteRate      | Bytes sent per second |
| OutboundMessageRate   | Messages sent per second |
| OutboundByteCount     | Total number of bytes sent |
| OutboundMessageCount  | Total number of messages sent |


##Contributing

Always feel free to fork and contribute any changes directly here on GitHub.

##Community

##Support

For any questions or feature request, please contact [AppDynamics Center of Excellence](mailto:ace-request@appdynamics.com).


