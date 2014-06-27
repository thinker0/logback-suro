/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.input;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.Message;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class LogbackAppender extends AppenderBase<ILoggingEvent> {
    protected static String localHostAddr = null;

    static {
        try {
            localHostAddr = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            localHostAddr = "N/A";
        }
    }

    private String formatterClass = JsonLog4jFormatter.class.getName();

    public void setFormatterClass(String formatterClass) {
        this.formatterClass = formatterClass;
    }

    public String getFormatterClass() {
        return formatterClass;
    }

    private String datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss,SSS";

    public void setDatetimeFormat(String datetimeFormat) {
        this.datetimeFormat = datetimeFormat;
    }

    public String getDatetimeFormat() {
        return datetimeFormat;
    }

    private String routingKey = "";

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    private String app = "defaultApp";

    public void setApp(String app) {
        this.app = app;
    }

    public String getApp() {
        return app;
    }

    private String compression = "1";

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public String getCompression() {
        return compression;
    }

    private String loadBalancerType = "eureka";

    public void setLoadBalancerType(String loadBalancerType) {
        this.loadBalancerType = loadBalancerType;
    }

    private String getLoadBalancerType() {
        return loadBalancerType;
    }

    private String loadBalancerServer;

    public void setLoadBalancerServer(String loadBalancerServer) {
        this.loadBalancerServer = loadBalancerServer;
    }

    private String getLoadBalancerServer() {
        return loadBalancerServer;
    }

    private String asyncQueueType = "memory";

    public void setAsyncQueueType(String asyncQueueType) {
        this.asyncQueueType = asyncQueueType;
    }

    public String getAsyncQueueType() {
        return asyncQueueType;
    }

    private String asyncMemoryQueueCapacity = "10000";

    public void setAsyncMemoryQueueCapacity(String memoryQueueCapacity) {
        this.asyncMemoryQueueCapacity = memoryQueueCapacity;
    }

    public String getAsyncMemoryQueueCapacity() {
        return asyncMemoryQueueCapacity;
    }

    private String asyncFileQueuePath = "/logs/suroClient";

    public String getAsyncFileQueuePath() {
        return asyncFileQueuePath;
    }

    public void setAsyncFileQueuePath(String fileQueuePath) {
        this.asyncFileQueuePath = fileQueuePath;
    }

    private String clientType = "async";

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getClientType() {
        return clientType;
    }

    protected SuroClient client;

    private ObjectMapper objectMapper;

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ClientConfig.LOG4J_FORMATTER, formatterClass);
        properties.setProperty(ClientConfig.LOG4J_DATETIMEFORMAT, datetimeFormat);
        properties.setProperty(ClientConfig.LOG4J_ROUTING_KEY, routingKey);
        properties.setProperty(ClientConfig.APP, app);
        properties.setProperty(ClientConfig.COMPRESSION, compression);
        properties.setProperty(ClientConfig.LB_TYPE, loadBalancerType);
        properties.setProperty(ClientConfig.LB_SERVER, loadBalancerServer);
        properties.setProperty(ClientConfig.ASYNC_MEMORYQUEUE_CAPACITY, asyncMemoryQueueCapacity);
        properties.setProperty(ClientConfig.ASYNC_QUEUE_TYPE, asyncQueueType);
        properties.setProperty(ClientConfig.ASYNC_FILEQUEUE_PATH, asyncFileQueuePath);
        properties.setProperty(ClientConfig.CLIENT_TYPE, clientType);

        return properties;
    }


    @Override
    public void start() {
        this.client = new SuroClient(createProperties());
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void stop() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        try {
            final Map payload = format(event);
            final Message message = new Message(routingKey, this.objectMapper.writeValueAsBytes(payload));
            this.client.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public Map format(ILoggingEvent event) {
        Map<String, Object> obj = Maps.newHashMap();
        obj.put("level", event.getLevel().levelStr);
        obj.put("logger", event.getLoggerName());
        obj.put("timestamp", event.getTimeStamp());
        obj.put("hostname", localHostAddr);
        obj.put("message", event.getFormattedMessage());
        addExceptionsIf(obj, "exceptions", event.getThrowableProxy());
        return obj;
    }

    private void addExceptionsIf(Map obj, String name, IThrowableProxy proxy) {
        if (proxy != null) {
            obj.put(name, addExceptions(proxy));
        }
    }

    private Map addExceptions(IThrowableProxy proxy) {
        final Map<String, Object> obj = Maps.newHashMap();
        obj.put("exceptionClass", proxy.getClassName());
        obj.put("exceptionMessage", proxy.getMessage());

        StackTraceElementProxy[] stackTrace = proxy.getStackTraceElementProxyArray();
        if (stackTrace != null) {
            List<Object> list = Lists.newArrayList();
            for (StackTraceElementProxy trace : stackTrace) {
                list.add(trace.toString());
            }
            obj.put("stacktrace", list);
        }
        addExceptionsIf(obj, "cause", proxy.getCause());
        final IThrowableProxy[] suppressed = proxy.getSuppressed();
        if (suppressed != null) {
            List<Object> list = Lists.newArrayList();
            for (IThrowableProxy throwable : suppressed) {
                if (throwable != null) {
                    list.add(addExceptions(throwable));
                }
            }
            if (!list.isEmpty()) {
                obj.put("suppressed", list);
            }
        }

        return obj;
    }

}
