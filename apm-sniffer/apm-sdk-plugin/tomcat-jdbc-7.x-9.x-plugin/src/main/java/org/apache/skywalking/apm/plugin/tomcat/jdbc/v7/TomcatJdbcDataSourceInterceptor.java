/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.skywalking.apm.plugin.tomcat.jdbc.v7;

import org.apache.skywalking.apm.agent.core.meter.MeterFactory;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.plugin.jdbc.connectionurl.parser.URLParser;
import org.apache.skywalking.apm.plugin.jdbc.trace.ConnectionInfo;
import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link TomcatJdbcDataSourceInterceptor} intercepted the method of tomcat-jdbc connectionPool info.
 */
public class TomcatJdbcDataSourceInterceptor implements InstanceMethodsAroundInterceptor {

    private static final String METER_NAME = "datasource";

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {

    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret) throws Throwable {
        DataSource dataSource = (DataSource) objInst;
        ConnectionInfo connectionInfo = URLParser.parser(dataSource.getUrl());
        String tagValue = connectionInfo.getDatabaseName() + "_" + connectionInfo.getDatabasePeer();
        final Map<String, Function<ConnectionPool, Supplier<Double>>> poolMetricMap = getPoolMetrics();
        final Map<String, Function<ConnectionPool, Supplier<Double>>> metricConfigMap = getConfigMetrics();
        poolMetricMap.forEach((key, value) -> MeterFactory.gauge(METER_NAME, value.apply(dataSource.getPool()))
                .tag("name", tagValue).tag("status", key).build());
        metricConfigMap.forEach((key, value) -> MeterFactory.gauge(METER_NAME, value.apply(dataSource.getPool()))
                .tag("name", tagValue).tag("status", key).build());
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {

    }

    private Map<String, Function<ConnectionPool, Supplier<Double>>> getPoolMetrics() {
        final Map<String, Function<ConnectionPool, Supplier<Double>>> poolMetricMap = new HashMap<>();
        poolMetricMap.put("activeConnections", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getActive());
        poolMetricMap.put("totalConnections", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getSize());
        poolMetricMap.put("idleConnections", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getIdle());
        poolMetricMap.put("threadsAwaitingConnection", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getWaitCount());
        return poolMetricMap;
    }

    private Map<String, Function<ConnectionPool, Supplier<Double>>> getConfigMetrics() {
        final Map<String, Function<ConnectionPool, Supplier<Double>>> metricConfigMap = new HashMap<>();
        metricConfigMap.put("validationTimeout", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getValidationQueryTimeout());
        metricConfigMap.put("minIdle", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getMinIdle());
        metricConfigMap.put("maxActive", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getMaxActive());
        metricConfigMap.put("maxWait", (ConnectionPool connectionPool) -> () -> (double) connectionPool.getJmxPool().getMaxWait());
        return metricConfigMap;
    }
}
