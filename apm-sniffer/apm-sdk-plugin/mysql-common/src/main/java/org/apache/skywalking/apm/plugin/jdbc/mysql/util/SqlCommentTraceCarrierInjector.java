/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.plugin.jdbc.mysql.util;

import org.apache.skywalking.apm.agent.core.base64.Base64;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.SW8CarrierItem;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.jdbc.trace.ConnectionInfo;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * SqlCommentTraceIdInjector
 */
public class SqlCommentTraceCarrierInjector {

    private static final ILog LOGGER = LogManager.getLogger(SqlCommentTraceCarrierInjector.class);
    public final static String TRACE_CARRIER_START_WITH = "/* [sw_trace_carrier:";
    public final static String TRACE_CARRIER_END_WITH = "] */";
    public final static String TRACE_CARRIER_END_WITH_ESCAPE = "] \\*/";

    public static String inject(String sql, String methodName, ContextCarrier contextCarrier, ConnectionInfo connectionInfo) {
        if (StringUtil.isNotBlank(sql)) {
            if (!sql.startsWith("/*") && !sql.startsWith("SET") && !sql.startsWith("select @@") && !sql.startsWith("SELECT @@")) {
                try {
                    if (contextCarrier != null) {
                        ContextManager.inject(contextCarrier);
                        CarrierItem next = contextCarrier.items();
                        String traceCarrier = "";
                        while (next.hasNext()) {
                            next = next.next();
                            if (SW8CarrierItem.HEADER_NAME.equals(next.getHeadKey())) {
                                traceCarrier = next.getHeadValue();
                                break;
                            }
                        }
                        return TRACE_CARRIER_START_WITH + traceCarrier + TRACE_CARRIER_END_WITH + sql;
                    } else {
                        AbstractSpan span = ContextManager.createLocalSpan(connectionInfo.getDBType() + "/Connection/" + methodName);
                        span.setLayer(SpanLayer.DB);
                        Tags.DB_TYPE.set(span, connectionInfo.getDBType());
                        span.setComponent(ComponentsDefine.MYSQL_JDBC_DRIVER);
                        Tags.DB_STATEMENT.set(span, sql);
                        ContextManager.stopSpan();
                        String injectedSql = generateNextExitSpanCarrier(span) + sql;
                        LOGGER.info("==> after injected sql is: {}", injectedSql);
                        return generateNextExitSpanCarrier(span) + sql;

                    }
                } catch (Exception e) {
                    LOGGER.error("carrier inject failed:", e);
                    return sql;
                }

            }
        }
        return sql;
    }

    private static String generateNextExitSpanCarrier(AbstractSpan localSpan) {
        String traceId = ContextManager.getGlobalTraceId();
        String segmentId = ContextManager.getSegmentId();
        int spanId = localSpan.getSpanId() + 1;
        String serviceName = Config.Agent.SERVICE_NAME;
        String instanceName = Config.Agent.INSTANCE_NAME;
        String primaryEndpointName = ContextManager.getPrimaryEndpointName();
        String swTraceCarrier = StringUtil.join(
                '-',
                "1",
                Base64.encode(traceId),
                Base64.encode(segmentId),
                spanId + "",
                Base64.encode(serviceName),
                Base64.encode(instanceName),
                Base64.encode(primaryEndpointName == null ? "" : primaryEndpointName),
                Base64.encode("mysql")
        );
        return TRACE_CARRIER_START_WITH + swTraceCarrier + TRACE_CARRIER_END_WITH;
    }

    public static String getOriginalSql(String carrierSql) {
        if (carrierSql.startsWith(TRACE_CARRIER_START_WITH) && carrierSql.contains(TRACE_CARRIER_END_WITH)) {
            return carrierSql.split(TRACE_CARRIER_END_WITH_ESCAPE)[1];
        } else {
            return carrierSql;
        }
    }
}
