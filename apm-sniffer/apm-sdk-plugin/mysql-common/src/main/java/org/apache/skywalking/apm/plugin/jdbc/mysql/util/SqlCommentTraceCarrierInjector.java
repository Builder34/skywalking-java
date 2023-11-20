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
import org.apache.skywalking.apm.util.StringUtil;

/**
 * SqlCommentTraceIdInjector
 */
public class SqlCommentTraceCarrierInjector {

    private static final ILog LOGGER = LogManager.getLogger(SqlCommentTraceCarrierInjector.class);
    public final static String TRACE_CARRIER_START_WITH = "/* [sw_trace_carrier:";
    public final static String TRACE_CARRIER_END_WITH = "] */";

    public static String inject(String sql, String methodName, ContextCarrier contextCarrier, String peer) {
        if (StringUtil.isNotBlank(sql)) {
            if (!sql.startsWith("/*") && !sql.startsWith("SET") && !sql.startsWith("select @@") && !sql.startsWith("SELECT @@")) {
                AbstractSpan span = null;
                if (contextCarrier == null) {
                    contextCarrier = new ContextCarrier();
                    span = ContextManager.createExitSpan("Driver/CarrierInject/" + methodName, contextCarrier, StringUtil.isBlank(peer) ? peer : "unknown");
                    span.setLayer(SpanLayer.DB);
                    Tags.DB_TYPE.set(span, ComponentsDefine.MYCAT.getName());
                    span.setComponent(ComponentsDefine.MYSQL_JDBC_DRIVER);
                }
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
                String injectedSql = TRACE_CARRIER_START_WITH + traceCarrier + TRACE_CARRIER_END_WITH + sql;
                if (span != null) {
                    Tags.DB_STATEMENT.set(span, injectedSql);
                    ContextManager.stopSpan();
                }
                LOGGER.info("==> inject carrier: peer:{}, traceId:{}, parentEndpoint: {}, parentService:{}, parentServiceInstance:{}, AddressUsedAtClient:{}, spanId:{}",
                        peer, contextCarrier.getTraceId(), contextCarrier.getParentEndpoint(), contextCarrier.getParentService(), contextCarrier.getParentServiceInstance(),
                        contextCarrier.getAddressUsedAtClient(), contextCarrier.getSpanId());
                return injectedSql;
            }
        }
        return sql;
    }
}
