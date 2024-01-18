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

package org.apache.skywalking.apm.plugin.jdbc.mycat.v1;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.server.ServerConnection;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.jdbc.SqlBodyUtil;
import org.apache.skywalking.apm.plugin.jdbc.mycat.v1.bean.MycatSpanInfo;
import org.apache.skywalking.apm.plugin.jdbc.mycat.v1.utils.ContextCarrierHandler;
import org.apache.skywalking.apm.util.StringUtil;

import java.lang.reflect.Method;

/**
 * MySQLConnectionExecuteInterceptor
 */
public class MySQLConnectionExecuteInterceptor implements InstanceMethodsAroundInterceptor {

    private static final ILog LOGGER = LogManager.getLogger(MySQLConnectionExecuteInterceptor.class);

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {
        ServerConnection frontendConnection = (ServerConnection) allArguments[1];
        if (StringUtil.isNotBlank(frontendConnection.getExecuteSql()) && frontendConnection.getExecuteSql().startsWith(ContextCarrierHandler.TRACE_CARRIER_START_WITH)) {
            MySQLConnection mysqlConnection = (MySQLConnection) objInst;
            AbstractSpan span = ContextManager.createExitSpan("MyCat/JDBI/" + method.getName(), mysqlConnection.getHost() + ":" + mysqlConnection.getPort());
            Tags.DB_TYPE.set(span, "Mysql");
            Tags.DB_INSTANCE.set(span, mysqlConnection.getSchema());
            Tags.DB_STATEMENT.set(span, SqlBodyUtil.limitSqlBodySize(ContextCarrierHandler.getOriginalSql(frontendConnection.getExecuteSql())));
            //span.setLayer(SpanLayer.DB);
            span.setComponent(ComponentsDefine.MYCAT);
            span.prepareForAsync();
            ContextManager.stopSpan();
            MycatSpanInfo mycatSpanInfo = (MycatSpanInfo) ((EnhancedInstance) frontendConnection).getSkyWalkingDynamicField();
            if (mycatSpanInfo != null) {
                ContextManager.continued(mycatSpanInfo.getContextSnapshot());
            }
            MycatSpanInfo df = new MycatSpanInfo(span.getOperationName(), frontendConnection.getExecuteSql(), ContextManager.capture());
            df.setSpan(span);
            objInst.setSkyWalkingDynamicField(df);
            LOGGER.info("==> objInst:{}, createExitSpan traceId: {}, spanId: {}, executeSql: {}", objInst.toString(), ContextManager.getGlobalTraceId(), ContextManager.getSpanId(), frontendConnection.getExecuteSql());
        } else {
            LOGGER.info("==> traceId: {}, spanId: {}, executeSql: {}", ContextManager.getGlobalTraceId(), ContextManager.getSpanId(), frontendConnection.getExecuteSql());
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret) throws Throwable {
//        ServerConnection frontendConnection = (ServerConnection) allArguments[1];
//        if (StringUtil.isNotBlank(frontendConnection.getExecuteSql()) && frontendConnection.getExecuteSql().startsWith(ContextCarrierHandler.TRACE_CARRIER_START_WITH)) {
//            ContextManager.stopSpan();
//            LOGGER.info("==> stopSpan traceId: {}, spanId: {}, executeSql: {}", ContextManager.getGlobalTraceId(), ContextManager.getSpanId(), frontendConnection.getExecuteSql());
//        } else {
//            LOGGER.info("==> traceId: {}, spanId: {}, executeSql: {}", ContextManager.getGlobalTraceId(), ContextManager.getSpanId(), frontendConnection.getExecuteSql());
//        }
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);
    }

}
