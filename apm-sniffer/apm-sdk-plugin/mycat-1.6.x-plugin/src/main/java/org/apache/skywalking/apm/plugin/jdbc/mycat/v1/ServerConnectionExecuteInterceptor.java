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
 * FrontendConnectionQueryInterceptor
 */
public class ServerConnectionExecuteInterceptor implements InstanceMethodsAroundInterceptor {

    private static final ILog LOGGER = LogManager.getLogger(ServerConnectionExecuteInterceptor.class);

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {
        if (allArguments[0] instanceof String) {
            String sql = (String) allArguments[0];
            AbstractSpan span = ContextManager.createLocalSpan("MyCat/JDBI/serverExecute");
            //SpanLayer.asDB(entrySpan);
            span.setComponent(ComponentsDefine.MYCAT);
            //Tags.DB_TYPE.set(entrySpan, ComponentsDefine.MYCAT.getName());
            Tags.DB_STATEMENT.set(span, SqlBodyUtil.limitSqlBodySize(ContextCarrierHandler.getOriginalSql(sql)));
            LOGGER.info("==> after extract:, traceId: {}, spanId:{}, ContextManager.getSpanId(): {}", ContextManager.getGlobalTraceId(), span.getSpanId(), ContextManager.getSpanId());
            objInst.setSkyWalkingDynamicField(new MycatSpanInfo(span.getOperationName(), sql, ContextManager.capture()));
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret) throws Throwable {
        LOGGER.info("==> after query method:, traceId: {}, ContextManager.getSpanId(): {}", ContextManager.getGlobalTraceId(), ContextManager.getSpanId());
        if (allArguments[0] instanceof String) {
            String sql = (String) allArguments[0];
            if (StringUtil.isNotBlank(sql) && sql.startsWith(ContextCarrierHandler.TRACE_CARRIER_START_WITH)) {
                ContextManager.stopSpan();
            }
        }
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().log(t);
    }
}
