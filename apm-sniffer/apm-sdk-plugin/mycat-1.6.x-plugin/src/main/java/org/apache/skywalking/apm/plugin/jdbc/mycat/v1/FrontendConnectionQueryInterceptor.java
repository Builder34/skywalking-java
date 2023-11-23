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

import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.jdbc.mycat.v1.utils.ContextCarrierHandler;
import org.apache.skywalking.apm.util.StringUtil;

import java.lang.reflect.Method;

/**
 * FrontendConnectionQueryInterceptor
 */
public class FrontendConnectionQueryInterceptor implements InstanceMethodsAroundInterceptor {

    private static final ILog LOGGER = LogManager.getLogger(FrontendConnectionQueryInterceptor.class);

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, MethodInterceptResult result) throws Throwable {
        if (allArguments[0] instanceof String) {
            String sql = (String) allArguments[0];
            if (StringUtil.isNotBlank(sql) && sql.startsWith(ContextCarrierHandler.TRACE_CARRIER_START_WITH)) {
                ContextCarrier contextCarrier = new ContextCarrier();
                AbstractSpan entrySpan = ContextManager.createEntrySpan("MyCat/JDBI/query", contextCarrier);
                //SpanLayer.asDB(entrySpan);
                entrySpan.setComponent(ComponentsDefine.MYCAT);
                String originalSql = ContextCarrierHandler.extract(contextCarrier, sql);
                //Tags.DB_TYPE.set(entrySpan, ComponentsDefine.MYCAT.getName());
                //Tags.DB_STATEMENT.set(entrySpan, SqlBodyUtil.limitSqlBodySize(originalSql));
            }
        }
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes, Object ret) throws Throwable {
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
