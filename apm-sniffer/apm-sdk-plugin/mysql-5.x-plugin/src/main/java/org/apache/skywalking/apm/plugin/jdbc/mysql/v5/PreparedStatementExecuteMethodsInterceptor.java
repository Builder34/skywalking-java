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

package org.apache.skywalking.apm.plugin.jdbc.mysql.v5;

import com.mysql.jdbc.SingleByteCharsetConverter;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.JDBC42PreparedStatement;
import com.mysql.jdbc.JDBC4PreparedStatement;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.DatabaseMetaData;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.plugin.jdbc.JDBCPluginConfig;
import org.apache.skywalking.apm.plugin.jdbc.PreparedStatementParameterBuilder;
import org.apache.skywalking.apm.plugin.jdbc.SqlBodyUtil;
import org.apache.skywalking.apm.plugin.jdbc.define.StatementEnhanceInfos;
import org.apache.skywalking.apm.plugin.jdbc.mysql.util.SqlCommentTraceCarrierInjector;
import org.apache.skywalking.apm.plugin.jdbc.trace.ConnectionInfo;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

public class PreparedStatementExecuteMethodsInterceptor implements InstanceMethodsAroundInterceptor {

    private static final ILog LOGGER = LogManager.getLogger(PreparedStatementExecuteMethodsInterceptor.class);

    @Override
    public final void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                                   Class<?>[] argumentsTypes, MethodInterceptResult result) {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        ContextCarrier contextCarrier = new ContextCarrier();

        /**
         * For avoid NPE. In this particular case, Execute sql inside the {@link com.mysql.jdbc.ConnectionImpl} constructor,
         * before the interceptor sets the connectionInfo.
         * When invoking prepareCall, cacheObject is null. Because it will determine procedures's parameter types by executing sql in mysql
         * before the interceptor sets the statementEnhanceInfos.
         * @see JDBCDriverInterceptor#afterMethod(EnhancedInstance, Method, Object[], Class[], Object)
         */
        if (cacheObject != null && cacheObject.getConnectionInfo() != null) {
            ConnectionInfo connectInfo = cacheObject.getConnectionInfo();
            AbstractSpan span = ContextManager.createExitSpan(
                    buildOperationName(connectInfo, method.getName(), cacheObject
                            .getStatementName()), contextCarrier, connectInfo.getDatabasePeer());
            Tags.DB_TYPE.set(span, connectInfo.getDBType());
            Tags.DB_INSTANCE.set(span, connectInfo.getDatabaseName());
            Tags.DB_STATEMENT.set(span, SqlBodyUtil.limitSqlBodySize(cacheObject.getSql()));
            span.setComponent(connectInfo.getComponent());
            if (JDBCPluginConfig.Plugin.JDBC.TRACE_SQL_PARAMETERS) {
                final Object[] parameters = cacheObject.getParameters();
                if (parameters != null && parameters.length > 0) {
                    int maxIndex = cacheObject.getMaxIndex();
                    String parameterString = getParameterString(parameters, maxIndex);
                    Tags.SQL_PARAMETERS.set(span, parameterString);
                }
            }
            SpanLayer.asDB(span);
            String injectedSql = SqlCommentTraceCarrierInjector.inject(cacheObject.getSql(), objInst.getClass().getName(), method.getName(), contextCarrier, connectInfo);
            //change original sql to add carrier info in sql body with comment
            try {
                LOGGER.info("==> objInst class name: {}", objInst.getClass().getName());
                Class<?> preparedStatementClass = getPreparedStatementClass(objInst);
                Field originalSqlField = preparedStatementClass.getDeclaredField("originalSql");
                originalSqlField.setAccessible(true);
                originalSqlField.set(objInst, injectedSql);
                PreparedStatement.ParseInfo parseInfo = getParseInfo(objInst, preparedStatementClass, injectedSql);
                Field parseInfoField = preparedStatementClass.getDeclaredField("parseInfo");
                parseInfoField.setAccessible(true);
                parseInfoField.set(objInst, parseInfo);
                Method initializeFromParseInfoMethod = preparedStatementClass.getDeclaredMethod("initializeFromParseInfo");
                initializeFromParseInfoMethod.setAccessible(true);
                //initializeFromParseInfoMethod.invoke(objInst);
                LOGGER.info("==> after reflect preparedStatement sql: {}", ((PreparedStatement) objInst).getPreparedSql());
            } catch (Exception e) {
                LOGGER.error(e, "reflect preparedStatement originalSql to set carrierInject sql failed: {}", e.getMessage());
            }
        }
    }

    private static PreparedStatement.ParseInfo getParseInfo(EnhancedInstance objInst, Class<?> preparedStatementClass, String injectedSql) throws NoSuchFieldException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Field dbmdField = preparedStatementClass.getDeclaredField("dbmd");
        Class<?> statementImplClassSuperclass = preparedStatementClass.getSuperclass();
        Field connectionField = statementImplClassSuperclass.getDeclaredField("connection");
        Field charEncodingField = statementImplClassSuperclass.getDeclaredField("charEncoding");
        Field charConverterField = statementImplClassSuperclass.getDeclaredField("charConverter");
        connectionField.setAccessible(true);
        dbmdField.setAccessible(true);
        charEncodingField.setAccessible(true);
        charConverterField.setAccessible(true);
        MySQLConnection connection = (MySQLConnection) connectionField.get(objInst);
        DatabaseMetaData dbmd = (DatabaseMetaData) dbmdField.get(objInst);
        String charEncoding = (String) charEncodingField.get(objInst);
        SingleByteCharsetConverter charConverter = (SingleByteCharsetConverter) charConverterField.get(objInst);
        return new PreparedStatement.ParseInfo(injectedSql, connection, dbmd, charEncoding, charConverter, true);
    }

    private static Class<?> getPreparedStatementClass(EnhancedInstance objInst) {
        Class<?> preparedStatementClass;
        boolean hasJDBC42PreparedStatement = false;
        try {
            if (objInst instanceof JDBC42PreparedStatement) {
                hasJDBC42PreparedStatement = true;
            }
        } catch (NoClassDefFoundError error) {
            //do not thing.
        }
        if (hasJDBC42PreparedStatement) {
            JDBC42PreparedStatement preparedStatement = (JDBC42PreparedStatement) objInst;
            preparedStatementClass = preparedStatement.getClass().getSuperclass().getSuperclass();
        } else if (objInst instanceof JDBC4PreparedStatement) {
            JDBC4PreparedStatement preparedStatement = (JDBC4PreparedStatement) objInst;
            preparedStatementClass = preparedStatement.getClass().getSuperclass();
        } else {
            PreparedStatement preparedStatement = (PreparedStatement) objInst;
            preparedStatementClass = preparedStatement.getClass();
        }
        return preparedStatementClass;
    }

    @Override
    public final Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments,
                                    Class<?>[] argumentsTypes, Object ret) {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        if (cacheObject != null && cacheObject.getConnectionInfo() != null) {
            ContextManager.stopSpan();
        }
        LOGGER.info("==> preparedStatement execute afterMethod sql: {}", ((PreparedStatement) objInst).getPreparedSql());
        return ret;
    }

    @Override
    public final void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                            Class<?>[] argumentsTypes, Throwable t) {
        StatementEnhanceInfos cacheObject = (StatementEnhanceInfos) objInst.getSkyWalkingDynamicField();
        if (cacheObject != null && cacheObject.getConnectionInfo() != null) {
            ContextManager.activeSpan().log(t);
        }
    }

    private String buildOperationName(ConnectionInfo connectionInfo, String methodName, String statementName) {
        return connectionInfo.getDBType() + "/JDBI/" + statementName + "/" + methodName;
    }

    private String getParameterString(Object[] parameters, int maxIndex) {
        return new PreparedStatementParameterBuilder()
                .setParameters(parameters)
                .setMaxIndex(maxIndex)
                .build();
    }
}
