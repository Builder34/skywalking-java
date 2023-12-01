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

package org.apache.skywalking.apm.plugin.jdbc.mycat.v1.utils;

import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * ContextCarrierHandler
 */
public class ContextCarrierHandler {

    private static final ILog LOGGER = LogManager.getLogger(ContextCarrierHandler.class);
    public final static String TRACE_CARRIER_START_WITH = "/* [sw_trace_carrier:";
    //replace,lastIndexOf no need escape
    public final static String TRACE_CARRIER_END_WITH = "] */";
    //split kwd need escape '*' to '\\*'
    public final static String TRACE_CARRIER_END_WITH_ESCAPE = "] \\*/";

    public static String extract(ContextCarrier contextCarrier, String carrierSql) throws Throwable {
        if (carrierSql.startsWith(TRACE_CARRIER_START_WITH)) {

            String[] carriedAndSql = carrierSql.split(TRACE_CARRIER_END_WITH_ESCAPE);
            String carrier = carriedAndSql[0].replace(TRACE_CARRIER_START_WITH, "");
            LOGGER.info("==> mycat server received contextCarrier is: {}", carrier);
            if (StringUtil.isNotBlank(carrier)) {
                CarrierItem items = contextCarrier.items();
                while (items.hasNext()) {
                    CarrierItem next = items.next();
                    next.setHeadValue(carrier);
                    break;
                }
                ContextManager.extract(contextCarrier);
                LOGGER.info("==> extract result: traceId:{}, segmentId:{}, parentEndpoint: {}, parentService:{}, parentServiceInstance:{}, AddressUsedAtClient:{}, spanId:{}",
                        contextCarrier.getTraceId(), contextCarrier.getTraceSegmentId(), contextCarrier.getParentEndpoint(), contextCarrier.getParentService(), contextCarrier.getParentServiceInstance(),
                        contextCarrier.getAddressUsedAtClient(), contextCarrier.getSpanId());
            }
            LOGGER.info("==> mycat server received originalSql is: {}", carriedAndSql[1]);
            return carriedAndSql[1];
        } else {
            return carrierSql;
        }
    }

    public static String getOriginalSql(String carrierSql) {
        if (carrierSql.startsWith(TRACE_CARRIER_START_WITH) && carrierSql.contains(TRACE_CARRIER_END_WITH)) {
            return carrierSql.split(TRACE_CARRIER_END_WITH_ESCAPE)[1];
        } else {
            return carrierSql;
        }
    }
}
