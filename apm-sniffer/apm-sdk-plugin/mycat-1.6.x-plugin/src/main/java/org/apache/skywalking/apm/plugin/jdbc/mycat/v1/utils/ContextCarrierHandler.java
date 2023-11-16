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
    public final static String TRACE_CARRIER_END_WITH = "] */";

    public static void extract(ContextCarrier contextCarrier, String carrierSql) throws Throwable {
        String carrier = carrierSql.substring(0, carrierSql.lastIndexOf(TRACE_CARRIER_END_WITH)).replace(TRACE_CARRIER_START_WITH, "").replace(TRACE_CARRIER_END_WITH, "");
        LOGGER.info("==> mycat server received contextCarrier is: {}", carrier);
        if (StringUtil.isNotBlank(carrier)) {
            CarrierItem items = contextCarrier.items();
            while (items.hasNext()) {
                CarrierItem next = items.next();
                next.setHeadValue(carrier);
                break;
            }
            ContextManager.extract(contextCarrier);
            LOGGER.info("==> extract result: traceId:{}, parentEndpoint: {}, parentService:{}, parentServiceInstance:{}, AddressUsedAtClient:{}, spanId:{}",
                    contextCarrier.getTraceId(), contextCarrier.getParentEndpoint(), contextCarrier.getParentService(), contextCarrier.getParentServiceInstance(),
                    contextCarrier.getAddressUsedAtClient(), contextCarrier.getSpanId());
        }
    }
}
