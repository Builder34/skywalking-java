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

package org.apache.skywalking.apm.plugin.jdbc.mycat.v1.bean;

import org.apache.skywalking.apm.agent.core.context.ContextSnapshot;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;

public class MycatSpanInfo {
    private String operateName;
    private String sql;
    private ContextSnapshot contextSnapshot;

    private AbstractSpan span;

    public String getOperateName() {
        return operateName;
    }

    public void setOperateName(String operateName) {
        this.operateName = operateName;
    }

    public ContextSnapshot getContextSnapshot() {
        return contextSnapshot;
    }

    public void setContextSnapshot(ContextSnapshot contextSnapshot) {
        this.contextSnapshot = contextSnapshot;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public MycatSpanInfo(String operateName, String sql, ContextSnapshot contextSnapshot) {
        this.operateName = operateName;
        this.sql = sql;
        this.contextSnapshot = contextSnapshot;
    }

    public AbstractSpan getSpan() {
        return span;
    }

    public void setSpan(AbstractSpan span) {
        this.span = span;
    }
}
