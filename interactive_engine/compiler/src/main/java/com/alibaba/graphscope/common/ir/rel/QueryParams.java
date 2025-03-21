/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

public class QueryParams {
    private Map<String, Object> params;

    public QueryParams() {
        this.params = Maps.newHashMap();
    }

    public QueryParams addParam(String key, Object value) {
        this.params.put(key, value);
        return this;
    }

    public Map<String, Object> getParams() {
        return Collections.unmodifiableMap(this.params);
    }
}
