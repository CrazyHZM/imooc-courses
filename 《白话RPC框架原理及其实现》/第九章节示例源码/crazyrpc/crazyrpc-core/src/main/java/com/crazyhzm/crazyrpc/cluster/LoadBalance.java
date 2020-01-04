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
 */

package com.crazyhzm.crazyrpc.cluster;

import com.crazyhzm.crazyrpc.config.ReferenceConfig;
import com.crazyhzm.crazyrpc.config.ServiceConfig;

import java.util.List;
import java.util.Random;

/**
 * @Description:
 * @Author: crazyhzm
 * @Date: Created in 2019-12-23 20:53
 */
public final class LoadBalance {

    private LoadBalance() {
    }

    /**
     * 根据负载均衡策略获取服务
     *
     * @param refrence
     * @param loadBalance
     * @return
     * @throws Exception
     */
    public static ServiceConfig getService(ReferenceConfig refrence, String loadBalance) throws Exception {
        List<ServiceConfig> services = refrence.getServices();
        if (services.isEmpty()) {
            throw new RuntimeException("没有可用的服务");
        }

        Long count = refrence.getRefCount();
        count++;
        refrence.setRefCount(count);

        if (LoadBalancePolicy.POLL.getName().equals(loadBalance)) {
            // 轮询
            return poll(count, services);
        } else if (LoadBalancePolicy.RANDOM.getName().equals(loadBalance)) {
            // 随机
            return random(services);
        }
        return null;
    }


    /**
     * 随机
     *
     * @param services
     * @return
     */
    private static ServiceConfig random(List<ServiceConfig> services) {
        return services.get(new Random().nextInt(services.size()));
    }

    /**
     * 轮询
     *
     * @param refCount
     * @param services
     * @return
     */
    private static ServiceConfig poll(long refCount, List<ServiceConfig> services) {
        long index = refCount % services.size();
        return services.get((int) index);
    }
}
