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

package com.crazyhzm.crazyrpc.config;

import com.crazyhzm.crazyrpc.common.SpringUtil;
import com.crazyhzm.crazyrpc.registry.ZookeeperClient;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * @Description:
 * @Author: crazyhzm
 * @Date: Created in 2019-12-23 01:17
 */
public class ServiceConfig implements InitializingBean, ApplicationContextAware, Serializable {

    private transient Logger logger = Logger.getLogger(ServiceConfig.class);

    private transient ApplicationContext applicationContext;

    private String id;

    private String name;

    private String impl;

    private String ref;

    private String ip;

    private int port;

    private String version;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!applicationContext.containsBean("server")) {
            logger.info("没有配置server，不发布到注册中心");
            return;
        }
        if (!applicationContext.containsBean("register")) {
            logger.info("没有配置register，不发布到注册中心");
            return;
        }

        // 发布服务到注册中心
        registerService();
    }

    /**
     * 获取spring上下文对象
     *
     * @param applicationContext
     * @throws BeansException
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * 发布服务到注册中心
     *
     * @throws Exception
     */
    private void registerService() throws Exception {
        RegisterConfig register = (RegisterConfig) SpringUtil.getApplicationContext().getBean("register");
        ServerConfig server = (ServerConfig) applicationContext.getBean("server");

        this.setPort(server.getPort());

        // zookeeper
        String basePath = "/crazyrpc/" + this.getName() + "/provider";
        String path = basePath + "/" + InetAddress.getLocalHost().getHostAddress() + "_" + port;

        ZookeeperClient client = ZookeeperClient.getInstance(register.getIp(), register.getPort());
        // 应用（路径）永久保存
        client.createPath(basePath);

        this.setIp(InetAddress.getLocalHost().getHostAddress());
        // 服务(数据)不永久保存，当与zookeeper断开连接20s左右自动删除
        client.saveNode(path, this);
        logger.info("服务发布成功:[" + path + "]");
    }

    @Override
    public String toString() {
        return "Service{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", impl='" + impl + '\'' +
                ", ref='" + ref + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", version=" + version +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getImpl() {
        return impl;
    }

    public void setImpl(String impl) {
        this.impl = impl;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
