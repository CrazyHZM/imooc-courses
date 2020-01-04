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

package com.crazyhzm.crazyrpc.remoting.transport;

import com.crazyhzm.crazyrpc.common.SpringUtil;
import com.crazyhzm.crazyrpc.common.TypeParseUtil;
import com.crazyhzm.crazyrpc.config.ServiceConfig;
import com.crazyhzm.crazyrpc.remoting.Request;
import com.crazyhzm.crazyrpc.remoting.Response;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * @Description:
 * @Author: crazyhzm
 * @Date: Created in 2019-12-17 09:50
 */
public class ServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger = Logger.getLogger(ServerHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Response response = new Response();
        try {
            Request request = (Request) msg;
            logger.info("服务端收到的消息:" + request);
            // 获取本地暴露的所有服务
            Map<String, ServiceConfig> serviceMap = SpringUtil.getApplicationContext().getBeansOfType(ServiceConfig.class);
            ServiceConfig service = null;
            // 匹配客户端请求的服务
            for (String key : serviceMap.keySet()) {
                if (serviceMap.get(key).getName().equals(request.getClassName())) {
                    service = serviceMap.get(key);
                    break;
                }
            }
            if (service == null) {
                throw new RuntimeException("没有找到服务:" + request.getClassName());
            }
            // 获取服务的实现类
            Object serviceImpl = SpringUtil.getApplicationContext().getBean(service.getRef());
            if (serviceImpl == null) {
                throw new RuntimeException("没有找到服务:" + request.getClassName());
            }

            // 转换参数和参数类型
            Map<String, Object> map = TypeParseUtil.parseTypeString2Class(request.getTypes(), request.getArgs());
            Class<?>[] classTypes = (Class<?>[]) map.get("classTypes");
            Object[] args = (Object[]) map.get("args");

            // 通过反射调用方法获取返回值
            Object result = serviceImpl.getClass().getMethod(request.getMethodName(), classTypes).invoke(serviceImpl, args);
            response.setResult(result);
            response.setSuccess(true);
        } catch (Throwable e) {
            logger.error("服务端接收消息发送异常", e);
            response.setSuccess(false);
            response.setError(e);
        }

        // 写响应
        logger.info("服务端响应内容:" + response);
        ctx.write(response);
        ctx.flush();
    }
}
