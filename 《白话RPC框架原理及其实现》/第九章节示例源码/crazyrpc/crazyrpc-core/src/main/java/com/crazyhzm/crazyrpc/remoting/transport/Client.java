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

import com.crazyhzm.crazyrpc.cluster.LoadBalance;
import com.crazyhzm.crazyrpc.common.SpringUtil;
import com.crazyhzm.crazyrpc.config.ClientConfig;
import com.crazyhzm.crazyrpc.config.ReferenceConfig;
import com.crazyhzm.crazyrpc.config.RpcContext;
import com.crazyhzm.crazyrpc.config.ServiceConfig;
import com.crazyhzm.crazyrpc.remoting.Request;
import com.crazyhzm.crazyrpc.remoting.Response;
import com.crazyhzm.crazyrpc.remoting.codec.Decoder;
import com.crazyhzm.crazyrpc.remoting.codec.Encoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: crazyhzm
 * @Date: Created in 2019-12-09 15:47
 */
public class Client {

    private Logger logger = Logger.getLogger(Client.class);

    private ReferenceConfig referenceConfig;

    private ChannelFuture channelFuture;

    private ClientHandler clientHandler;

    public Client(ReferenceConfig referenceConfig) {
        this.referenceConfig = referenceConfig;
    }

    public ServiceConfig connectServer() {
        logger.info("正在连接远程服务端:" + referenceConfig);
        // 客户端线程
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // 解码
                        ch.pipeline().addLast(new Encoder(Request.class));

                        // 编码
                        ch.pipeline().addLast(new Decoder(Response.class));

                        // 收发消息
                        clientHandler = new ClientHandler();

                        // 超时处理类
                        ch.pipeline().addLast(new RpcReadTimeoutHandler(clientHandler, referenceConfig.getTimeout(), TimeUnit.MILLISECONDS));

                        ch.pipeline().addLast(clientHandler);
                    }
                });

        try {
            if (!StringUtils.isEmpty(referenceConfig.getDirectServerIp())) {
                channelFuture = bootstrap.connect(referenceConfig.getDirectServerIp(), referenceConfig.getDirectServerPort()).sync();
                logger.info("点对点服务连接成功");
            } else {
                ClientConfig client = (ClientConfig) SpringUtil.getApplicationContext().getBean("client");
                logger.info("客户端负载均衡策略:" + client.getLoadBalance());

                ServiceConfig serviceConfig = LoadBalance.getService(referenceConfig, client.getLoadBalance());
                channelFuture = bootstrap.connect(serviceConfig.getIp(), serviceConfig.getPort()).sync();
                logger.info("连接远程服务端成功:" + serviceConfig);
                return serviceConfig;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Response remoteCall(Request request) throws Throwable {

        // 发送请求
        channelFuture.channel().writeAndFlush(request).sync();
        channelFuture.channel().closeFuture().sync();

        // 接收响应
        Response response = clientHandler.getResponse();
        logger.info("服务端响应：" + response);

        if (response.getSuccess()) {
            return response;
        }

        throw response.getError();
    }

}
