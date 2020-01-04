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

import com.crazyhzm.crazyrpc.remoting.Request;
import com.crazyhzm.crazyrpc.remoting.Response;
import com.crazyhzm.crazyrpc.remoting.codec.Decoder;
import com.crazyhzm.crazyrpc.remoting.codec.Encoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

/**
 * @Description:
 * @Author: crazyhzm
 * @Date: Created in 2019-12-09 17:01
 */
public class Server extends Thread{

    private Logger logger = Logger.getLogger(Server.class);

    private Integer port;

    public Server(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        logger.info("RPC服务端正在启动...");
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 解码器
                            ch.pipeline().addLast(new Decoder(Request.class));
                            // 编码器
                            ch.pipeline().addLast(new Encoder(Response.class));
                            // 消息处理类
                            ch.pipeline().addLast(new ServerHandler());
                        }

                    })
                    // backlog大小设置为128
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 连接时测试链接的状态
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口，开始接收进来的链接
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            logger.info("RPC服务端启动完成，监听【" + port + "】端口");

            // 等待服务器关闭
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.error("RPC服务端启动异常，监听【" + port + "】端口", e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
