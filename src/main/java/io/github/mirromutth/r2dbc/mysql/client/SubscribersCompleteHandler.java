/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.mirromutth.r2dbc.mysql.client;

import io.github.mirromutth.r2dbc.mysql.message.backend.BackendMessage;
import io.github.mirromutth.r2dbc.mysql.message.frontend.FrontendMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;

import java.util.Queue;

/**
 * A handler for ensure subscribers complete.
 */
final class SubscribersCompleteHandler extends ChannelDuplexHandler {

    private final EmitterProcessor<FrontendMessage> requestProcessor;

    private final Queue<ResponseReceiver> responseReceivers;

    SubscribersCompleteHandler(
        EmitterProcessor<FrontendMessage> requestProcessor,
        Queue<ResponseReceiver> responseReceivers
    ) {
        this.requestProcessor = requestProcessor;
        this.responseReceivers = responseReceivers;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);

        this.requestProcessor.onComplete();

        for (ResponseReceiver responseReceiver = responseReceivers.poll(); responseReceiver != null; responseReceiver = responseReceivers.poll()) {
            responseReceiver.success(Flux.empty());
        }
    }
}
