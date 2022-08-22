/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package cc.kamma;

import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

@State(Scope.Thread)
public class MyBenchmark {


    private ByteBuf compositeByteBuf;
    private OldWayCompression oldWayCompression;
    private NewWayCompression newWayCompression;


    @Setup
    public void setup() {
        String header = "{\"arg\":{" + "\"channel\":\"" + "books" + "\",\"instId\":\"" + "BTC_USDT" + "\"},\"action\":\"snapshot\",\"data\":";
        String data = "{\"asks\":[[\"20522.1\",\"0\",\"0\",\"0\"],[\"20522.4\",\"0\",\"0\",\"0\"],[\"20524.7\",\"0\",\"0\",\"0\"],[\"20526.7\",\"0\",\"0\",\"0\"],[\"20527.5\",\"0\",\"0\",\"0\"],[\"20528.5\",\"0\",\"0\",\"0\"],[\"20528.9\",\"99.98917565\",\"0\",\"1\"],[\"20529.4\",\"177.53260215\",\"0\",\"1\"],[\"20530.1\",\"145.35772765\",\"0\",\"1\"],[\"20530.3\",\"0\",\"0\",\"0\"],[\"20530.7\",\"0\",\"0\",\"0\"],[\"20531.1\",\"147.04375741\",\"0\",\"1\"],[\"20531.2\",\"205.87051678\",\"0\",\"1\"],[\"20532.4\",\"242.40828878\",\"0\",\"1\"],[\"20533.1\",\"0\",\"0\",\"0\"],[\"20533.3\",\"0\",\"0\",\"0\"],[\"20535.5\",\"45.97739283\",\"0\",\"1\"],[\"20535.7\",\"27.78394094\",\"0\",\"1\"],[\"20536.2\",\"25.75780233\",\"0\",\"1\"],[\"20536.5\",\"28.60589743\",\"0\",\"1\"],[\"20537\",\"24.45500126\",\"0\",\"1\"],[\"20537.9\",\"32.03965906\",\"0\",\"1\"],[\"20538.5\",\"32.41683306\",\"0\",\"1\"],[\"20538.7\",\"39.23490519\",\"0\",\"1\"],[\"20538.8\",\"25.27668324\",\"0\",\"1\"],[\"20539.3\",\"40.87718456\",\"0\",\"1\"],[\"20539.9\",\"39.68106811\",\"0\",\"1\"],[\"20832.4\",\"0\",\"0\",\"0\"]],\"bids\":[[\"20527.7\",\"8.74469427\",\"0\",\"1\"],[\"20526.9\",\"37.51071026\",\"0\",\"1\"],[\"20525.7\",\"29.04651703\",\"0\",\"1\"],[\"20524.7\",\"41.11388161\",\"0\",\"1\"],[\"20524.1\",\"45.63367285\",\"0\",\"1\"],[\"20523.7\",\"38.34844377\",\"0\",\"1\"],[\"20523\",\"38.28975685\",\"0\",\"1\"],[\"20522.4\",\"48.51191315\",\"0\",\"1\"],[\"20522.1\",\"45.23597708\",\"0\",\"1\"],[\"20521.8\",\"43.958843\",\"0\",\"1\"],[\"20521.7\",\"0\",\"0\",\"0\"],[\"20521.5\",\"28.82134227\",\"0\",\"1\"],[\"20521.4\",\"0\",\"0\",\"0\"],[\"20520.7\",\"310.6171456\",\"0\",\"1\"],[\"20520.4\",\"0\",\"0\",\"0\"],[\"20520.3\",\"283.64901176\",\"0\",\"1\"],[\"20520.2\",\"359.47988924\",\"0\",\"1\"],[\"20519.6\",\"384.59183875\",\"0\",\"1\"],[\"20519\",\"259.32780759\",\"0\",\"1\"],[\"20518.9\",\"163.13501869\",\"0\",\"1\"],[\"20518.8\",\"290.30291177\",\"0\",\"1\"],[\"20518.7\",\"323.80617477\",\"0\",\"1\"],[\"20518.3\",\"345.92835759\",\"0\",\"1\"],[\"20517.6\",\"100.13804541\",\"0\",\"1\"],[\"20517.2\",\"141.62790696\",\"0\",\"1\"],[\"20516.9\",\"0\",\"0\",\"0\"],[\"20516.7\",\"0\",\"0\",\"0\"],[\"20233.3\",\"0\",\"0\",\"0\"],[\"20233.2\",\"0\",\"0\",\"0\"],[\"20232.4\",\"0\",\"0\",\"0\"],[\"20232.3\",\"0\",\"0\",\"0\"],[\"20232.2\",\"0\",\"0\",\"0\"],[\"20232\",\"0\",\"0\",\"0\"]],\"ts\":\"1657518515970\",\"checksum\":180616684}";
        String tail = "}";


        ByteBuf dataBuf = ByteBufAllocator.DEFAULT.directBuffer(data.length());
        ByteBuf headerBuf = ByteBufAllocator.DEFAULT.directBuffer(header.length());
        ByteBuf end = ByteBufAllocator.DEFAULT.directBuffer(tail.length());

        headerBuf.writeBytes(header.getBytes(StandardCharsets.UTF_8));
        dataBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));
        end.writeBytes(tail.getBytes(StandardCharsets.UTF_8));

        compositeByteBuf = Unpooled.wrappedBuffer(headerBuf, dataBuf, end);

        oldWayCompression = new OldWayCompression();
        newWayCompression = new NewWayCompression();
    }

    @Benchmark
    public void oldWayCompression() throws Exception {
        oldWayCompression.compression(compositeByteBuf.duplicate());
    }

    @Benchmark
    public void newWayCompression() throws Exception {
        newWayCompression.compression(compositeByteBuf.duplicate());
    }

    class OldWayCompression extends JdkZlibEncoder {


        OldWayCompression() {
            super(1);

        }
        void compression(ByteBuf byteBuf) throws Exception {
            FakeCtx ctx = new FakeCtx();
            ByteBuf out = this.allocateBuffer(ctx, byteBuf, true);
            encode(ctx, byteBuf, out);
            out.release();
        }
    }

    class NewWayCompression extends BetterJdkZlibEncoder {


        NewWayCompression() {
            super(1);

        }
        void compression(ByteBuf byteBuf) throws Exception {
            FakeCtx ctx = new FakeCtx();
            ByteBuf out = this.allocateBuffer(ctx, byteBuf, true);
            encode(ctx, byteBuf, out);
            out.release();
        }
    }

    class FakeCtx implements ChannelHandlerContext {

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public EventExecutor executor() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public ChannelHandler handler() {
            return null;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object evt) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object msg) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelHandlerContext read() {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg) {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelHandlerContext flush() {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return ByteBufAllocator.DEFAULT;
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return null;
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }
    }

}
