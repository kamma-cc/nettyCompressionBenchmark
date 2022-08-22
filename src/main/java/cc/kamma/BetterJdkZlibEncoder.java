package cc.kamma;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.compression.CompressionException;
import io.netty.handler.codec.compression.ZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.PromiseNotifier;
import io.netty.util.internal.ObjectUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * replaced {@link io.netty.handler.codec.compression.JdkZlibEncoder}
 * remove the copying of off-heap to heap array
 */
public class BetterJdkZlibEncoder extends ZlibEncoder {
    private final ZlibWrapper wrapper;
    private final Deflater deflater;
    private volatile boolean finished;
    private volatile ChannelHandlerContext ctx;

    /*
     * GZIP support
     */
    private final CRC32 crc = new CRC32();
    private static final byte[] gzipHeader = {0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
    private boolean writeHeader = true;
    private static final int THREAD_POOL_DELAY_SECONDS = 10;

    /**
     * Creates a new zlib encoder with the default compression level ({@code 6})
     * and the default wrapper ({@link ZlibWrapper#ZLIB}).
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder() {
        this(6);
    }

    /**
     * Creates a new zlib encoder with the specified {@code compressionLevel}
     * and the default wrapper ({@link ZlibWrapper#ZLIB}).
     *
     * @param compressionLevel
     *        {@code 1} yields the fastest compression and {@code 9} yields the
     *        best compression.  {@code 0} means no compression.  The default
     *        compression level is {@code 6}.
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder(int compressionLevel) {
        this(ZlibWrapper.ZLIB, compressionLevel);
    }

    /**
     * Creates a new zlib encoder with the default compression level ({@code 6})
     * and the specified wrapper.
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder(ZlibWrapper wrapper) {
        this(wrapper, 6);
    }

    /**
     * Creates a new zlib encoder with the specified {@code compressionLevel}
     * and the specified wrapper.
     *
     * @param compressionLevel
     *        {@code 1} yields the fastest compression and {@code 9} yields the
     *        best compression.  {@code 0} means no compression.  The default
     *        compression level is {@code 6}.
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder(ZlibWrapper wrapper, int compressionLevel) {
        ObjectUtil.checkInRange(compressionLevel, 0, 9, "compressionLevel");
        ObjectUtil.checkNotNull(wrapper, "wrapper");

        if (wrapper == ZlibWrapper.ZLIB_OR_NONE) {
            throw new IllegalArgumentException(
                    "wrapper '" + ZlibWrapper.ZLIB_OR_NONE + "' is not " +
                            "allowed for compression.");
        }

        this.wrapper = wrapper;
        deflater = new Deflater(compressionLevel, wrapper != ZlibWrapper.ZLIB);
    }

    /**
     * Creates a new zlib encoder with the default compression level ({@code 6})
     * and the specified preset dictionary.  The wrapper is always
     * {@link ZlibWrapper#ZLIB} because it is the only format that supports
     * the preset dictionary.
     *
     * @param dictionary  the preset dictionary
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder(byte[] dictionary) {
        this(6, dictionary);
    }

    /**
     * Creates a new zlib encoder with the specified {@code compressionLevel}
     * and the specified preset dictionary.  The wrapper is always
     * {@link ZlibWrapper#ZLIB} because it is the only format that supports
     * the preset dictionary.
     *
     * @param compressionLevel
     *        {@code 1} yields the fastest compression and {@code 9} yields the
     *        best compression.  {@code 0} means no compression.  The default
     *        compression level is {@code 6}.
     * @param dictionary  the preset dictionary
     *
     * @throws CompressionException if failed to initialize zlib
     */
    public BetterJdkZlibEncoder(int compressionLevel, byte[] dictionary) {
        ObjectUtil.checkInRange(compressionLevel, 0, 9, "compressionLevel");
        ObjectUtil.checkNotNull(dictionary, "dictionary");

        wrapper = ZlibWrapper.ZLIB;
        deflater = new Deflater(compressionLevel);
        deflater.setDictionary(dictionary);
    }

    @Override
    public ChannelFuture close() {
        return close(ctx().newPromise());
    }

    @Override
    public ChannelFuture close(final ChannelPromise promise) {
        ChannelHandlerContext ctx = ctx();
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            return finishEncode(ctx, promise);
        } else {
            final ChannelPromise p = ctx.newPromise();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    ChannelFuture f = finishEncode(ctx(), p);
                    PromiseNotifier.cascade(f, promise);
                }
            });
            return p;
        }
    }

    private ChannelHandlerContext ctx() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            throw new IllegalStateException("not added to a pipeline");
        }
        return ctx;
    }

    @Override
    public boolean isClosed() {
        return finished;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf uncompressed, ByteBuf out) throws Exception {
        if (finished) {
            out.writeBytes(uncompressed);
            return;
        }

        int len = uncompressed.readableBytes();
        if (len == 0) {
            return;
        }

        if (uncompressed.nioBufferCount() == 1) {
            ByteBuffer in = uncompressed.internalNioBuffer(uncompressed.readerIndex(), len);
            compressData(in, out);
        } else {
            ByteBuffer[] ins = uncompressed.nioBuffers(uncompressed.readerIndex(), len);
            for (ByteBuffer in: ins) {
                compressData(in, out);
            }
        }
//        deflate(out, true);
    }

    private void compressData(ByteBuffer in, ByteBuf out) {
        try {
            if (writeHeader) {
                writeHeader = false;
                if (wrapper == ZlibWrapper.GZIP) {
                    out.writeBytes(gzipHeader);
                }
            }

            if (wrapper == ZlibWrapper.GZIP) {
                int position = in.position();
                crc.update(in);
                in.position(position);
            }

            deflater.setInput(in);
            for (;;) {
                deflate(out, true);
                if (deflater.needsInput()) {
                    // Consumed everything
                    break;
                } else {
                    if (!out.isWritable()) {
                        // We did not consume everything but the buffer is not writable anymore. Increase the
                        // capacity to make more room.
                        out.ensureWritable(out.writerIndex());
                    }
                }
            }
        } catch (Throwable cause) {
            out.release();
            throw cause;
        }
    }

    @Override
    protected final ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg,
                                           boolean preferDirect) throws Exception {
        int sizeEstimate = (int) Math.ceil(msg.readableBytes() * 1.001) + 12;
        if (writeHeader) {
            switch (wrapper) {
                case GZIP:
                    sizeEstimate += gzipHeader.length;
                    break;
                case ZLIB:
                    sizeEstimate += 2; // first two magic bytes
                    break;
                default:
                    // no op
            }
        }
        return ctx.alloc().directBuffer(sizeEstimate);
    }

    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
        ChannelFuture f = finishEncode(ctx, ctx.newPromise());
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                ctx.close(promise);
            }
        });

        if (!f.isDone()) {
            // Ensure the channel is closed even if the write operation completes in time.
            ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    ctx.close(promise);
                }
            }, THREAD_POOL_DELAY_SECONDS, TimeUnit.SECONDS);
        }
    }

    private ChannelFuture finishEncode(final ChannelHandlerContext ctx, ChannelPromise promise) {
        if (finished) {
            promise.setSuccess();
            return promise;
        }

        finished = true;
        ByteBuf footer = ctx.alloc().directBuffer();
        if (writeHeader && wrapper == ZlibWrapper.GZIP) {
            // Write the GZIP header first if not written yet. (i.e. user wrote nothing.)
            writeHeader = false;
            footer.writeBytes(gzipHeader);
        }

        deflater.finish();

        while (!deflater.finished()) {
            deflate(footer, true);
            if (!footer.isWritable()) {
                // no more space so write it to the channel and continue
                ctx.write(footer);
                footer = ctx.alloc().directBuffer();
            }
        }
        if (wrapper == ZlibWrapper.GZIP) {
            int crcValue = (int) crc.getValue();
            int uncBytes = deflater.getTotalIn();
            footer.writeByte(crcValue);
            footer.writeByte(crcValue >>> 8);
            footer.writeByte(crcValue >>> 16);
            footer.writeByte(crcValue >>> 24);
            footer.writeByte(uncBytes);
            footer.writeByte(uncBytes >>> 8);
            footer.writeByte(uncBytes >>> 16);
            footer.writeByte(uncBytes >>> 24);
        }
        deflater.end();
        return ctx.writeAndFlush(footer, promise);
    }

    private void deflate(ByteBuf out, boolean last) {
        final int flush = last ? Deflater.SYNC_FLUSH : Deflater.NO_FLUSH;
        if (out.hasArray()) {
            int numBytes;
            do {
                int writerIndex = out.writerIndex();
                numBytes = deflater.deflate(
                        out.array(), out.arrayOffset() + writerIndex, out.writableBytes(), flush);
                out.writerIndex(writerIndex + numBytes);
            } while (numBytes > 0);
        } else if (out.nioBufferCount() == 1) {
            // Use internalNioBuffer because nioBuffer is allowed to copy,
            // which is fine for reading but not for writing.
            int numBytes;
            do {
                int writerIndex = out.writerIndex();
                ByteBuffer buffer = out.internalNioBuffer(writerIndex, out.writableBytes());
                numBytes = deflater.deflate(buffer, flush);
                out.writerIndex(writerIndex + numBytes);
            } while (numBytes > 0);
        } else {
            throw new IllegalArgumentException(
                    "Don't know how to deflate buffer without array or NIO buffer count of 1: " + out);
        }
    }

    private void deflateJdk6(ByteBuf out) {
        int numBytes;
        do {
            int writerIndex = out.writerIndex();
            numBytes = deflater.deflate(
                    out.array(), out.arrayOffset() + writerIndex, out.writableBytes());
            out.writerIndex(writerIndex + numBytes);
        } while (numBytes > 0);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }
}