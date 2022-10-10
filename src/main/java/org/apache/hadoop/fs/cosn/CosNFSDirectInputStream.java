package org.apache.hadoop.fs.cosn;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.*;

public class CosNFSDirectInputStream extends FSInputStream{
    public static final Logger LOG =
            LoggerFactory.getLogger(CosNFSDirectInputStream.class);

    // read buffer direct used the CosNFSInputStream

    private FileSystem.Statistics statistics;
    private final Configuration conf;
    private final NativeFileSystemStore store;
    private final String key;
    private long position = 0;
    private long nextPos = 0;
    private long lastByteStart = -1;
    private long partRemaining;
    private long fileSize;
    private long bufferStart;
    private long bufferEnd;
    private byte[] buffer;
    private boolean closed = false;
    private final int socketErrMaxRetryTimes;
    // last read cache
    com.google.common.cache.Cache<Pair<Long, Long>, byte[]> page_cache;
    private static final long READ_CACHE_SIZE = 16 * Unit.KB;
    private static final long READ_AHEAD_BASE = 128 * Unit.KB;
    private static final long READ_AHEAD_MAX = 4 * Unit.MB;
    private long readAheadBonus = READ_AHEAD_BASE;
    private long lastReadEnd = -1;
    private String clientId;

    private final ExecutorService readAheadExecutorService;
    private boolean logIODetail;

    /**
     * Input Stream
     *
     * @param conf config
     * @param store native file system
     * @param statistics statis
     * @param key cos key
     * @param fileSize file size
     * @param readAheadExecutorService thread executor
     */
    public CosNFSDirectInputStream(
            Configuration conf,
            String clientId,
            NativeFileSystemStore store,
            FileSystem.Statistics statistics,
            String key,
            long fileSize,
            ExecutorService readAheadExecutorService,
            com.google.common.cache.Cache<Pair<Long, Long>, byte[]> page_cache) {
        super();
        this.conf = conf;
        this.clientId = clientId;
        this.store = store;
        this.statistics = statistics;
        this.key = key;
        this.fileSize = fileSize;
        this.bufferStart = -1;
        this.bufferEnd = -1;

        this.socketErrMaxRetryTimes = conf.getInt(
                CosNConfigKeys.CLIENT_SOCKET_ERROR_MAX_RETRIES,
                CosNConfigKeys.DEFAULT_CLIENT_SOCKET_ERROR_MAX_RETRIES);
        this.readAheadExecutorService = readAheadExecutorService;
        this.closed = false;
        this.page_cache = page_cache;
        this.logIODetail =conf.getBoolean(
                CosNConfigKeys.COSN_LOG_IO_DETAIL,
                CosNConfigKeys.DEFAULT_COSN_LOG_IO_DETAIL);
    }

    private synchronized void reopen(long pos) throws IOException {
    }

    private void logIf(String s, Object... objects) {
        if(this.logIODetail) {
            LOG.info(s, objects);
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        this.checkOpened();

        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (pos > this.fileSize) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }

        if (this.position == pos) {
            return;
        }

        this.position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        byte[] oneByteBuff = new byte[1];
        int ret = read(oneByteBuff, 0, 1);

        if (this.statistics != null) {
            this.statistics.incrementBytesRead(1);
        }

        return (ret <= 0) ? -1 : (oneByteBuff[0] & 0xff);
    }

    private int readFromCache(byte[] b, int off, int len) {
        // locate range in 16k cache
        long byteStart = this.position;
        long byteEnd = this.position + len - 1;
        if (byteEnd >= this.fileSize) {
            byteEnd = this.fileSize - 1;
        }
        Pair<Long, Long> range = Pair.<Long, Long>of(byteStart, byteEnd);
        byte[] last_read_cache = this.page_cache.getIfPresent(range);

        // cache miss
        if(last_read_cache == null) {
            return 0;
        } else {
            assert len == last_read_cache.length;
            // abandon old buffer because cache hit always means random read
            this.buffer = last_read_cache;
        }

        // cache hit
        logIf("ClientId: {}, manual page cache hit, key: {}, offset: {}, len: {}, range start: {}, range end: {}",
                this.clientId, this.key, off, len, byteStart, byteEnd);

        int bytesRead = 0;
        for (int i = 0; i < this.buffer.length; i++) {
            b[off + bytesRead] = this.buffer[i];
            bytesRead++;
            if (bytesRead >= len) {
                break;
            }
        }

        this.position += bytesRead;
        this.lastReadEnd = this.position;

        return bytesRead;
    }

    private boolean shouldReadAhead(long byteStart, long byteEnd) {
        // we could locate this read in cache if we read ahead using current bonus
        return byteStart >= this.lastReadEnd && byteEnd <= this.lastReadEnd + this.readAheadBonus;
    }

    private int readFromBuffer(byte[] b, int off, int len) throws IOException {
        // locate range in read ahead cache
        long byteStart = this.position;
        long byteEnd = this.position + len - 1;
        if (byteEnd >= this.fileSize) {
            byteEnd = this.fileSize - 1;
        }

        // read ahead cache miss, get data from cos
        if(byteStart < bufferStart || byteEnd > bufferEnd) {
            if (!shouldReadAhead(byteStart, byteEnd)) {
                this.readAheadBonus = READ_AHEAD_BASE;
            }
            long readStart = byteStart;
            long readEnd = this.position + Math.max(len, this.readAheadBonus) - 1;
            if (readEnd >= this.fileSize) {
                readEnd = this.fileSize - 1;
            }
            CosNFSInputStream.ReadBuffer readBuffer = new CosNFSInputStream.ReadBuffer(readStart, readEnd);
            if (readBuffer.getBuffer().length == 0) {
                readBuffer.setStatus(CosNFSInputStream.ReadBuffer.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(
                        new CosNFileReadTask(this.conf, this.clientId, this.key, this.store,
                                readBuffer, this.socketErrMaxRetryTimes));
            }

            IOException innerException = null;

            logIf("ClientId: {}, push read object task: {}, offset: {}, len: {}, range start: {}, range end: {}",
                    this.clientId, this.key, off, len, readBuffer.getStart(), readBuffer.getEnd());
            readBuffer.lock();
            //LOG.info("get lock of read object task [{}], offset [{}], len [{}]", this.key, off, len);
            try {
                readBuffer.await(CosNFSInputStream.ReadBuffer.INIT);
                logIf("ClientId: {}, after read object task: {}, offset: {}, len: {}, range start: {}, range end: {}",
                        this.clientId, this.key, off, len, readBuffer.getStart(), readBuffer.getEnd());
                if (readBuffer.getStatus() == CosNFSInputStream.ReadBuffer.ERROR) {
                    innerException = readBuffer.getException();
                    this.buffer = null;
                    this.bufferStart = -1;
                    this.bufferEnd = -1;
                } else {
                    // abandon old buffer because we need this one
                    this.buffer = readBuffer.getBuffer();
                    this.bufferStart = readBuffer.getStart();
                    this.bufferEnd = readBuffer.getEnd();
                }

            } catch (InterruptedException e) {
                LOG.warn("interrupted exception occurs when wait a read buffer.");
            } finally {
                readBuffer.unLock();
            }

            if (null == this.buffer) {
                LOG.error(String.format("Null IO stream key:%s", this.key), innerException);
                throw new IOException("Null IO stream.", innerException);
            }
        } else {
            this.readAheadBonus = Math.min(this.readAheadBonus * 2, READ_AHEAD_MAX);
            logIf("ClientId: {}, read ahead cache hit, key: {}, offset: {}, len: {}, range start: {}, range end: {}, bonus: {}",
                    this.clientId, this.key, off, len, byteStart, byteEnd, this.readAheadBonus);
        }

        int bytesRead = 0;
        for (int i = (int)(this.position - this.bufferStart); i < this.buffer.length; i++) {
            b[off + bytesRead] = this.buffer[i];
            bytesRead++;
            if (bytesRead >= len) {
                break;
            }
        }

        // cache every random read less than 16k
        if (len <= READ_CACHE_SIZE && this.position == this.bufferStart) {
            byte[] cache_data = new byte[bytesRead];
            System.arraycopy(b, off, cache_data, 0, bytesRead);
            Pair<Long, Long> range = Pair.<Long, Long>of(byteStart, byteEnd);
            this.page_cache.put(range, cache_data);
        }

        this.position += bytesRead;
        this.lastReadEnd = this.position;

        return bytesRead;
    }
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        this.checkOpened();
        //LOG.info("Begin to read object [{}], offset [{}], len [{}], pos [{}]", this.key, off, len, this.position);
        long start = System.currentTimeMillis();
        if (len == 0) {
            return 0;
        }

        if (off < 0 || len < 0 || len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        int bytesRead = readFromCache(b, off, len);
        if (bytesRead == 0) {
            bytesRead = readFromBuffer(b, off, len);
        }

        if (null != this.statistics && bytesRead > 0) {
            this.statistics.incrementBytesRead(bytesRead);
        }

        long costMs = (System.currentTimeMillis() - start);
        long byteStart = this.position - bytesRead;
        long byteEnd = this.position + len - 1 - bytesRead;
        if (byteEnd >= this.fileSize) {
            byteEnd = this.fileSize - 1;
        }

        logIf("ClientId: {}, read object: {}, offset: {}, len: {}, costMs: {}, range start: {}, range end: {}",
                this.clientId, this.key, off, len, costMs, byteStart, byteEnd);

        return bytesRead == 0 ? -1 : bytesRead;
    }


    @Override
    public int available() throws IOException {
        this.checkOpened();

        long remaining = this.fileSize - this.position;
        if(remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) remaining;
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.buffer = null;
    }

    private void checkOpened() throws IOException {
        if(this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }
}
