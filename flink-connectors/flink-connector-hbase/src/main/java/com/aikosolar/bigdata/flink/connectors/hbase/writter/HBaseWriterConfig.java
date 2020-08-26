package com.aikosolar.bigdata.flink.connectors.hbase.writter;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author carlc
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HBaseWriterConfig implements Serializable {
    private int writeBufferSize = -1;
    private int asyncFlushSize = -1;
    private long asyncFlushInterval = -1;
    private boolean async;
    private String durability;
    private Map<String, String> hbaseConfig = new HashMap<>();

    public static class Builder {
        private int writeBufferSize = 5 * 1024 * 1024;
        private int asyncFlushSize = 5000;
        // 单位:秒
        private long asyncFlushInterval = 60;
        private boolean async;
        private String durability;
        private Map<String, String> config = new HashMap<>();

        public static synchronized Builder me() {
            return new Builder();
        }

        public Builder setDurability(String durability) {
            this.durability = durability;
            return this;
        }

        public Builder writeBufferSize(int writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder conf(String key, String value) {
            this.config.put(key, value);
            return this;
        }

        public Builder conf(Map<String, String> config) {
            if (MapUtils.isNotEmpty(config)) {
                for (Map.Entry<String, String> e : config.entrySet()) {
                    this.config.put(e.getKey(), e.getValue());
                }
            }
            return this;
        }

        public Builder aync() {
            this.async = true;
            return this;
        }

        public Builder flushInterval(long interval) {
            this.asyncFlushInterval = interval;
            return this;
        }

        public Builder flushSize(int size) {
            this.asyncFlushSize = size;
            return this;
        }

        public Builder async(boolean enable) {
            this.async = enable;
            return this;
        }

        public HBaseWriterConfig build() {
            return new HBaseWriterConfig(writeBufferSize, asyncFlushSize, asyncFlushInterval, async, durability, config);
        }

    }
}
