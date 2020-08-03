package com.aikosolar.bigdata.flink.common.utils;

/**
 * @author carlc
 */
public class IOUtils {
    public static void closeQuietly(AutoCloseable r) {
        try {
            r.close();
        } catch (Exception e) {
        }
    }
}