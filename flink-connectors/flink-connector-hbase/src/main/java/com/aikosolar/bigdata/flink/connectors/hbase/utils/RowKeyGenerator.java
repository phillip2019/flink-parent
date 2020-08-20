package com.aikosolar.bigdata.flink.connectors.hbase.utils;

import com.google.common.base.Joiner;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.function.Function;

/**
 * RowKey生成器(爱旭)
 *
 * @author carlc
 */
public class RowKeyGenerator {

    public static final Function<String, String> defaultAppendFun = x -> x + "|" + RandomStringUtils.random(5, true, true);

    /**
     * 生成符合爱旭要求的rowkey(上层应用保证items每项不得为null)
     */
    public static String gen(String... items) {
        return gen(defaultAppendFun, items);
    }

    public static String gen(Function<String, String> appendFun, String... items) {
        String rawString = Joiner.on("|").join(items);
        String rowKey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString;
        if (appendFun != null) {
            rowKey = appendFun.apply(rowKey);
        }
        return rowKey;
    }
}