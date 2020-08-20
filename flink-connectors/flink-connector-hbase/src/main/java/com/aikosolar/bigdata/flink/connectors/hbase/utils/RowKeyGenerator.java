package com.aikosolar.bigdata.flink.connectors.hbase.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * RowKey生成器(爱旭)
 *
 * @author carlc
 */
public class RowKeyGenerator {
    /**
     * 生成符合爱旭要求的rowkey
     */
    public static String gen(String eqpId, String putTime) {
        String rawString = eqpId + "|" + putTime;
        String randomStr = RandomStringUtils.random(5, true, true);
        return DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString + "|" + randomStr;
    }
}
