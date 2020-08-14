package com.aikosolar.bigdata.flink.dynamic.cep;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.cep.pattern.Pattern;


/**
 * Pattern选择器
 *
 * @author carlc
 */
public abstract class DynamicPatternSelector<T> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    /**
     * 选择Pattern
     */
    public abstract Pair<String/*namespace*/, Pattern<T, ?>> select() throws Exception;

    /**
     * 单位:毫秒
     */
    public abstract long getPeriod();
}