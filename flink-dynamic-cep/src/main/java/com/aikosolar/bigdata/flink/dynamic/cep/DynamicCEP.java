package com.aikosolar.bigdata.flink.dynamic.cep;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 动态CEP
 *
 * @author carlc
 */
public class DynamicCEP extends CEP {

    public static <T> PatternStream<T> dynamicPattern(DataStream<T> input, DynamicPatternSelector<T> selector) {
        Pattern<T, ?> pattern = null;
        if (selector.getPeriod() != 0) {
            ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);
            ScheduledFuture<Pattern<T, ?>> f =
                    scheduledExecutor.schedule(() -> getPattern(selector), selector.getPeriod(), TimeUnit.MILLISECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduledExecutor.shutdown()));
            try {
                pattern = f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            pattern = getPattern(selector);
        }
        return CEP.pattern(input, pattern);
    }

    private static <T> Pattern<T, ?> getPattern(DynamicPatternSelector<T> selector) {
        Pattern<T, ?> pattern = null;
        try {
            Pair<String, Pattern<T, ?>> pair = selector.select();
            pattern = pair.getRight();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load dynamic pattern", e);
        }
        return pattern;
    }
}