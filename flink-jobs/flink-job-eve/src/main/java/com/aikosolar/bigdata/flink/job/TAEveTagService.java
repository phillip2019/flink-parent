package com.aikosolar.bigdata.flink.job;

import com.aikosolar.bigdata.flink.job.enums.EveStep;
import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
@SuppressWarnings("all")
public class TAEveTagService implements EveTagService {

    @Override
    public EveStep tag(String text) {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        throw new UnsupportedOperationException("不支持");
    }

    private TAEveTagService() {
    }

    private static class Holder {
        private static final EveTagService instance = new TAEveTagService();
    }

    public static EveTagService me() {
        return Holder.instance;
    }
}
