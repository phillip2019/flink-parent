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
        if ("1.0".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("3.0".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("4.0".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("6.0".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("7.0".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("8.0".contains(text.trim())) {
            return EveStep.CHANGE;
        }
        return null;

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
