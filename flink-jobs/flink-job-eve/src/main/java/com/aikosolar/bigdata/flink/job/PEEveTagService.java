package com.aikosolar.bigdata.flink.job;

import com.aikosolar.bigdata.flink.job.enums.EveStep;
import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
@SuppressWarnings("all")
public class PEEveTagService implements EveTagService {

    @Override
    public EveStep tag(String text) {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        if ("Recipe started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("Input: Define manually rework: <N/Y> N Recipe: /PROCESS/V1PDV101;6".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("Memory Text1 deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Memory Text2 Evacuate tube and pressure test".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Memory Text1 unload".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("Recipe End Recipe:".contains(text.trim())) {
            return EveStep.CHANGE;
        }
        return null;
    }

    private PEEveTagService() {
    }

    private static class Holder {
        private static final EveTagService instance = new PEEveTagService();
    }

    public static EveTagService me() {
        return Holder.instance;
    }
}
