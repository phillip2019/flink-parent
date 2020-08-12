package com.aikosolar.bigdata.flink.job;

import com.aikosolar.bigdata.flink.job.enums.EveStep;
import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
@SuppressWarnings("all")
public class DFEveTagService implements EveTagService {
    @Override
    public EveStep tag(String text) {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        if ("recipe end".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("pump down".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("2nd POCl3-deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Set tube pressure".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Loading completed".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("process started".equalsIgnoreCase(text.trim())) {
            return EveStep.CHANGE;
        }
        return null;
    }

    private DFEveTagService() {
    }

    private static class Holder {
        private static final EveTagService instance = new DFEveTagService();
    }

    public static EveTagService me() {
        return Holder.instance;
    }


}
