package com.aikosolar.bigdata.flink.job;

import com.aikosolar.bigdata.flink.job.enums.EveStep;
import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
@SuppressWarnings("all")
public class PREveTagService implements EveTagService {

    @Override
    public EveStep tag(String text) {
        if (StringUtils.isBlank(text)) {
            return null;
        }
        if ("process started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("Loading done".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("prepare Alox deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Evacuate Tube and MFCs".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Fill tube with N2".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if (("end of process").equals(text.trim())) {
            return EveStep.CHANGE;
        }
        return null;
    }


    private PREveTagService() {
    }

    private static class Holder {
        private static final EveTagService instance = new PREveTagService();
    }

    public static EveTagService me() {
        return Holder.instance;
    }
}
