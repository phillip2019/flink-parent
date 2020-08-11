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
        if ("Recipe started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("Memory Text1 Increase pressure for temp. stabilization".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("Switch to local scrubber exhaust.".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Memory Text1 Evacuate Tube and MFCs".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Memory Text1 Fill tube with N2".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("Recipe End Recipe:".equalsIgnoreCase(text.trim())) {
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
