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
        if ("process started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("Loading done".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("Memory Text1 prepare deposition Layer 1".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Evacuate Tube and MFCs".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Fill tube with N2".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if (text.trim().contains("end of process")) {
            return EveStep.CHANGE;
        }
        return null;
    }
/*process started
Loading done
Memory Text1 prepare deposition Layer 1
Evacuate Tube and MFCs
Fill tube with N2
end of process*/
    private PEEveTagService() {
    }

    private static class Holder {
        private static final EveTagService instance = new PEEveTagService();
    }

    public static EveTagService me() {
        return Holder.instance;
    }
}
