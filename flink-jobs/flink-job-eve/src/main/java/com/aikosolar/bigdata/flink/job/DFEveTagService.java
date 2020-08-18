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
        if ("process started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("Loading completed".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("1st POCl3-deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("2nd POCl3-deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("Unloading supervision".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("recipe end".equalsIgnoreCase(text.trim())) {
            return EveStep.CHANGE;
        }

      /*  if ("process started".equalsIgnoreCase(text.trim())) {
            return EveStep.LOAD;
        }
        if ("pump down".equalsIgnoreCase(text.trim())) {
            return EveStep.CONDITION;
        }
        if ("1st POCl3-deposition".equalsIgnoreCase(text.trim())) {
            return EveStep.PROCESS;
        }
        if ("Check Bubbler back to atmo pressure".equalsIgnoreCase(text.trim())) {
            return EveStep.CLEAN;
        }
        if ("backfill".equalsIgnoreCase(text.trim())) {
            return EveStep.UNLOAD;
        }
        if ("recipe end".equalsIgnoreCase(text.trim())) {
            return EveStep.CHANGE;
        }*/
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
