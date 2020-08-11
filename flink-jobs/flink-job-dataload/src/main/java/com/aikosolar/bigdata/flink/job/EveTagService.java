package com.aikosolar.bigdata.flink.job;

import com.aikosolar.bigdata.flink.job.enums.EveStep;

/**
 * @author carlc
 */
public interface EveTagService {
    EveStep tag(String text);
}
