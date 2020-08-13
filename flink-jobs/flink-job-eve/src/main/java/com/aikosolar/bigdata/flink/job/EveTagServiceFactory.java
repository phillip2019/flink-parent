package com.aikosolar.bigdata.flink.job;

import org.apache.commons.lang3.StringUtils;

/**
 * @author carlc
 */
public class EveTagServiceFactory {
    public static EveTagService getEveTagService(String type) {
        if (StringUtils.isBlank(type)) {
            return null;
        }

        switch (type) {
            case "DF":
                return DFEveTagService.me();
            case "PE":
                return PEEveTagService.me();
            case "PR":
                return PREveTagService.me();
            case "TA":
                return TAEveTagService.me();
            default: {
                // 目前内置的仅包括:DF,PE,PR,TA类型,后续扩展可以在此写case语句,或者不写
                // 如果不写，则按照如下规则获取EveTagService
                // 1.如果type长度 <= 3, 则认为是类是DF/PE/PR这种简写，则拼接成类的全限定名 => 加载 => 调用约定的静态方法(me) => 返回
                // 2.如果type长度 > 3,  则认为是类的全限定名 => 加载 => 调用约定的静态方法(me) => 返回
                try {
                    String clazzName = type;
                    if (type.length() <= 3) {
                        clazzName = String.format("com.aikosolar.bigdata.flink.job.%sEveTagService", type);
                    }
                    return (EveTagService) (Class.forName(clazzName)).getMethod("me").invoke(null);
                } catch (Exception e) {
                    // NO-OP
                    return null;
                }
            }
        }
    }
}
