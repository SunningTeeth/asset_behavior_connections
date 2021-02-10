package org.daijb.huat.services.utils;

/**
 * @author daijb
 * @date 2021/2/10 17:40
 */
public class StringUtil {

    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0 || "null".equals(str);
    }
}
