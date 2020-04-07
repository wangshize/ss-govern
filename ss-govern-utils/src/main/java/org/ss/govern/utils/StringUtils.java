package org.ss.govern.utils;

/**
 * @author wangsz
 * @create 2020-04-05
 **/
public class StringUtils {

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }
}
