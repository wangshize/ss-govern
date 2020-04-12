package org.ss.govern.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangsz
 * @create 2020-04-13
 **/
public class ThreadUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadUtils.class);

    public static void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            LOG.error("thread sleep is interrupted....");
        }
    }
}
