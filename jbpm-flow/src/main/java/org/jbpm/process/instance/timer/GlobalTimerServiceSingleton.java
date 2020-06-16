package org.jbpm.process.instance.timer;

import org.drools.time.TimerService;
import org.jbpm.process.core.timer.impl.GlobalTimerService;
import org.jbpm.process.core.timer.impl.JMSSchedulerService;

public class GlobalTimerServiceSingleton {

    private static GlobalTimerService globalTimerService;

    public static TimerService getTimerService() {
        if (globalTimerService == null) {
            synchronized (GlobalTimerServiceSingleton.class) {
                if (globalTimerService == null) {
                    globalTimerService = new GlobalTimerService(new JMSSchedulerService());
                }
            }
        }
        return globalTimerService;
    }

}