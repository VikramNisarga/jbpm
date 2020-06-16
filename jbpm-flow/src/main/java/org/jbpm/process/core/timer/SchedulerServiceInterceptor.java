package org.jbpm.process.core.timer;

import org.drools.time.impl.TimerJobInstance;

public interface SchedulerServiceInterceptor {
    void internalSchedule(TimerJobInstance timerJobInstance);
}