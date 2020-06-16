package org.jbpm.process.core.timer.impl;

import org.drools.time.impl.TimerJobInstance;
import org.jbpm.process.core.timer.GlobalSchedulerService;
import org.jbpm.process.core.timer.SchedulerServiceInterceptor;

public class DelegateSchedulerServiceInterceptor implements SchedulerServiceInterceptor {

    protected GlobalSchedulerService delegate;
    
    public DelegateSchedulerServiceInterceptor(GlobalSchedulerService service) {
        this.delegate = service;
    }

    @Override
    public void internalSchedule(TimerJobInstance timerJobInstance) {
        this.delegate.internalSchedule(timerJobInstance);
    }
}