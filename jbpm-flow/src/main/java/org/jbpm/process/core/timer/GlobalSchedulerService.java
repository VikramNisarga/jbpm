package org.jbpm.process.core.timer;

import org.drools.time.InternalSchedulerService;
import org.drools.time.JobHandle;
import org.drools.time.SchedulerService;
import org.drools.time.TimerService;

public interface GlobalSchedulerService extends SchedulerService, InternalSchedulerService {
     /**
     * Provides handle to inject timerService that owns this scheduler service and initialize it
     * @param timerService owner of this scheduler service
     */
    void initScheduler(TimerService timerService);
    
    /**
     * Allows to shutdown the scheduler service
     */
    void shutdown();
    
    JobHandle buildJobHandleForContext(NamedJobContext ctx);
    
    boolean isTransactional();
    
    boolean retryEnabled();
    
    void setInterceptor(SchedulerServiceInterceptor interceptor);
}