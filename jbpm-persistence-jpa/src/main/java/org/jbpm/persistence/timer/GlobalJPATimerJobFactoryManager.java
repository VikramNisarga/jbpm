package org.jbpm.persistence.timer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.drools.command.CommandService;
import org.drools.persistence.SingleSessionCommandService;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.time.InternalSchedulerService;
import org.drools.time.Job;
import org.drools.time.JobContext;
import org.drools.time.JobHandle;
import org.drools.time.SelfRemovalJob;
import org.drools.time.SelfRemovalJobContext;
import org.drools.time.Trigger;
import org.drools.time.impl.TimerJobFactoryManager;
import org.drools.time.impl.TimerJobInstance;
import org.jbpm.process.instance.timer.TimerManager.ProcessJobContext;

public class GlobalJPATimerJobFactoryManager implements TimerJobFactoryManager {
    private Map<Integer, Map<Long, TimerJobInstance>> timerInstances;
    private Map<Long, TimerJobInstance> singleTimerInstances;
    private CommandService commandService;

    public GlobalJPATimerJobFactoryManager() {
        timerInstances = new ConcurrentHashMap<Integer, Map<Long, TimerJobInstance>>();
        singleTimerInstances = new ConcurrentHashMap<Long, TimerJobInstance>();

    }

    public TimerJobInstance createTimerJobInstance(Job job, JobContext ctx, Trigger trigger, JobHandle handle,
            InternalSchedulerService scheduler) {
        Map<Long, TimerJobInstance> local = null;
        if (ctx instanceof ProcessJobContext) {
            int sessionId = ((StatefulKnowledgeSession) ((ProcessJobContext) ctx).getKnowledgeRuntime()).getId();
            Map<Long, TimerJobInstance> instances = timerInstances.get(sessionId);
            if (instances == null) {
                instances = new ConcurrentHashMap<Long, TimerJobInstance>();
                timerInstances.put(sessionId, instances);
            }
            local = timerInstances.get(sessionId);
        } else {
            local = singleTimerInstances;
        }
        ctx.setJobHandle(handle);
        GlobalJpaTimerJobInstance jobInstance = new GlobalJpaTimerJobInstance(new SelfRemovalJob(job),
                new SelfRemovalJobContext(ctx, local), trigger, handle, scheduler);

        return jobInstance;
    }

    public void addTimerJobInstance(TimerJobInstance instance) {
        
        JobContext ctx = instance.getJobContext();
        if (ctx instanceof SelfRemovalJobContext) {
            ctx = ((SelfRemovalJobContext) ctx).getJobContext();
        }
        Map<Long, TimerJobInstance> instances = null;
        if (ctx instanceof ProcessJobContext) {
            int sessionId = ((StatefulKnowledgeSession) ((ProcessJobContext) ctx).getKnowledgeRuntime()).getId();
            instances = timerInstances.get(sessionId);
            if (instances == null) {
                instances = new ConcurrentHashMap<Long, TimerJobInstance>();
                timerInstances.put(sessionId, instances);
            }
        } else {
            instances = singleTimerInstances;
        }
        instances.put( instance.getJobHandle().getId(),
                                 instance ); 
    }

    public void removeTimerJobInstance(TimerJobInstance instance) {
        JobContext ctx = instance.getJobContext();
        if (ctx instanceof SelfRemovalJobContext) {
            ctx = ((SelfRemovalJobContext) ctx).getJobContext();
        }
        Map<Long, TimerJobInstance> instances = null;
        if (ctx instanceof ProcessJobContext) {
            int sessionId = ((StatefulKnowledgeSession) ((ProcessJobContext) ctx).getKnowledgeRuntime()).getId();
            instances = timerInstances.get(sessionId);
            if (instances == null) {
                instances = new ConcurrentHashMap<Long, TimerJobInstance>();
                timerInstances.put(sessionId, instances);
            }
        } else {
            instances = singleTimerInstances;
        }
        instances.remove( instance.getJobHandle().getId() );    

    }

    public Collection<TimerJobInstance> getTimerJobInstances(Integer sessionId) {
        Map<Long, TimerJobInstance> sessionTimerJobs = timerInstances.get(sessionId);
        if (sessionTimerJobs == null) {
            return Collections.EMPTY_LIST;
        }
        return sessionTimerJobs.values();
    }

    public void setCommandService(CommandService commandService) {
        this.commandService = commandService;
    }

    public CommandService getCommandService() {
        return this.commandService;
    }

    public Collection<TimerJobInstance> getTimerJobInstances() {
        return singleTimerInstances.values();
    }
}