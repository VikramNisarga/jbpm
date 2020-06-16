/*
 * Copyright 2010 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jbpm.process.core.timer.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.drools.command.Command;
import org.drools.command.CommandService;
import org.drools.command.Context;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.time.AcceptsTimerJobFactoryManager;
import org.drools.time.InternalSchedulerService;
import org.drools.time.Job;
import org.drools.time.JobContext;
import org.drools.time.JobHandle;
import org.drools.time.SelfRemovalJobContext;
import org.drools.time.SessionClock;
import org.drools.time.TimerService;
import org.drools.time.Trigger;
import org.drools.time.impl.DefaultJobHandle;
import org.drools.time.impl.DefaultTimerJobFactoryManager;
import org.drools.time.impl.TimerJobFactoryManager;
import org.drools.time.impl.TimerJobInstance;
import org.jbpm.process.core.timer.GlobalSchedulerService;
import org.jbpm.process.instance.timer.TimerManager.ProcessJobContext;

/**
 * A default Scheduler implementation that uses the JDK built-in
 * ScheduledThreadPoolExecutor as the scheduler and the system clock as the
 * clock.
 */
public class GlobalTimerService
        implements TimerService, SessionClock, InternalSchedulerService, AcceptsTimerJobFactoryManager {

    private AtomicLong idCounter = new AtomicLong();

    protected GlobalSchedulerService schedulerService;
    protected TimerJobFactoryManager jobFactoryManager = DefaultTimerJobFactoryManager.instance;
    protected ConcurrentHashMap<Integer, List<GlobalJobHandle>> timerJobsPerSession = new ConcurrentHashMap<Integer, List<GlobalJobHandle>>();
    private String timerServiceId;

    public GlobalTimerService(GlobalSchedulerService schedulerService) {
        this.schedulerService = schedulerService;
        this.schedulerService.initScheduler(this);
        try {
            this.jobFactoryManager = (TimerJobFactoryManager) Class.forName("org.jbpm.persistence.timer.GlobalJPATimerJobFactoryManager").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getTimerServiceId() {
        return timerServiceId;
    }

    public void setTimerServiceId(String timerServiceId) {
        this.timerServiceId = timerServiceId;
    }

    public void setTimerJobFactoryManager(TimerJobFactoryManager timerJobFactoryManager) {
        this.jobFactoryManager = timerJobFactoryManager;
        if (this.jobFactoryManager.getCommandService() == null) {
    		this.jobFactoryManager.setCommandService(timerJobFactoryManager.getCommandService());
    	}
    }

    public void setCounter(long counter) {
        idCounter = new AtomicLong( counter );
    }
    
    public TimerJobFactoryManager getTimerJobFactoryManager() {
        return this.jobFactoryManager;
    }

    /**
     * @inheritDoc
     */
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public void shutdown() {
        // forcing a shutdownNow instead of a regular shutdown()
        // to avoid delays on shutdown. This is an irreversible 
        // operation anyway, called on session dispose.
    }

    public JobHandle scheduleJob(Job job,
                                 JobContext ctx,
                                 Trigger trigger) {

        if (ctx instanceof ProcessJobContext) {
            ProcessJobContext processCtx = (ProcessJobContext) ctx;
            List<GlobalJobHandle> jobHandles = timerJobsPerSession.get(((StatefulKnowledgeSession) processCtx.getKnowledgeRuntime()).getId());
            if (jobHandles == null) {
                jobHandles = new CopyOnWriteArrayList<GlobalJobHandle>();
                timerJobsPerSession.put(((StatefulKnowledgeSession) processCtx.getKnowledgeRuntime()).getId(), jobHandles);
            }else {
                // check if the given job is already scheduled
                for (GlobalJobHandle handle : jobHandles) {
                    long timerId = handle.getTimerId();
                    if (timerId == processCtx.getTimer().getId()) {
                        // this timer job is already registered
                        return handle;
                    }
                }
            }
            GlobalJobHandle jobHandle = (GlobalJobHandle) this.schedulerService.scheduleJob(job, ctx, trigger);
            jobHandles.add(jobHandle);
            
            return jobHandle;
        }

        GlobalJobHandle jobHandle = (GlobalJobHandle) this.schedulerService.scheduleJob(job, ctx, trigger);
        return jobHandle;
    }

    public void internalSchedule(TimerJobInstance timerJobInstance) {
        if (this.schedulerService instanceof InternalSchedulerService) {
            ((InternalSchedulerService) this.schedulerService).internalSchedule(timerJobInstance);
        } else {
            throw new UnsupportedOperationException("Unsupported scheduler operation internalSchedule on class " + this.schedulerService.getClass()); 
        }
    }

    public boolean removeJob(JobHandle jobHandle) {
        if (jobHandle == null) {
            return false;
        }
        
        int sessionId = ((GlobalJobHandle) jobHandle).getSessionId();
        List<GlobalJobHandle> handles = timerJobsPerSession.get(sessionId);
        if (handles == null) {
            return this.schedulerService.removeJob(jobHandle);
        }
        if (handles.contains(jobHandle)) {
            handles.remove(jobHandle);
            if (handles.isEmpty()) {
                timerJobsPerSession.remove(sessionId);
            }
            return this.schedulerService.removeJob(jobHandle);
        } else {
            return false;
        }
    }


    public CommandService getCommandService(JobContext jobContext) {
        JobContext ctxorig = jobContext;
        if (ctxorig instanceof SelfRemovalJobContext) {
            ctxorig = ((SelfRemovalJobContext) ctxorig).getJobContext();
        }
        ProcessJobContext ctx = null;
        if (ctxorig instanceof ProcessJobContext) {
            ctx = (ProcessJobContext) ctxorig;
        } else {
            return jobFactoryManager.getCommandService(); 
        }
        // RuntimeEngine runtime = manager.getRuntimeEngine(ProcessInstanceIdContext.get(ctx.getProcessInstanceId()));
        // if (runtime == null) {
        //     throw new RuntimeException("No runtime engine found, could not be initialized yet");
        // }
        
        // if (runtime.getKieSession() instanceof CommandBasedStatefulKnowledgeSession) {
        //     CommandBasedStatefulKnowledgeSession cmd = (CommandBasedStatefulKnowledgeSession) runtime.getKieSession();
        //     ctx.setKnowledgeRuntime((InternalKnowledgeRuntime) ((KnowledgeCommandContext) cmd.getCommandService().getContext()).getKieSession());
            
        //     return new DisposableCommandService(cmd.getCommandService(), manager, runtime, schedulerService.retryEnabled());
        // } else if (runtime.getKieSession() instanceof InternalKnowledgeRuntime) {
        //     ctx.setKnowledgeRuntime((InternalKnowledgeRuntime) runtime.getKieSession());
            
            
        // }
        
        return jobFactoryManager.getCommandService();
    }

    public static class DisposableCommandService implements CommandService {

        private CommandService delegate;
        private boolean retry = false;
        
        
        public DisposableCommandService(CommandService delegate,  boolean retry) {
            this.delegate = delegate;
            this.retry = retry;
        }

        public <T> T execute(Command<T> command) {
        	try {
        		return delegate.execute(command);
        	} catch (RuntimeException e) {
        		if (retry) {
        			return delegate.execute(command);
        		} else {
        			throw e;
        		}
        	}
        }

        public Context getContext() {
            return delegate.getContext();
        }
        
        public void dispose() {
            System.out.println("need to implement dispose");
        }
        
    }

    public static class GlobalJobHandle extends DefaultJobHandle
        implements
        JobHandle{
    
        private static final long     serialVersionUID = 510l;
    
        public GlobalJobHandle(long id) {
            super(id);
        }
        
        public long getTimerId() {
            JobContext ctx = this.getTimerJobInstance().getJobContext();
            if (ctx instanceof SelfRemovalJobContext) {
                ctx = ((SelfRemovalJobContext) ctx).getJobContext();
            }
            return ((ProcessJobContext)ctx).getTimer().getId();
        }
    
        public int getSessionId() {
            JobContext ctx = this.getTimerJobInstance().getJobContext();
            if (ctx instanceof SelfRemovalJobContext) {
                ctx = ((SelfRemovalJobContext) ctx).getJobContext();
            }
            if (ctx instanceof ProcessJobContext) {
                return ((StatefulKnowledgeSession) ((ProcessJobContext)ctx).getKnowledgeRuntime()).getId();
            }
            
            return -1;
        }

    }

    public long getTimeToNextJob() {
        // TODO Auto-generated method stub
        return 0;
    }

    public Collection<TimerJobInstance> getTimerJobInstances(int id) {
        Collection<TimerJobInstance> timers = new ArrayList<TimerJobInstance>();
        List<GlobalJobHandle> jobs = timerJobsPerSession.get(id); {
            if (jobs != null) {
                for (GlobalJobHandle job : jobs) {
                    timers.add(job.getTimerJobInstance());
                }
            }
        }        
        return timers;
    }

    public Collection<TimerJobInstance> getTimerJobInstances() {
         Collection<TimerJobInstance> timers = new ArrayList<TimerJobInstance>();
        List<GlobalJobHandle> jobs = timerJobsPerSession.get(0); {
            if (jobs != null) {
                for (GlobalJobHandle job : jobs) {
                    timers.add(job.getTimerJobInstance());
                }
            }
        }        
        return timers;
    }

}
