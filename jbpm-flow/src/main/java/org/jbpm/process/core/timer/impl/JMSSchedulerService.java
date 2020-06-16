package org.jbpm.process.core.timer.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.time.AcceptsTimerJobFactoryManager;
import org.drools.time.InternalSchedulerService;
import org.drools.time.Job;
import org.drools.time.JobContext;
import org.drools.time.JobHandle;
import org.drools.time.TimerService;
import org.drools.time.Trigger;
import org.drools.time.impl.TimerJobInstance;
import org.jbpm.process.core.timer.GlobalSchedulerService;
import org.jbpm.process.core.timer.NamedJobContext;
import org.jbpm.process.core.timer.SchedulerServiceInterceptor;
import org.jbpm.process.core.timer.TimerServiceRegistry;
import org.jbpm.process.core.timer.impl.GlobalTimerService.GlobalJobHandle;
import org.jbpm.process.instance.timer.TimerManager.ProcessJobContext;
import org.jbpm.process.instance.timer.TimerManager.StartProcessJobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSSchedulerService implements GlobalSchedulerService {

	public JMSSchedulerService() {
		super();
	}

	private static final Logger logger = LoggerFactory.getLogger(JMSSchedulerService.class);
	private final AtomicLong idCounter = new AtomicLong();
	private TimerService globalTimerService;
	private SchedulerServiceInterceptor interceptor = new DelegateSchedulerServiceInterceptor(this);
	private static JNDIMessageUtility scheduler;
	private static AtomicInteger timerServiceCounter = new AtomicInteger();

	public JobHandle scheduleJob(final Job job, final JobContext ctx, final Trigger trigger) {
		final Long id = idCounter.getAndIncrement();
		String jobname = null;
		if (ctx instanceof ProcessJobContext) {
			final ProcessJobContext processCtx = (ProcessJobContext) ctx;
			jobname = ((StatefulKnowledgeSession) processCtx.getKnowledgeRuntime()).getId() + "-"
					+ processCtx.getProcessInstanceId() + "-" + processCtx.getTimer().getId();
			if (processCtx instanceof StartProcessJobContext) {
				jobname = "StartProcess-" + ((StartProcessJobContext) processCtx).getProcessId() + "-"
						+ processCtx.getTimer().getId();
			}
		} else if (ctx instanceof NamedJobContext) {
			jobname = ((NamedJobContext) ctx).getJobName();
		} else {
			jobname = "Timer-" + ctx.getClass().getSimpleName() + "-" + id;

		}
		/*
		 * final JobKey jobKey = new JobKey(jobname, "jbpm");
		 * 
		 * JobDetail jobDetail = null;
		 */
		/*
		 * try { jobDetail = scheduler.getJobDetail(jobKey); } catch (SchedulerException
		 * e) { throw new RuntimeException(e); }
		 */

		/*
		 * if (jobDetail != null) { final TimerJobInstance timerJobInstance2 =
		 * (TimerJobInstance) jobDetail.getJobDataMap() .get("timerJobInstance"); return
		 * timerJobInstance2.getJobHandle(); }
		 */
		final GlobalJMSJobHandle jmsJobHandle = new GlobalJMSJobHandle(id, jobname, "jbpm");
		final TimerJobInstance jobInstance = ((AcceptsTimerJobFactoryManager) globalTimerService)
				.getTimerJobFactoryManager().createTimerJobInstance(job, ctx, trigger, jmsJobHandle,
						(InternalSchedulerService) globalTimerService);
		jmsJobHandle.setTimerJobInstance((TimerJobInstance) jobInstance);

		interceptor.internalSchedule(jobInstance);
		return jmsJobHandle;
	}

	public void shutdownNow() {
	}

	public boolean removeJob(final JobHandle jobHandle) {
		/*
		 * System.out.println("deleting deleting"); final GlobalQuartzJobHandle
		 * quartzJobHandle = (GlobalQuartzJobHandle) jobHandle;
		 * 
		 * try { final boolean removed = scheduler .deleteJob(new
		 * JobKey(quartzJobHandle.getJobName(), quartzJobHandle.getJobGroup())); return
		 * removed; } catch (final SchedulerException e) {
		 * 
		 * throw new RuntimeException("Exception while removing job", e); } catch (final
		 * RuntimeException e) { SchedulerMetaData metadata; try { metadata =
		 * scheduler.getMetaData(); if
		 * (metadata.getJobStoreClass().isAssignableFrom(JobStoreCMT.class)) { return
		 * true; } } catch (final SchedulerException e1) {
		 * 
		 * } throw e; }
		 */
		return true;
	}

	public void internalSchedule(final TimerJobInstance timerJobInstance) {
		/*
		 * final GlobalQuartzJobHandle quartzJobHandle = (GlobalQuartzJobHandle)
		 * timerJobInstance.getJobHandle(); // Define job instance JobKey jobKey = new
		 * JobKey(quartzJobHandle.getJobName(), quartzJobHandle.getJobGroup()); final
		 * JobDetail jobq =
		 * JobBuilder.newJob().withIdentity(jobKey).ofType(QuartzJob.class).build();
		 * 
		 * jobq.getJobDataMap().put("timerJobInstance", timerJobInstance);
		 * System.out.println("time time time2: "+
		 * timerJobInstance.getTrigger().hasNextFireTime()); // Define a Trigger that
		 * will fire "now" final org.quartz.Trigger triggerq =
		 * TriggerBuilder.newTrigger().forJob(jobq)
		 * .startAt(timerJobInstance.getTrigger().hasNextFireTime()).startNow().build();
		 */

		// Schedule the job with the trigger
		// try {
		/*
		 * i f (scheduler.isShutdown()) { return; }
		 */
		((AcceptsTimerJobFactoryManager) globalTimerService).getTimerJobFactoryManager()
				.addTimerJobInstance(timerJobInstance);
		/*
		 * final JobDetail jobDetail = scheduler .getJobDetail(jobKey);
		 */
		/*
		 * JobDetail jobDetail = null; if (jobDetail == null) {
		 * //scheduler.scheduleJob(jobq, triggerq);
		 * JNDIMessageUtility.getSessiontoQueue( timerJobInstance,"FileWatcherQueue");
		 */
		JNDIMessageUtility.schedule(timerJobInstance);

		/*
		 * } else { // need to add the job again to replace existing especially
		 * important if jobs // are persisted in db scheduler.addJob(jobq, true);
		 * 
		 * scheduler.rescheduleJob( new TriggerKey(quartzJobHandle.getJobName() +
		 * "_trigger", quartzJobHandle.getJobGroup()), triggerq); }
		 */

		/*
		 * } catch (final ObjectAlreadyExistsException e) { // in general this should
		 * not happen even in clustered environment but just in // case // already
		 * registered jobs should be caught in scheduleJob but due to race // conditions
		 * it might not // catch it in time - clustered deployments only logger.
		 * warn("Job has already been scheduled, most likely running in cluster: {}",
		 * e.getMessage());
		 */

		/*
		 * } catch (final JobPersistenceException e) { if (e.getCause() instanceof
		 * NotSerializableException) { // in case job cannot be persisted, like rule
		 * timer then make it in memory internalSchedule(new
		 * InmemoryTimerJobInstanceDelegate(quartzJobHandle.getJobName(),
		 * ((GlobalTimerService) globalTimerService).getTimerServiceId())); } else {
		 * ((AcceptsTimerJobFactoryManager)
		 * globalTimerService).getTimerJobFactoryManager()
		 * .removeTimerJobInstance(timerJobInstance); throw new RuntimeException(e); } }
		 * catch (SchedulerException e) { ((AcceptsTimerJobFactoryManager)
		 * globalTimerService).getTimerJobFactoryManager()
		 * .removeTimerJobInstance(timerJobInstance); throw new
		 * RuntimeException("Exception while scheduling job", e); }
		 */
	}

	public synchronized void initScheduler(final TimerService timerService) {
		this.globalTimerService = timerService;
		timerServiceCounter.incrementAndGet();

		if (scheduler == null) {
			scheduler.initialize();
			/*
			 * try { scheduler = StdSchedulerFactory.getDefaultScheduler();
			 * scheduler.start(); } catch (SchedulerException e) { throw new
			 * RuntimeException("Exception when initializing QuartzSchedulerService", e); }
			 */

		}
	}

	public void shutdown() {
		int current = timerServiceCounter.decrementAndGet();
		if (scheduler != null && current == 0) {
			// try {
			scheduler.shutdown();
			/*
			 * } catch (SchedulerException e) {
			 * logger.warn("Error encountered while shutting down the scheduler", e); }
			 */
			scheduler = null;
		}
	}

	public void forceShutdown() {
		/*
		 * if (scheduler != null) { try { scheduler.shutdown(); } catch
		 * (SchedulerException e) {
		 * logger.warn("Error encountered while shutting down (forced) the scheduler",
		 * e); } scheduler = null; }
		 */
	}

	public JobHandle buildJobHandleForContext(final NamedJobContext ctx) {
		return new GlobalJMSJobHandle(-1, ctx.getJobName(), "jbpm");
	}

	public boolean isTransactional() {
		/*
		 * try { Class<?> jobStoreClass = scheduler.getMetaData().getJobStoreClass(); if
		 * (JobStoreSupport.class.isAssignableFrom(jobStoreClass)) { return true; } }
		 * catch (Exception e) { logger.
		 * warn("Unable to determine if quartz is transactional due to problems when checking job store class"
		 * , e); } return false;
		 */
		return true;
	}

	public boolean retryEnabled() {
		return false;
	}

	public void setInterceptor(SchedulerServiceInterceptor interceptor) {
		this.interceptor = interceptor;

	}

	public static class GlobalJMSJobHandle extends GlobalJobHandle {

		private static final long serialVersionUID = 510l;
		private String jobName;
		private String jobGroup;

		public GlobalJMSJobHandle(final long id, final String name, final String group) {
			super(id);
			this.jobName = name;
			this.jobGroup = group;
		}

		public String getJobName() {
			return jobName;
		}

		public void setJobName(final String jobName) {
			this.jobName = jobName;
		}

		public String getJobGroup() {
			return jobGroup;
		}

		public void setJobGroup(final String jobGroup) {
			this.jobGroup = jobGroup;
		}

	}

	/*
	 * public static class QuartzJob implements org.quartz.Job { public QuartzJob()
	 * {
	 * 
	 * }
	 * 
	 * public void execute(final JobExecutionContext quartzContext) throws
	 * JobExecutionException { TimerJobInstance timerJobInstance =
	 * (TimerJobInstance) quartzContext.getJobDetail().getJobDataMap()
	 * .get("timerJobInstance"); System.out.println("testing"); try {
	 * ((Callable<Void>) timerJobInstance).call(); } catch (Exception e) { boolean
	 * reschedule = true; Integer failedCount = (Integer)
	 * quartzContext.getJobDetail().getJobDataMap().get("failedCount"); if
	 * (failedCount == null) { failedCount = new Integer(0); } failedCount++;
	 * quartzContext.getJobDetail().getJobDataMap().put("failedCount", failedCount);
	 * if (failedCount > 5) {
	 * logger.error("Timer execution failed 5 times in a roll, unscheduling ({})",
	 * quartzContext.getJobDetail()); reschedule = false; } // let's give it a bit
	 * of time before failing/retrying try { Thread.sleep(failedCount * 1000); }
	 * catch (InterruptedException e1) { logger.debug("Got interrupted", e1); }
	 * throw new JobExecutionException("Exception when executing scheduled job", e,
	 * reschedule); }
	 * 
	 * } }
	 */

	public static class InmemoryTimerJobInstanceDelegate implements TimerJobInstance, Serializable, Callable<Void> {

		private static final long serialVersionUID = 1L;
		private String jobname;
		private String timerServiceId;
		private transient TimerJobInstance delegate;

		public InmemoryTimerJobInstanceDelegate(String jobName, String timerServiceId) {
			this.jobname = jobName;
			this.timerServiceId = timerServiceId;
		}

		public JobHandle getJobHandle() {
			findDelegate();
			return delegate.getJobHandle();
		}

		public Job getJob() {
			findDelegate();
			return delegate.getJob();
		}

		public Trigger getTrigger() {
			findDelegate();
			return delegate.getTrigger();
		}

		public JobContext getJobContext() {
			findDelegate();
			return delegate.getJobContext();
		}

		protected void findDelegate() {
			if (delegate == null) {
				Collection<TimerJobInstance> timers = ((AcceptsTimerJobFactoryManager) TimerServiceRegistry
						.getInstance().get(timerServiceId)).getTimerJobFactoryManager().getTimerJobInstances();
				for (TimerJobInstance instance : timers) {
					if (((GlobalJMSJobHandle) instance.getJobHandle()).getJobName().equals(jobname)) {
						delegate = instance;
						break;
					}
				}
			}
		}

		@SuppressWarnings("unchecked")
		public Void call() throws Exception {
			findDelegate();
			return ((Callable<Void>) delegate).call();
		}

	}

}