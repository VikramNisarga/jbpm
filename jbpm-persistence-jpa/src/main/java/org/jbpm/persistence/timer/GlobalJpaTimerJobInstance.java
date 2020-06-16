package org.jbpm.persistence.timer;

import javax.persistence.EntityManagerFactory;

import org.drools.KnowledgeBase;
import org.drools.SessionConfiguration;
import org.drools.command.CommandService;
import org.drools.command.impl.CommandBasedStatefulKnowledgeSession;
import org.drools.common.InternalKnowledgeRuntime;
import org.drools.persistence.SingleSessionCommandService;
import org.drools.persistence.jpa.JDKCallableJobCommand;
import org.drools.persistence.jpa.JpaTimerJobInstance;
import org.drools.persistence.jpa.processinstance.JPAWorkItemManager;
import org.drools.runtime.Environment;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.process.WorkItemManager;
import org.drools.time.InternalSchedulerService;
import org.drools.time.Job;
import org.drools.time.JobContext;
import org.drools.time.JobHandle;
import org.drools.time.SelfRemovalJobContext;
import org.drools.time.Trigger;
import org.jbpm.process.core.timer.impl.GlobalTimerService;
import org.jbpm.process.instance.timer.GlobalTimerServiceSingleton;
import org.jbpm.process.instance.timer.TimerManager.ProcessJobContext;

import com.pft.workflow.core.JBPMHelper;
import com.pft.workflow.core.JPAHelper;
import com.pft.workflow.core.WorkFlowPersistenceService;
import com.pft.workflow.core.bpmengine.SessionManager;
import com.pft.workflow.kbase.DefaultKnowledgeBaseManager;

public class GlobalJpaTimerJobInstance extends JpaTimerJobInstance {
	private static final long serialVersionUID = -5383556604449217342L;
	private String timerServiceId;
	private transient StatefulKnowledgeSession knowledgeSession;

	public GlobalJpaTimerJobInstance(Job job, JobContext ctx, Trigger trigger, JobHandle handle,
			InternalSchedulerService scheduler) {
		super(job, ctx, trigger, handle, scheduler);
		timerServiceId = ((GlobalTimerService) scheduler).getTimerServiceId();
	}

	public void setSession(StatefulKnowledgeSession knowledgeSession) {
		this.knowledgeSession = knowledgeSession;

	}

	public Void call() throws Exception {
		try {
			JDKCallableJobCommand command = new JDKCallableJobCommand(this);
			// if (scheduler == null) {
			// scheduler = (InternalSchedulerService)
			// TimerServiceRegistry.getInstance().get(timerServiceId);
			// }
			setSchedulerService();
			JobContext jobContext = getJobContext();
			if (jobContext instanceof SelfRemovalJobContext) {
				jobContext = ((SelfRemovalJobContext) jobContext).getJobContext();
			}
			ProcessJobContext ctx = null;
			if (jobContext instanceof ProcessJobContext) {
				ctx = (ProcessJobContext) jobContext;
			}
			/*
			 * EntityManagerFactory emf =
			 * WorkFlowPersistenceService.getCurrentEntityManagerFactory(); Environment env
			 * = JBPMHelper.createEnvironment(emf);
			 * 
			 * DefaultKnowledgeBaseManager manager = new DefaultKnowledgeBaseManager();
			 * KnowledgeBase knowledgeBase = manager.getKnowledgeBase();
			 */
			// long pid = ctx.getProcessInstanceId();
			/*
			 * InitialContext initialContext = new InitialContext(); UserTransaction utx =
			 * (UserTransaction) initialContext.lookup("java:comp/UserTransaction");
			 * utx.begin();
			 */

			// StatefulKnowledgeSession knowledgeSession =
			// SessionManager.loadStatefulKnowledgeSession(knowledgeBase,
			// JPAHelper.getKsessionId(pid));
			// WorkItemManager workItemManager = knowledgeSession.getWorkItemManager();
			if (this.knowledgeSession != null) {
				CommandBasedStatefulKnowledgeSession cs = (CommandBasedStatefulKnowledgeSession) this.knowledgeSession;
				CommandService commandService = cs.getCommandService();
				SingleSessionCommandService ssc = (SingleSessionCommandService) commandService;
				ctx.setKnowledgeRuntime((InternalKnowledgeRuntime) ssc.getStatefulKnowledgeSession());
				knowledgeSession.execute(command);
			}

			/*
			 * singleSessionCommandService = new
			 * SingleSessionCommandService(JPAHelper.getKsessionId(pid), knowledgeBase, new
			 * SessionConfiguration(), env); StatefulKnowledgeSession
			 * statefulKnowledgeSession =
			 * singleSessionCommandService.getStatefulKnowledgeSession();
			 * ctx.setKnowledgeRuntime((InternalKnowledgeRuntime)statefulKnowledgeSession);
			 * singleSessionCommandService.execute(command);
			 */
			// utx.commit();
			return null;
		} catch (Exception e) {
			throw e;
		}

	}

	public void setSchedulerService() {
		super.scheduler = (InternalSchedulerService) GlobalTimerServiceSingleton.getTimerService();
	}
}