package org.jbpm.process.core.timer.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.drools.time.JobContext;
import org.drools.time.impl.TimerJobInstance;
import org.jbpm.process.instance.timer.TimerManager.ProcessJobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pft.framework.core.RequestContext;

public class JNDIMessageUtility {

	private static final String DEFAULT_CONNECTION_FACTORY = "java:/RemoteJmsXA";
	private static final String INITIAL_CONTEXT_FACTORY = "org.jboss.naming.remote.client.InitialCONTEXTFactory";
	private final static Properties ENV = new Properties();
	private final static Logger LOG = LoggerFactory.getLogger(JNDIMessageUtility.class);
	private static InitialContext CONTEXT;
	private static ConnectionFactory connectionFactory;
	// private static Connection connection;
	// private static Session session;
	// private static Queue queue;
	private static HashMap<String, Destination> DESTINATIONS;
	private static final String DEFAULT_QUEUE_PREFIX = "queue/";
	private static final String PERF_MS = "/JNDIMessageUtility/{} <perf>:{} ms";
	private static MessageProducer messageProducer;

	public static void initialize() {
		try {
			CONTEXT = new InitialContext();
			final String connectionFactoryString = System.getProperty("timer.connection.factory",
					DEFAULT_CONNECTION_FACTORY);
			LOG.info("Attempting to acquire connection factory \"" + connectionFactoryString + "\"");
			connectionFactory = (ConnectionFactory) CONTEXT.lookup(connectionFactoryString);
			LOG.info("Found connection factory \"" + connectionFactoryString + "\" in JNDI");
		} catch (NamingException e) {
			throw new RuntimeException(e);
		}

	}

	public static void schedule(TimerJobInstance instance) {
		ObjectMessage objectMessage;
		Connection connection = null;
		try {
			LOG.info("sending timer job to queue: " + instance);
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
			Queue queue = session.createQueue(System.getProperty("TimerQueue", "TimerQueue"));
			com.pft.framework.core.ApplicationContext context = RequestContext.getCurrentApplicationContext();
			messageProducer = session.createProducer(queue);
			objectMessage = session.createObjectMessage((Serializable) instance);
			objectMessage.setLongProperty("_HQ_SCHED_DELIVERY", instance.getTrigger().hasNextFireTime().getTime());
			JobContext ctx = instance.getJobContext();
			if (ctx instanceof ProcessJobContext) {
				final ProcessJobContext processCtx = (ProcessJobContext) ctx;
				objectMessage.setStringProperty("JMSXGroupID",
						context.getTenantId() + "-" + processCtx.getProcessInstanceId());
				objectMessage.setJMSCorrelationID(processCtx.getProcessInstanceId() + "-"
						+ instance.getJobHandle().getId() + "-" + context.getTenantId());
				objectMessage.setStringProperty("TenantId", String.valueOf(context.getTenantId()));
				objectMessage.setStringProperty("ProcessInstanceId", String.valueOf(processCtx.getProcessInstanceId()));
			}
			connection.start();
			messageProducer.send(objectMessage);
		} catch (JMSException e) {
			LOG.error("Unable to schedule timer for " + instance);
			throw new RuntimeException(e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (JMSException e) {
					LOG.warn("Unable to close jms connection : " + e);
				}
			}
		}

	}

}
