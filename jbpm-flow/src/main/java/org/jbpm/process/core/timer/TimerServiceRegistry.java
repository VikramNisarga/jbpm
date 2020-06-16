package org.jbpm.process.core.timer;

import java.util.concurrent.ConcurrentHashMap;

import org.drools.time.TimerService;
import org.jbpm.process.core.timer.impl.GlobalTimerService;

public class TimerServiceRegistry {
    public static final String TIMER_SERVICE_SUFFIX = "-timerServiceId";
    private ConcurrentHashMap<String, TimerService> registeredServices = new ConcurrentHashMap<String, TimerService>();
    
    private static TimerServiceRegistry instance;
    
    public static TimerServiceRegistry getInstance() {
        if (instance == null) {
            instance = new TimerServiceRegistry();
        }
        
        return instance;
    }
    
    /**
     * Registers timerServie under given id. In case timer service is already registered  
     * with this id it will be overridden.
     * @param id key used to get hold of the timer service instance
     * @param timerService fully initialized TimerService instance
     */
    public void registerTimerService(String id, TimerService timerService) {   
        if (timerService instanceof GlobalTimerService) {
            ((GlobalTimerService) timerService).setTimerServiceId(id);
        }
        this.registeredServices.put(id, timerService);
    }
    
    /**
     * Returns TimerService instance registered under given key
     * @param id timer service identifier
     * @return returns timer service instance or null of there was none registered with given id
     */
    public TimerService get(String id) {
        if (id == null) {
            return null;
        }
        return this.registeredServices.get(id);
    }
    
    /**
     * Removes TimerService from the registry.
     * @param id timer service identifier
     * @return returns TimerService instance returned from the registry for cleanup tasks
     */
    public TimerService remove(String id) {
        return this.registeredServices.remove(id);
    }
}