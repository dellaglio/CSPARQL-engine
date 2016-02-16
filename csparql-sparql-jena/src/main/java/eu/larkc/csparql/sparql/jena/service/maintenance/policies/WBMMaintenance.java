package eu.larkc.csparql.sparql.jena.service.maintenance.policies;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;
import eu.larkc.csparql.sparql.jena.service.maintenance.UpdateEvent;

public class WBMMaintenance implements MaintenancePolicy {
	private static Logger logger = LoggerFactory.getLogger(WBMMaintenance.class);
	
	TreeSet<UpdateEvent> planUpdateEvents(QueryIterServiceMaintainedCache qi) {
		TreeSet<UpdateEvent> results = new TreeSet<UpdateEvent>(UpdateEvent.Comparators.SCORE);
		HashMap<Binding,Long> currentWindowBindingsTS= qi.getCurrentBindingOfWindow();
		HashMap<Binding,Integer> cacheChangeRates=qi.getChangeRate();
		for (Binding e : qi.getExpiredWindowBindings()) {
			long lastTime = currentWindowBindingsTS.get(e);			
			// for each fund calculate the score
			lastTime += qi.getWindowLength()*1000;
			/*int changeRate = cacheChangeRates.get(qi.getkeyBinding(e));
			//computing V and L
			UpdateEvent tempEvent = UpdateEvent.planOneUpdateEvent(e, changeRate, lastTime, qi.getTnow(),qi.getBBT(qi.getkeyBinding(e)));*/
			int changeRate;
			UpdateEvent tempEvent=null;
			if(qi.isCached(e))
				{
				changeRate = cacheChangeRates.get(qi.getkeyBinding(e));
				//computing V and L
				tempEvent = UpdateEvent.planOneUpdateEvent(e, changeRate, lastTime, 
						qi.getTnow(),qi.getBBT(qi.getkeyBinding(e)));
			}else{
				logger.warn("there is no change rate and bbt for the window binding => window binding has no compatible mapping in cache and cache is restrcited");
				logger.warn("random change rate is assigned and bbt is the current time to specify the window mapping is stale and include them in the elecyed lists");
				changeRate=Integer.MAX_VALUE;				
				//computing V and L
				tempEvent = UpdateEvent.planOneUpdateEvent(e, changeRate, lastTime, 
						qi.getTnow(),qi.getTnow());
			}
			
			//add the update event to the sorted list
			results.add(tempEvent);
			//logger.debug("tempEvent:" + tempEvent + " scores " + tempEvent.fundToScore + " originalScore " + tempEvent.scoreForUpdate);
		}
		logger.debug("event list:" + results);
		return results;
	}
	@Override
	public Set<Binding> updatePolicy(QueryIterRepeatApply qi, int budget) {
		// plan event for the current expired data
		TreeSet<UpdateEvent> plannedExpriedUpdate = new TreeSet<UpdateEvent>();

		if(qi instanceof QueryIterServiceMaintainedCache){
			HashMap<Binding,Long> windowEntriesTS = ((QueryIterServiceMaintainedCache)qi).getCurrentBindingOfWindow();
			if(!(budget<windowEntriesTS.size()))
				return windowEntriesTS.keySet();
			plannedExpriedUpdate = planUpdateEvents(((QueryIterServiceMaintainedCache)qi));/*.getExpiredWindowBindings(),
					((QueryIterServiceMaintainedCache)qi).getCacheChangeRate(),
					((QueryIterServiceMaintainedCache)qi).getCacheBBT());*/

			// choose TopK from currentExpriedUpdate
			int tempBudget = budget;
			Set<Binding> electedElements = new HashSet<Binding>();
			while (tempBudget > 0 && plannedExpriedUpdate.size() > 0) {
				UpdateEvent tempEvent = plannedExpriedUpdate.pollLast();
				electedElements.add(tempEvent.BKGBindingToUpdate);
				tempBudget--;
			}
			
			return electedElements;

	
		}
		return null;
	}

}
