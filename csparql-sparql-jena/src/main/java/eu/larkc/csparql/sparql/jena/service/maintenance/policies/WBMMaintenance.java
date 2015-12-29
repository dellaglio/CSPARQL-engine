package eu.larkc.csparql.sparql.jena.service.maintenance.policies;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;

public class WBMMaintenance implements MaintenancePolicy {

	@Override
	public Set<Binding> updatePolicy(QueryIterRepeatApply qi, int budget) {
		Set<Binding> result=new HashSet<Binding>();

		if(qi instanceof QueryIterServiceMaintainedCache){
			HashMap<Binding,Long> windowEntriesTS = ((QueryIterServiceMaintainedCache)qi).getCurrentBindingOfWindow();
			if(!(budget<windowEntriesTS.size()))
				return windowEntriesTS.keySet();
			
			
			return ((QueryIterServiceMaintainedCache)qi).getTopKLRUInWindow(windowEntriesTS.keySet(),budget);	
		}
		return null;
	}

}
