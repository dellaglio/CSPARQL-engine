package eu.larkc.csparql.sparql.jena.service.maintenance.policies;


import java.util.*;
import java.util.Map.Entry;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;

public class LRUMaintenance implements MaintenancePolicy{

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
