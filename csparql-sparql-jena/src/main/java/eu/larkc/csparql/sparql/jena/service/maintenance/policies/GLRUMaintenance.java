package eu.larkc.csparql.sparql.jena.service.maintenance.policies;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;

public class GLRUMaintenance implements MaintenancePolicy {

	@Override
	public Set<Binding> updatePolicy(QueryIterRepeatApply qi, int budget) {
		
		if(qi instanceof QueryIterServiceMaintainedCache){
			return ((QueryIterServiceMaintainedCache)qi).getTopKGLRU(budget);	
		}
		return null;
	}

}
