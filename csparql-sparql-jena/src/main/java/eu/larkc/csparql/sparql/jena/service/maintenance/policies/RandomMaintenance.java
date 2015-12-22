package eu.larkc.csparql.sparql.jena.service.maintenance.policies;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

import eu.larkc.csparql.sparql.jena.service.QueryIterServiceCache;
import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;

public class RandomMaintenance implements MaintenancePolicy{
	private static Logger logger = LoggerFactory.getLogger(RandomMaintenance.class);
	
	@Override
	public Set<Binding> updatePolicy(QueryIterRepeatApply qi, int budget) {
		Set<Binding> result=new HashSet<Binding>();

		if(qi instanceof QueryIterServiceMaintainedCache){
			HashMap<Binding,Long> windowEntriesTS = ((QueryIterServiceMaintainedCache)qi).getCurrentBindingOfWindow();
			if(!(budget<windowEntriesTS.size()))
				return windowEntriesTS.keySet();
			Iterator<Binding> winIt=windowEntriesTS.keySet().iterator();
			boolean[] flags = new boolean[windowEntriesTS.size()];
			Arrays.fill(flags,Boolean.FALSE);
			if(budget==windowEntriesTS.size())
				Arrays.fill(flags, Boolean.TRUE);
			else
				Arrays.fill(flags,0,budget,Boolean.TRUE);
			Collections.shuffle(Arrays.asList(flags));
			int flagCounter=0;
			while(winIt.hasNext()){
				if(flags[flagCounter])
					result.add(winIt.next());
				else winIt.next();
			}
			return result;

		}
		return null;

	}

}
