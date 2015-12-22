package eu.larkc.csparql.sparql.jena.service.maintenance.policies;

import java.util.HashMap;
import java.util.Set;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;


public interface MaintenancePolicy {
	Set<Binding> updatePolicy( QueryIterRepeatApply sbm, int budget);
}
