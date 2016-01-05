package eu.larkc.csparql.sparql.jena.service.maintenance;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;

import eu.larkc.csparql.sparql.jena.service.OpServiceCache;

public class QueryIterServiceWSJRandomMaintenance extends QueryIterServiceWSJMaintenance{

	public QueryIterServiceWSJRandomMaintenance(QueryIterator input, OpServiceCache opService, ExecutionContext context) {
		super(input, opService, context);
	}

	@Override
	protected void maintain() {
		
		
	}

}
