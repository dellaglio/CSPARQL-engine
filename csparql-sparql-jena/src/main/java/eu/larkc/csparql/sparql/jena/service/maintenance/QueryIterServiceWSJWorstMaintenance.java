package eu.larkc.csparql.sparql.jena.service.maintenance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;

import eu.larkc.csparql.sparql.jena.service.OpServiceCache;

public class QueryIterServiceWSJWorstMaintenance extends QueryIterServiceWSJMaintenance {

	private static Logger logger = LoggerFactory.getLogger(QueryIterServiceWSJWorstMaintenance.class);
	public QueryIterServiceWSJWorstMaintenance(QueryIterator input, OpServiceCache opService, ExecutionContext context) {
		super(input, opService, context);
	}

	@Override
	protected void maintain() {
		// TODO Auto-generated method stub
		
	}

}
