package eu.larkc.csparql.sparql.jena.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterService;

import eu.larkc.csparql.common.config.Config;
//import com.hp.hpl.jena.sparql.util.Symbol;

public class OpExecutorAcqua extends OpExecutor {
	private static Logger logger = LoggerFactory.getLogger(OpExecutorAcqua.class);
	
	
	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);
		
	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
//		System.out.println("window triggered for "+execCxt.getContext().getAsString(Symbol.create("http://jena.hpl.hp.com/ARQ/system#query")));
		if(opService instanceof OpServiceCache){
			switch (Config.INSTANCE.getMaintenanceType()) {
			case "fifo":
			{
				return new QueryIterServiceCacheFIFO(input, (OpServiceCache) opService, execCxt) ;				
			}
			case "wsj-random":
			{
				return new QueryIterServiceCacheRandom(input, (OpServiceCache) opService, execCxt) ;				
			}
			case "global-lru":
			{
				return new QueryIterServiceCacheLRU(input, (OpServiceCache)opService, execCxt);
			}
			case "no-maintenance":
			default :
				{
					return new QueryIterServiceCache(input, (OpServiceCache) opService, execCxt) ;
				}
			}
		

			
	}
		return new QueryIterService(input, opService, execCxt) ;
    }

}


