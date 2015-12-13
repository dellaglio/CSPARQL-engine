package eu.larkc.csparql.sparql.jena.service;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterService;
//import com.hp.hpl.jena.sparql.util.Symbol;

public class OpExecutorAcqua extends OpExecutor {

	
	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);
		
	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
//		System.out.println("window triggered for "+execCxt.getContext().getAsString(Symbol.create("http://jena.hpl.hp.com/ARQ/system#query")));
		if(opService instanceof OpServiceCache)
			return new QueryIterServiceCache(input, (OpServiceCache) opService, execCxt) ;
		return new QueryIterService(input, opService, execCxt) ;
    }

}


