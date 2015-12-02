package eu.larkc.csparql.ui.cache;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;

public class OpExecutorAcqua extends OpExecutor {
	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);
	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
		System.out.println("Catched!");
		return super.execute(opService, input);
	}
	
	
}