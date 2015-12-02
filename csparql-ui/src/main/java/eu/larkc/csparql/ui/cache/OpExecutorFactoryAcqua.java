package eu.larkc.csparql.ui.cache;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.OpExecutorFactory;

public class OpExecutorFactoryAcqua implements OpExecutorFactory{

	@Override
	public OpExecutor create(ExecutionContext execConcext) {
		return new OpExecutorAcqua(execConcext);
	}

}
