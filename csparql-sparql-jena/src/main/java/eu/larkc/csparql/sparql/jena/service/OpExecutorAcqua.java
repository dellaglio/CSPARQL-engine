package eu.larkc.csparql.sparql.jena.service;


import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.iterator.QueryIterService;



public class OpExecutorAcqua extends OpExecutor {

	
	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);
		
	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
		System.out.println("Catched!");
		
		
		
		/*Set<Binding> outerContent = new HashSet<Binding>();
		while(input.hasNext()){
			Binding b = input.next();
			//System.out.println(b);
			outerContent.add(b);
		}
		QueryIterator outerContentIterator =  new QueryIterPlainWrapper(outerContent.iterator());
		*/
		//if (QueryExecUtils.moe.equalsIgnoreCase("csparql"))
    		//return new QueryIterService(input, opService, execCxt) ;
		return new QueryIterServiceCache(input, opService, execCxt) ;
    		
    		
    	/*if (QueryExecUtils.moe.equalsIgnoreCase("WBM")) {
    		return new QueryIterServiceWBM(input, opService, execCxt) ;
    	}
    	if (QueryExecUtils.moe.equalsIgnoreCase("cache")) return new QueryIterServiceCache(input, opService, execCxt) ; 
    	if (QueryExecUtils.moe.equalsIgnoreCase("rand")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceRand(input, opService, execCxt) ; 
    	}
    	if (QueryExecUtils.moe.equalsIgnoreCase("global")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceGlobal(input, opService, execCxt) ; 
    	}
    	if (QueryExecUtils.moe.equalsIgnoreCase("LRU")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceLRU(input, opService, execCxt) ; 
    	}if (QueryExecUtils.moe.equalsIgnoreCase("LWLRU")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceLWLRU(input, opService, execCxt) ; 
    	}if (QueryExecUtils.moe.equalsIgnoreCase("BST")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceBST(input, opService, execCxt) ; 
    	}if (QueryExecUtils.moe.equalsIgnoreCase("WST")) {
    		//if(QueryExecUtils.updateBudget>QueryExecUtils.windowlength) return null;
    		return new QueryIterServiceWST(input, execCxt) ; 
    	}
    	else return null;
		
		return super.execute(opService, input);*/
	}
	
	
}