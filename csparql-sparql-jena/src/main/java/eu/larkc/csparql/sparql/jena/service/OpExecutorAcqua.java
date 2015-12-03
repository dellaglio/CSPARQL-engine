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

import eu.larkc.csparql.common.config.Config;



public class OpExecutorAcqua extends OpExecutor {
	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);
	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
		//System.out.println("Catched!");
		Set<Binding> outerContent = new HashSet<Binding>();
		while(input.hasNext()){
			Binding b = input.next();
			//System.out.println(b);
			outerContent.add(b);
		}
		QueryIterator outerContentIterator =  new QueryIterPlainWrapper(outerContent.iterator());
		// we should automatically identify if it is a SBM policy or 1-1 mapping
		switch(Config.INSTANCE.getJenaCachingTypeForService()){
		case "csparql":
			return new QueryIterService(outerContentIterator, opService, execCxt) ;
		case "WBM":
			return new QueryIterServiceWBM(input, opService, execCxt) ;
			/*case "cache":
			return new QueryIterServiceCache(input, opService, execCxt) ; 
		case "rand":
			return new QueryIterServiceRand(input, opService, execCxt) ; 
		case "global":
			return new QueryIterServiceGlobal(input, opService, execCxt) ; 
		case"LRU":
			return new QueryIterServiceLRU(input, opService, execCxt) ; 
		case "LWLRU":
			return new QueryIterServiceLWLRU(input, opService, execCxt) ; 
		case "BST":
    		return new QueryIterServiceBST(input, opService, execCxt) ; 
		case "WST":
			return new QueryIterServiceWST(input, execCxt) ; 
		case "SBMCsparql":{
			QueryIterServiceSBM tempSBM = new QueryIterServiceSBM(input, opService, execCxt);
			tempSBM.mypolicy = new SBMCsparql();
			tempSBM.executePlicy();}
		case "SBMBGP":{
			QueryIterServiceSBM tempSBM = new QueryIterServiceSBM(input, opService, execCxt);

		}
				if (QueryExecUtils.moe.contains("Flexible") && QueryExecUtils.moe.contains("BGP") && QueryExecUtils.moe.contains("IBM")) {
					tempSBM.mypolicy = new SBMIBMBGPFlexible();
					tempSBM.executePlicy();
				} else if (QueryExecUtils.moe.contains("BGP") && !QueryExecUtils.moe.contains("IBM")) {
					tempSBM.mypolicy = new SBMBGP();
					tempSBM.executePlicy();
				} else if (QueryExecUtils.moe.contains("Agg")&& !QueryExecUtils.moe.contains("IBM")) {
					tempSBM.mypolicy = new SBMAgg();
					tempSBM.executePlicy();
				} else if (QueryExecUtils.moe.contains("Random")) {
					tempSBM.mypolicy = new SBMRandom();
					tempSBM.executePlicy();
				} else if (QueryExecUtils.moe.contains("LRU")) {
					tempSBM.mypolicy = new SBMLRU();
					tempSBM.executePlicy();
				}else if (QueryExecUtils.moe.contains("Csparql")) {

				}else if (QueryExecUtils.moe.contains("BGP") && QueryExecUtils.moe.contains("IBM")) {
					tempSBM.mypolicy = new SBMIBMBGP();
					tempSBM.executePlicy();
				}
				else if (QueryExecUtils.moe.contains("Agg") && QueryExecUtils.moe.contains("IBM")) {
					tempSBM.mypolicy = new SBMIBMAgg();
					tempSBM.executePlicy();
				} 

				return tempSBM;*/

		default : 
			return super.execute(opService, input);
		}}
}


