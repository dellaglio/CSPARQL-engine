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
import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.GLRUMaintenance;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.LRUMaintenance;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.RandomMaintenance;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.WBMMaintenance;

public class OpExecutorAcqua extends OpExecutor {
	private static Logger logger = LoggerFactory.getLogger(OpExecutorAcqua.class);


	protected OpExecutorAcqua(ExecutionContext execCxt) {
		super(execCxt);

	}

	@Override
	protected QueryIterator execute(OpService opService, QueryIterator input) {
		//		System.out.println("window triggered for "+execCxt.getContext().getAsString(Symbol.create("http://jena.hpl.hp.com/ARQ/system#query")));
		if(opService instanceof OpServiceCache){
			if (Config.INSTANCE.isJenaCacheUsingMaintenance()){
				if(true){// TODO: check if it is a 1-1 mapping
					//logger.debug("????????????????????????????????????????????????????????????        i arrived the maintenance step");
					QueryIterServiceMaintainedCache mc=new QueryIterServiceMaintainedCache(input, (OpServiceCache)opService, execCxt);
					//mc.readChangeRatesForMaintenance();
					switch (Config.INSTANCE.getMaintenanceType()) {
					case "fifo":
					{
						//mc.mypolicy=new 										
					}
					case "wsj-random":
					{
						mc.mypolicy=new RandomMaintenance();	
						break;
					}
					case "lru":
					{
						mc.mypolicy=new LRUMaintenance();
						break;
					}
					case "global-lru":
					{
						mc.mypolicy=new GLRUMaintenance();
						break;
					}
					case "wbm":{
						mc.mypolicy=new WBMMaintenance();
						break;
					}
					}
					mc.executePolicy();
					return mc;
					}else{
						/*
						 * QueryIterServiceMaintainedMNCache mnc=new QueryIterServiceMaintainedMNCache(input, (OpServiceCache)opService, execCxt);
						
						switch among execution policies
						execute policy
						return mnc*/
						return null;
					}
			}else{
				return new QueryIterServiceCache(input, (OpServiceCache) opService, execCxt) ;
			}


		}else
			return new QueryIterService(input, opService, execCxt) ;
	}

}


