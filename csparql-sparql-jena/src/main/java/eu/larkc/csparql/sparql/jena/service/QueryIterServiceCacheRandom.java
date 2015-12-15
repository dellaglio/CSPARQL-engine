package eu.larkc.csparql.sparql.jena.service;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.http.Service;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;
import com.hp.hpl.jena.sparql.engine.main.QC;

import eu.larkc.csparql.common.config.Config;

/*
 * the cache is maintained randomly using a specific update budget (budget is granted based on first-come-first-serve) 
 * doesn't matter if the element is expired or not
 */
public class QueryIterServiceCacheRandom extends QueryIterRepeatApply{
	private static Logger logger = LoggerFactory.getLogger(QueryIterServiceCacheRandom.class);
	private CacheAcqua serviceCache;
	private OpService opService;
	private int budgetUsed;

	public QueryIterServiceCacheRandom(QueryIterator input, OpServiceCache opService, ExecutionContext context){
		super(input, context) ;
		serviceCache = opService.getCache();
		this.opService = opService ;	
		budgetUsed=0;
	}


	@Override
	protected QueryIterator nextStage(Binding outerBinding) {
		Binding key = serviceCache.getKeyBinding(outerBinding);
		if(budgetUsed<Config.INSTANCE.getBudget()){
			//the consumed budget is less than allowed, retrive it from service and add/replace it in cache	regardless if cache has it or not	
			/*if(!serviceCache.contains(key) ){
				//If cache misses the outerBinding 
				logger.debug(key+" windows entry without matching entry in cache! fetching from service URL!");
			 */	
			Op op = QC.substitute(opService, outerBinding) ;
			QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

			Set<Binding> values = new HashSet<Binding>();
			while(qIter.hasNext()){
				Binding b = qIter.nextBinding();
				values.add(serviceCache.getValueBinding(b));
			}
			serviceCache.put(key, values);
			budgetUsed++;
			/*}else {
				logger.debug(key+" windows entry found matching entry in cache");
			}*/

		} 
		Set<Binding> ret = serviceCache.get(key);
		if(ret==null) {
			logger.warn("element is missing in cache but no budget is left to fetch it! we assume it has no mapping ...");
			return null;
		}
		QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;

	}
}
