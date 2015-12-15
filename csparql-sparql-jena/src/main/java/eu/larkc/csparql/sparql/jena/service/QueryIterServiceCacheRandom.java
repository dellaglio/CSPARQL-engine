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
		/*
		 * if update budget is left
		 * 	1)if requested element is not in cache we fetch it from remote and add it to cache
		 * 	2)if requested element is in cache we update a random cache entry
		 * if no update budget is left
		 * 	1)if element is in cache we return it
		 * 	2)if element is not in cache we return null
		 */
		Binding key = serviceCache.getKeyBinding(outerBinding);
		if(budgetUsed<Config.INSTANCE.getBudget()){
			if(!serviceCache.contains(key)){
				Op op = QC.substitute(opService, outerBinding) ;
				QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

				Set<Binding> values = new HashSet<Binding>();
				while(qIter.hasNext()){
					Binding b = qIter.nextBinding();
					values.add(serviceCache.getValueBinding(b));
				}
				serviceCache.put(key, values);
				budgetUsed++;
			}else{
				Binding keyRND= serviceCache.RandomKey();
				Op op = QC.substitute(opService, keyRND) ;
				QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

				Set<Binding> values = new HashSet<Binding>();
				while(qIter.hasNext()){
					Binding b = qIter.nextBinding();
					values.add(serviceCache.getValueBinding(b));
				}
				serviceCache.put(keyRND, values);
				budgetUsed++;
			}
		} else {
			if (!serviceCache.contains(key)){
				logger.warn("element is missing in cache but no budget is left to fetch it! we assume it has no mapping ...");
				return null;
			}else{

			}
		}
		Set<Binding> ret = serviceCache.get(key);
		QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;

	}
}
