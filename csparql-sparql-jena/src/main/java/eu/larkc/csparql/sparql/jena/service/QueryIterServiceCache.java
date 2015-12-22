package eu.larkc.csparql.sparql.jena.service;

import java.util.HashMap;
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



public class QueryIterServiceCache  extends QueryIterRepeatApply{
	
		private static Logger logger = LoggerFactory.getLogger(QueryIterServiceCache.class);
		private CacheAcqua serviceCache;
		private OpService opService;
		
		public QueryIterServiceCache(QueryIterator input, OpServiceCache opService, ExecutionContext context){
			super(input, context) ;
			serviceCache = ((OpServiceCache) opService).getCache();
			this.opService = opService ;		
		}


		@Override
		protected QueryIterator nextStage(Binding outerBinding) {
			//check if the outerBinding hits the cache
			Binding key = serviceCache.getKeyBinding(outerBinding);
			if(!serviceCache.contains(key)){
				logger.debug(key+" windows entry without matching entry in cache! fetching from service URL!");
				Op op = QC.substitute(opService, outerBinding) ;
		        QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

		        Set<Binding> values = new HashSet<Binding>();
		        while(qIter.hasNext()){
		        	Binding b = qIter.nextBinding();
		        	values.add(serviceCache.getValueBinding(b));
		        }
		        	serviceCache.put(key, values);
		        
			} else logger.debug(key+" windows entry found matching entry in cache");
			
			Set<Binding> ret = serviceCache.get(key);
			QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
			QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
			return qIter2 ;

		}
	}