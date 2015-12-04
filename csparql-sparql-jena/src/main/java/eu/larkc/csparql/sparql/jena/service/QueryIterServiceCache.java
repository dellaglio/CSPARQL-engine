package eu.larkc.csparql.sparql.jena.service;

import java.util.Set;

import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;

public class QueryIterServiceCache  extends QueryIterRepeatApply{
	CacheAcqua jenaCache;
	OpService opService ;
	
	public QueryIterServiceCache(QueryIterator input, OpService opService, ExecutionContext context)
	{
		super(input, context) ;
		jenaCache = CacheAcqua.INSTANCE;
		this.opService = opService ;		

	}


	@Override
	protected QueryIterator nextStage(Binding outerBinding)
	{

		Binding key = jenaCache.getKeyBinding(outerBinding);
		Set<Binding> tmp = jenaCache.get(key);
		if(tmp!=null){
			QueryIterator qIter = new QueryIterPlainWrapper(tmp.iterator());			
			QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
			return qIter2 ;
		}
		else{
			System.out.println(key+" windows entry without matching entry in cache!!!");
			return null;

		}
	}
}
