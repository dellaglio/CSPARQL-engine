package eu.larkc.csparql.sparql.jena.service;

import java.util.Iterator;
import java.util.Set;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.http.Service;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;
import com.hp.hpl.jena.sparql.engine.main.QC;

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
		if(tmp==null){//if the key is not in cache we retrive it from remote and put it in the cache making sure that tmp is not null
			System.out.println(key+" windows entry without matching entry in cache! fetching from service URL!");
			Op op = QC.substitute(opService, outerBinding) ;
	        QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;
	        QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
	        
	        /*QueryIterator qItercopy = Service.exec((OpService)op, getExecContext().getContext()) ;
	        QueryIterator qIter2copy = new QueryIterCommonParent(qItercopy, outerBinding, getExecContext()) ;
	        // Materialise, otherwise we may have outstanding incoming data.
	        // Alows the server to fulfil the request as soon as possible.
	        // In extremis, can cause a deadlock when SERVICE loops back to this server.
	        QueryIterator returnIt= QueryIter.materialize(qIter2, getExecContext()) ;*/
	        while(qIter2.hasNext())
	        {
	        	Binding solb=qIter2.next();
	        	jenaCache.put(solb);
	        } 
	        jenaCache.printContent();
	        tmp = jenaCache.get(key);
		}
		if (tmp==null) return null;
		QueryIterator qIter = new QueryIterPlainWrapper(tmp.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;
		
	}
}
