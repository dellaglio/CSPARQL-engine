package eu.larkc.csparql.sparql.jena.service;

import java.util.Set;

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
import com.hp.hpl.jena.sparql.util.Symbol;

public class QueryIterServiceCache  extends QueryIterRepeatApply{
	CacheAcqua serviceCache;
	OpService opService;
//	QueryRunner qr;
	
	public QueryIterServiceCache(QueryIterator input, OpService opService, ExecutionContext context)
	{
		super(input, context) ;
//		jenaCache = CacheAcqua.INSTANCE;
		serviceCache = ((OpServiceCache) opService).getCache();
		this.opService = opService ;		
//		qr =(QueryRunner) context.getContext().get(Symbol.create("acqua:runner"));	
	}


	@Override
	protected QueryIterator nextStage(Binding outerBinding) {
		Binding key = qr.getKeyBinding(outerBinding);
		Set<Binding> tmp = serviceCache.get(key);
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
	        	serviceCache.put(qr.getKeyBinding(solb),qr.getValueBinding(solb));
	        } 
	        //jenaCache.printContent();
	        tmp = serviceCache.get(key);
		}else System.out.println(key+" windows entry with matching entry in cache!");
		if (tmp==null) return null;
		QueryIterator qIter = new QueryIterPlainWrapper(tmp.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;
		
	}
}
