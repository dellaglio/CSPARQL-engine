package eu.larkc.csparql.sparql.jena.service;
import eu.larkc.csparql.common.config.Config;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.BindingProjectNamed;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementService;


public class OpServiceCache extends OpService {
	private static Logger logger = LoggerFactory.getLogger(OpServiceCache.class);
	private CacheAcqua cache;

	public OpServiceCache(Node serviceNode, Op subOp, boolean silent,Set<Var> keys, Set<Var> values) {
		this(serviceNode,subOp,null,silent,keys,values);
		/*super(serviceNode, subOp, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize(),keys,values);
		if (Config.INSTANCE.fillJenaServiceCacheAtStart())
			fillCache(serviceNode, subOp);*/
		logger.debug("OpServiceCache instantiated!!!");
	}

	public OpServiceCache(Node serviceNode, Op subOp, ElementService elt, boolean silent,Set<Var> keys, Set<Var> values){
		super(serviceNode, subOp, elt, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize(),keys,values);
		if (Config.INSTANCE.fillJenaServiceCacheAtStart())
			fillCache(serviceNode, subOp);
		logger.debug("OpServiceCache instantiated!!!");
	}
	private void fillCache(Node endPoint,Op subOp){
		//we should add a LIMIT at the end of service clause to fetch only according to the cache size 
		long length =Config.INSTANCE.getJenaServiceCachingSize();

		Query query = OpAsQuery.asQuery(subOp);

		subOp = new OpSlice(subOp, Long.MIN_VALUE /*query.getOffset()*/ /*start*/, length/*query.getLimit()*//*length*/) ;
		query = OpAsQuery.asQuery(subOp);

		//adding the change rate sub-query to the query from service
		
		
		//logger.debug("query for caching is >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + query);

		
		QueryExecution qe = QueryExecutionFactory.sparqlService(
				endPoint.getURI(), query);
		ResultSet rs = qe.execSelect();	
		while (rs.hasNext() && cache.size()!=Config.INSTANCE.getJenaServiceCachingSize()) {
			QuerySolution qs = rs.next();//nextSolution();
			BindingProjectNamed solb = (BindingProjectNamed) BindingUtils
					.asBinding(qs);
			//logger.debug("put in cache>>>>>>>"+solb);
			cache.put(solb);
			//printContent();
		}				
	}	

	@Override
	public Op1 copy(Op newOp) {
		OpServiceCache ret = new OpServiceCache(getService(), getSubOp(), getServiceElement(), getSilent(),cache.getKeyVars(),cache.getValueVars());
		ret.setCache(cache);
		return ret;
	}

	protected void setCache(CacheAcqua cache){
		this.cache = cache;
	}

	public CacheAcqua getCache(){
		return cache;
	}

	@Override
	public String getName() {
		return "servicec";
	}

}
