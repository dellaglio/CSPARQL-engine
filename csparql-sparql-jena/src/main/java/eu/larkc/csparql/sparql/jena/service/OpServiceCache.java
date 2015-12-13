package eu.larkc.csparql.sparql.jena.service;
import eu.larkc.csparql.common.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.syntax.ElementService;


public class OpServiceCache extends OpService {
	private static Logger logger = LoggerFactory.getLogger(OpServiceCache.class);
	private CacheAcqua cache;

	public OpServiceCache(Node serviceNode, Op subOp, boolean silent) {
		super(serviceNode, subOp, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize());
		//TODO: set keyVars, valueVars 
    	logger.debug("OpServiceCache instantiated!!!");
	}
	
    public OpServiceCache(Node serviceNode, Op subOp, ElementService elt, boolean silent){
    	super(serviceNode, subOp, elt, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize());
		logger.debug("OpServiceCache instantiated!!!");
    }
	
	@Override
	public Op1 copy(Op newOp) {
		OpServiceCache ret = new OpServiceCache(getService(), getSubOp(), getServiceElement(), getSilent());
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
