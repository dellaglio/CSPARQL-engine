package eu.larkc.csparql.sparql.jena.service;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.syntax.ElementService;

import eu.larkc.csparql.common.config.Config;

public class OpServiceCache extends OpService {
	private CacheAcqua cache;

	public OpServiceCache(Node serviceNode, Op subOp, boolean silent) {
		super(serviceNode, subOp, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize());
	}
	
    public OpServiceCache(Node serviceNode, Op subOp, ElementService elt, boolean silent){
    	super(serviceNode, subOp, elt, silent);
		cache = new CacheAcqua(Config.INSTANCE.getJenaServiceCachingSize());
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

}
