package eu.larkc.csparql.sparql.jena.ext;

import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class Timestamps {
	public static final Timestamps INSTANCE =  new Timestamps();
	
	private Map<Statement,Long> timestamps;

	private Timestamps(){
		timestamps = new HashMap<Statement,Long>();
	}
	
	public boolean contains(Statement key){
		return timestamps.containsKey(key);
	}
	
	public NodeValue get(Statement key){
		if(contains(key))
			return NodeValue.makeInteger(timestamps.get(key));
		return NodeValue.makeBoolean(false);
	}
	
	public void put(Statement key, Long value){
		timestamps.put(key, value);
	}

	public void init(){
		timestamps.clear();
	}
	
	public void clear(){
		timestamps.clear();
	}

}
