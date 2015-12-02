package eu.larkc.csparql.sparql.jena.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.lib.cache.CacheLRU;
import org.apache.jena.atlas.lib.cache.CacheSetLRU;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

//possible K: Binding, long
//possible V: Binding, Set<Binding>, List<Binding>, ???

public class CacheAcqua extends CacheLRU<Binding,Binding> {

	private List<Var> keys; 
	private List<Var> values;
	
	public CacheAcqua(float loadFactor, int maxSize, List<Var> keyVars, List<Var> valueVars) {
		super(loadFactor, maxSize);
		keys=keyVars;
		values=valueVars;
		
		// TODO Auto-generated constructor stub
	}
	
	public List<Var> getKeyVars(){
		return keys;
	}

	public List<Var> getValueVars(){
		return values;
	}
	
	public boolean contains(Binding key){
		if (super.containsKey(key)) return true;
		else return false;
	}
	
	public Binding getKeyBinding(Binding b){
		Iterator<Var> keyIt = keys.iterator();
		BindingMap keyBm = BindingFactory.create();
		while(keyIt.hasNext()){
			Var TempKey=keyIt.next();
			Node tempValue = b.get(TempKey);
			keyBm.add(TempKey, tempValue);
		}
		return keyBm;
	}
	public Binding getValueBinding(Binding b){
		Iterator<Var> valueIt = values.iterator();
		BindingMap valueBm = BindingFactory.create();
		while(valueIt.hasNext()){
			Var TempKey=valueIt.next();
			Node tempValue = b.get(TempKey);
			valueBm.add(TempKey, tempValue);
		}
		return valueBm;
	}
	
	public Binding get(Binding key){
		return super.get(key);
	}
	public Binding put(Binding key, Binding value){
		return super.put(key, value);
	}
	public Binding put(Binding b){
		Binding keyBm = getKeyBinding(b);
		Binding valueBm = getValueBinding(b);
		
		return super.put(keyBm, valueBm);
		
	}

}
