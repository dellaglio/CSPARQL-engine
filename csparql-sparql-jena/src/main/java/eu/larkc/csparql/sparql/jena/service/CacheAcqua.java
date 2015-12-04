package eu.larkc.csparql.sparql.jena.service;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.atlas.lib.cache.CacheLRU;
import org.apache.jena.atlas.lib.cache.CacheSetLRU;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingProjectNamed;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;


//possible K: Binding, long
//possible V: Binding, Set<Binding>, List<Binding>, ???

public class CacheAcqua extends CacheLRU<Binding,Set<Binding>> {

	public static final CacheAcqua INSTANCE = new CacheAcqua();

	
	private List<Var> keys; 
	private List<Var> values;
	public CacheAcqua(){
		super(0.8f, 1000);
		
	}
	public void init( QueryRunner qr) {
		keys=qr.computeCacheKeyVars();
		values=qr.computeCacheValueVars();
		/*TODO
		 * here I temporarily fill the cache according to this query and keyvars=?S and value vars ?P2,?O2
		 *  
		final String querySERVICE = "REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 10] WHERE { ?S ?P ?O "
		   		+"SERVICE <http://localhost:3030/test/sparql> {?S ?P2 ?O2}"
				+ "}";
				
		but it should be actually filled from 
		remote data provider according to query*/ 	 
		fillCache(qr.getQuery(),qr.getSERVICEEndpointURI());
		
	}
	
	private void fillCache(Query query,List<String> endpoints) {
		QueryExecution qe = QueryExecutionFactory.sparqlService(
				endpoints.get(0), query);//TODO: for the moment we assume that there is only one service cluase in the query
		ResultSet as = qe.execSelect();
		for (; as.hasNext();) {
			QuerySolution qs = as.nextSolution();

			BindingProjectNamed solb = (BindingProjectNamed) BindingUtils
					.asBinding(qs);
			
			put(solb);
		}		
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
	
	public Set<Binding> get(Binding key){
		return super.get(key);
	}
	public Set<Binding> put(Binding key, Set<Binding> value){
		return super.put(key, value);
	}
	public Set<Binding> put(Binding b){
		Binding keyBm = getKeyBinding(b);
		Binding valueBm = getValueBinding(b);
		
		Set<Binding> prevBinding=super.get(keyBm);
		if(prevBinding!=null){
			prevBinding.add(valueBm);
		return super.put(keyBm, prevBinding);
		}else{
			HashSet<Binding> bhs=new HashSet<>();
			bhs.add(valueBm);
			return super.put(keyBm, bhs);
		}
		
	}

}
