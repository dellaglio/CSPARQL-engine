package eu.larkc.csparql.sparql.jena.service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.atlas.lib.cache.CacheLRU;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingProjectNamed;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;


//possible K: Binding, long
//possible V: Binding, Set<Binding>, List<Binding>, ???
//if we only have one instance of a cacheLRU then for multiple queries, later query will override key Vars and value Vars of the previous query
//and also cache entries have inconsistent key and value vars.
//to retrive values from cache
//therefore one solution for each cache entry we should specify the corresponding key and value vars.
//
public class CacheAcqua extends CacheLRU<Binding,Set<Binding>> {

//	private int cacheSize=50;

	private List<Var> keys; 
	private List<Var> values;
	
	public CacheAcqua(float loadFactor, int maxSize, List<Var> keys, List<Var> values){
		super(loadFactor, maxSize);
		this.keys = keys;
		this.values = values;
	}
	
	public CacheAcqua(int cacheSize){
		super(0.8f, cacheSize);
//		this.cacheSize = cacheSize;
	}
	
	public void init(QueryRunner qr) {
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
//		fillCache(qr);
	}

//	private void fillCache(QueryRunner qr) {
//		
//		for(int i=0;i<qr.extractServiceClauses();i++){
//			OpService opService=qr.getSERVICEEndpointURI().get(i);
//			Node endpoint = opService.getService();
//			//System.out.println("endpoint>>>>>>>"+endpoint.toString()+opService);
//			Query query = OpAsQuery.asQuery(opService.getSubOp());
//			QueryExecution qe = QueryExecutionFactory.sparqlService(
//					endpoint.getURI(), query);
//			ResultSet rs = qe.execSelect();	
//			while (rs.hasNext() && super.size()!=cacheSize) {
//				QuerySolution qs = rs.next();//nextSolution();
//				BindingProjectNamed solb = (BindingProjectNamed) BindingUtils
//						.asBinding(qs);
//				System.out.println("put in cache>>>>>>>"+solb);
//				put(qr.getKeyBinding(solb),qr.getValueBinding(solb));
//				//printContent();
//			}				
//		}	
//			
//	}

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
	
	public Set<Binding> put(Binding key, Binding value){
		HashSet<Binding> v=new HashSet<Binding>();
		v.add(value);
		return super.put(key, v);
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
	public void printContent(){
		System.out.println("START PRINTING CACHE CONTENT");
		Iterator<Binding> it = super.keys();
        while(it.hasNext()){
        	Binding temp = it.next();
        	System.out.println(temp+" "+super.get(temp));
        }
        System.out.println("END PRINTING CACHE CONTENT");
		
	}

}
