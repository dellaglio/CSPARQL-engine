package eu.larkc.csparql.sparql.jena.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.atlas.lib.cache.CacheLRU;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private class TimedKey implements Comparable<TimedKey>{
		Binding key;
		long timeStamp;
		public TimedKey(Binding b, long t){
			key=b;
			timeStamp=t;
		}
		public int compareTo(TimedKey o) {
			final int BEFORE = -1;
		    final int EQUAL = 0;
		    final int AFTER = 1;
			if (this.timeStamp>o.timeStamp)
				return AFTER;
			else if (this.timeStamp<o.timeStamp)
				return BEFORE;
			else return EQUAL;
		}		
	}
//	private int cacheSize=50;
	private static Logger logger = LoggerFactory.getLogger(CacheAcqua.class);
	
	private Set<Var> keys; 
	private Set<Var> values;
	private HashMap<Binding, Long> updateBBT;
	public HashMap<Binding, Integer> cacheChangeRate;
	private HashMap<Binding,Long> lastUpdateTimeOfKey;
	public CacheAcqua(float loadFactor, int maxSize, Set<Var> keys, Set<Var> values){
		super(loadFactor, maxSize);
		this.keys = keys;
		this.values = values;
		updateBBT=new HashMap<Binding, Long>();
		cacheChangeRate=new HashMap<Binding, Integer>();
		lastUpdateTimeOfKey=new HashMap<Binding,Long>();
	}
	
	public CacheAcqua(int cacheSize, Set<Var> keys, Set<Var> values){
		super(0.8f, cacheSize);
		this.keys=keys;
		this.values=values;
		updateBBT=new HashMap<Binding, Long>();		
		cacheChangeRate=new HashMap<Binding, Integer>();
		lastUpdateTimeOfKey=new HashMap<Binding,Long>();
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

	public Set<Var> getKeyVars(){
		return keys;
	}

	public Set<Var> getValueVars(){
		return values;
	}

	public boolean contains(Binding key){
		if (super.containsKey(key)) return true;
		else return false;
	}

	public Binding getKeyBinding(Binding b){
		//logger.debug(">>keys"+keys);
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
		//logger.debug("????????????????????????????????????????????????filling the cache with "+key+"????????"+ value);
		lastUpdateTimeOfKey.put(key, System.currentTimeMillis());
		return super.put(key, value);
	}
	
	public Set<Binding> put(Binding key, Binding value){
		//logger.debug("????????????????????????????????????????????????filling the cache with "+key+"????????"+ value);
		lastUpdateTimeOfKey.put(key, System.currentTimeMillis());
		HashSet<Binding> v=new HashSet<Binding>();
		v.add(value);
		return super.put(key, v);
	}
	
	public Set<Binding> put(Binding b){
		//logger.debug("????????????????????????????????????????????????filling the cache with "+b);
		Binding keyBm = getKeyBinding(b);
		lastUpdateTimeOfKey.put(keyBm, System.currentTimeMillis());
		Binding valueBm = getValueBinding(b);

		Set<Binding> prevBinding=super.get(keyBm);
		if(prevBinding!=null){
			prevBinding.add(valueBm);
			//logger.debug("????????????????????????????????????????????????filling the cache with "+keyBm+"????????"+ prevBinding);
			return super.put(keyBm, prevBinding);
		}else{
			HashSet<Binding> bhs=new HashSet<>();
			bhs.add(valueBm);
			//logger.debug("????????????????????????????????????????????????filling the cache with "+keyBm+"????????"+ bhs);
			return super.put(keyBm, bhs);
		}		
		
	}
	
	/*
	 * get top k for LRU maintenance of the whole cache
	 */	
	public Set<Binding> getGLRUTopK(int budget){
		logger.debug("????????????????????last update time of keys "+lastUpdateTimeOfKey);		
		Set<Binding> result=new HashSet<Binding>();
		Set<Entry<Binding,Long>> set = lastUpdateTimeOfKey.entrySet();
	    List<Entry<Binding,Long>> list = new ArrayList<Entry<Binding,Long>>(
	                set);
		Collections.sort(list, new Comparator<Map.Entry<Binding, Long>>() {
            public int compare(Map.Entry<Binding, Long> o1,
                    Map.Entry<Binding, Long> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
		for (int y=0;y<budget;y++){
			result.add(list.get(y).getKey());
		}
		return result;
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

	public void printLastUpdateTimeOfContent(){
		System.out.println("START PRINTING CACHE CONTENT LAST UPDATE TIME");
		Iterator<Binding> it = lastUpdateTimeOfKey.keySet().iterator();
        while(it.hasNext()){
        	Binding temp = it.next();
        	System.out.println(temp+" "+lastUpdateTimeOfKey.get(temp));
        }
        System.out.println("END PRINTING CACHE CONTENT LAST UPDATE TIME");
		
	}
	private class EntrySet implements Comparable<EntrySet>{

		Binding b;
		long t;
		public EntrySet(Binding key,long lastUpdateTime){
			b=key;
			t=lastUpdateTime;
		}
		Binding getKey(){
			return b;
		}
		@Override
		public int compareTo(EntrySet o) {
			
			return (int)( this.t - o.t);
		}
		
	}
	
	/*
	 * get top k for LRU maintenance for the a subset of cache that has compatible mappings in window
	 */	
	public Set<Binding> getTopKLRU(Set<Binding> keySet, int budget) {
		logger.debug("????????????????????content of window "+keySet);
		logger.debug("????????????????????last update time of keys "+lastUpdateTimeOfKey);
		Iterator<Binding> bIt= keySet.iterator();
		SortedSet<EntrySet> subList = new TreeSet<EntrySet>();
		
		while(bIt.hasNext()){
				Binding curKey = bIt.next();
				long timeOfLastUpdate = lastUpdateTimeOfKey.get(getKeyBinding(curKey));
				subList.add(new EntrySet(getKeyBinding(curKey),timeOfLastUpdate));
			}
		
		Set<Binding> result=new HashSet<Binding>();
		int i=0;
		while(i<budget){
			result.add(subList.first().getKey());
			i++;
		}
		return result;
	}

}
