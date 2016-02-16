package eu.larkc.csparql.sparql.jena.service.maintenance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.binding.TimestampedBindingHashMap;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.sparql.engine.http.Service;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.Symbol;

import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.sparql.jena.service.CacheAcqua;
import eu.larkc.csparql.sparql.jena.service.OpServiceCache;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.MaintenancePolicy;

public class QueryIterServiceMaintainedCache extends QueryIterRepeatApply {

	private static Logger logger = LoggerFactory.getLogger(QueryIterServiceMaintainedCache.class);
	private CacheAcqua serviceCache;
	private OpService opService;

	/*private List<Vertex> currentVertexInWindow;
	private BiadjacencyMatrix subgraphOfCurrentWindow;
	private List<Vertex> expriedVsInCurrentWindow;
	private BiadjacencyMatrix subgraphForUpating;*/

	private static int width;
	private static int slide;
	private static long tnow;

	QueryIterator outerContentIterator = null;

	Set<Binding> electedList = new HashSet<Binding>();
	public MaintenancePolicy mypolicy = null;

	private HashMap<Binding, Long> currentBindingsInWindow;

	public HashMap<Binding, Long> getCurrentBindingOfWindow(){
		return currentBindingsInWindow;
	}	

	public List<Binding> getExpiredWindowBindings() {
		List<Binding> result = new ArrayList<Binding>();
		for (Binding curV : currentBindingsInWindow.keySet()) {
			//int originalID = curV.originalVertex.getIntID();
			if (isExpired(curV)==1)
				result.add(curV);

		}
		return result;
	}
	public List<Binding> getExpiredOrNotCachedWindowBindings() {
		List<Binding> result = new ArrayList<Binding>();
		for (Binding curV : currentBindingsInWindow.keySet()) {
			//int originalID = curV.originalVertex.getIntID();
			if (isExpired(curV)!=0)//either 1 (i.e., expired) or -1 (i.e., not cached)
				result.add(curV);

		}
		return result;
	}
	
	public List<Binding> getExpiredBindings(Collection<Binding> collection) {
		List<Binding> result = new ArrayList<Binding>();
		for (Binding curV : collection) {
			//int originalID = curV.originalVertex.getIntID();
			if (isExpired(curV)==1)
				result.add(curV);

		}
		return result;
	}



	protected int isExpired(Binding key) {

		try{long bbt = serviceCache.getCacheBBT().get(serviceCache.getKeyBinding(key));
		if (bbt <= tnow) {
			return 1;
		} else {
			return 0;//fresh
		}
		}catch(Exception e){logger.error(key + " is not cached "+serviceCache.getKeyBinding(key)); return -1;}
	}

	private HashMap<Binding, Long> getCurrentBindingsInWindow(QueryIterator input) {
		HashMap<Binding, Long> results = new HashMap<Binding, Long>();
		int i = 0;
		while (input.hasNext()) {
			Binding b = input.next();
			BindingBase b1 = (BindingBase) b;
			Long t = null;
			while (b1 instanceof TimestampedBindingHashMap || b1.getParent() != null) {
				if (b1 instanceof TimestampedBindingHashMap) {
					t = ((TimestampedBindingHashMap) b1).getMaxTimestamp();
					Long prevT = results.get(b);
					if (prevT == null)
						results.put(b, t);
					else
						results.put(b, Math.min(prevT, t));
					break;
				}
				b1 = (BindingBase) b1.getParent();
			}
			i++;
		}
		//logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+i);
		//System.out.println(results.keySet().size());
		return results;
	}

	public QueryIterServiceMaintainedCache(QueryIterator input, OpServiceCache opService, ExecutionContext context){
		super(input, context) ;
		serviceCache = opService.getCache();
		this.opService = opService ;	
		Context ec = context.getContext();
		QueryIterServiceMaintainedCache.width = Integer.parseInt(ec.getAsString(Symbol.create("acqua:width")));
		QueryIterServiceMaintainedCache.slide = Integer.parseInt(ec.getAsString(Symbol.create("acqua:slide")));
		QueryIterServiceMaintainedCache.tnow = Long.parseLong(ec.getAsString(Symbol.create("acqua:tnow")));
		currentBindingsInWindow = getCurrentBindingsInWindow(input);
		//logger.debug("filled currentbinding array with window content. size is "+currentBindingsInWindow.size());
		outerContentIterator = new QueryIterPlainWrapper(currentBindingsInWindow.keySet().iterator());

	}

	public void executePolicy() {
		Iterator<Binding> allCacheKeysIt= serviceCache.keys();
		HashSet<Binding> A = new HashSet<Binding>();//fresh non-compatible cache subset
		HashSet<Binding> B = new HashSet<Binding>();//stale non-compatible cache subset
		while(allCacheKeysIt.hasNext()){
			Binding cachedKey = allCacheKeysIt.next();
			HashMap<Binding, Long> BbtOfServiceCache = serviceCache.getCacheBBT();
			if(currentBindingsInWindow.get(cachedKey)==null){//non-compatible=cached entry is not in windows
				if(BbtOfServiceCache.get(cachedKey)>tnow) //fresh non-compatible cache entry A
					A.add(cachedKey);
				else B.add(cachedKey);					
			}
		}
		Iterator<Binding> staleNonCompatibleCacheEntryIt=B.iterator();
		Iterator<Binding> freshNonCompatibleCacheEntryIt = A.iterator();
		logger.debug(">>>>>>>>>>>>>>>>>>>currentBindingsInWindow "+currentBindingsInWindow);
		electedList = this.mypolicy.updatePolicy(this, Config.INSTANCE.getBudget());
		logger.debug("???????????????????elected List "+electedList);
		
		Set<Binding> resultToUpdateInCache = new HashSet<Binding>();
		int totalTripleUpdatedInCache = 0;

		for (Binding b : electedList) {
			//logger.debug("??????????????????????????????????????????????"+b);
			Set<Binding> tempResults = MaintainKey(b);
			
			resultToUpdateInCache.addAll(tempResults);
			Set<Binding> tempBsforUpdate = serviceCache.get(serviceCache.getKeyBinding(b));
			if (tempBsforUpdate.size() != tempResults.size()) {
				throw new RuntimeException("querying results number is not equal to the results number in local");
			}
			//serviceCache.updateBBT(b, tnow);	
			Set<Binding> currentCachedValue = serviceCache.get(serviceCache.getKeyBinding(b));
			if( currentCachedValue!=null){//maintain a compatible mapping in cache
				/*if(currentCachedValue!=tempResults)
					serviceCache.adaptChangeRate(serviceCache.getKeyBinding(b));*/	//this part is adaptive part to be investigated later			
				serviceCache.put(serviceCache.getKeyBinding(b), tempResults,tnow);
			}else{//replace a non-compatible mapping of cache with a new compatible mapping for a window mapping
				Binding evictedKey=null;
				if(staleNonCompatibleCacheEntryIt.hasNext())
					evictedKey=staleNonCompatibleCacheEntryIt.next();
				else if(freshNonCompatibleCacheEntryIt.hasNext())
					evictedKey=freshNonCompatibleCacheEntryIt.next();
				else logger.error("size of window is larger than size of cache!! no key to evict, incomplete response will be produced");
				serviceCache.remove(evictedKey);
				//the change rate of the new cache entry is max_value
				int newCR=getCR(b);
				serviceCache.put(serviceCache.getKeyBinding(b), tempResults,tnow,newCR);
			}
			totalTripleUpdatedInCache += tempResults.size();
		}	

	}
	

	private int getCR(Binding b) {
		Binding k= serviceCache.getKeyBinding(b);
		String query = "PREFIX omv: <http://omv.ontoware.org/2005/05/ontology#> " +
				   "SELECT ?changeRate " +
				   "WHERE { k.get(Var.alloc(serviceCache.getKeyVars().iterator().next().getVarName())) <http://example.org/changeRate> ?changeRate. " +
				   "}";
		Query jquery = QueryFactory.create(query) ;

		 QueryEngineHTTP qexec = QueryExecutionFactory.createServiceRequest("http://sparql.bioontology.org/sparql", jquery);
		 ResultSet results = qexec.execSelect() ;
		 if(results.hasNext())
		 {QuerySolution soln = results.nextSolution() ;
		 Literal cr = soln.getLiteral("changeRate") ;
		       return cr.getInt();
		    }else
		    	logger.error("no change rate for key "+k);
		// TODO Auto-generated method stub
		return 0;
	}

	private Set<Binding> MaintainKey(Binding outerBinding) {
		//logger.debug("maintaining "+outerBinding);
		Op op = QC.substitute(opService, outerBinding) ;
		QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

		Set<Binding> values = new HashSet<Binding>();
		while(qIter.hasNext()){
			Binding b = qIter.nextBinding();
			values.add(serviceCache.getValueBinding(b));
		}
		return values;
		
	}
	public Binding getkeyBinding(Binding b){
		return serviceCache.getKeyBinding(b);
	}
	public boolean isCached(Binding b){
		return serviceCache.contains(serviceCache.getKeyBinding(b));
	}
	protected QueryIterator getInput() {
		return outerContentIterator;
	}
	@Override
	protected QueryIterator nextStage(Binding outerBinding) {
		//check if the outerBinding hits the cache
		Binding key = serviceCache.getKeyBinding(outerBinding);
		if(!serviceCache.contains(key)){
			logger.error(key+" windows entry without matching entry in cache! no budget is left to update! we assume it has no binding!");
			/*Op op = QC.substitute(opService, outerBinding) ;
	        QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

	        Set<Binding> values = new HashSet<Binding>();
	        while(qIter.hasNext()){
	        	Binding b = qIter.nextBinding();
	        	values.add(serviceCache.getValueBinding(b));
	        }
	        //if(values.size()!=0)
			 */	        	
			return null;
			//serviceCache.put(key, MaintainKey(outerBinding));

		}// else logger.debug(key+" windows entry found matching entry in cache");

		Set<Binding> ret = serviceCache.get(key);
		QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;

	}



	public Set<Binding> getTopKLRUInWindow(Set<Binding> keySet, int budget) {		
		return serviceCache.getTopKLRU(keySet,budget);		
	}

	public Set<Binding> getTopKGLRU(int budget) {		
		return serviceCache.getGLRUTopK(budget);		
	}

	public static long getTnow() {
		return tnow;
	}

	public HashMap<Binding, Integer> getChangeRate() {
		return serviceCache.getCacheChangeRate();
	}

	public static int getSlideLength(){
		return slide;
	}

	public static int getWindowLength(){
		return width;
	}

	public long getBBT(Binding originalID) {
		// TODO Auto-generated method stub
		return serviceCache.getCacheBBT().get(originalID);
	}

}

