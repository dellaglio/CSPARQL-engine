package eu.larkc.csparql.sparql.jena.service.maintenance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.binding.TimestampedBindingHashMap;
import com.hp.hpl.jena.sparql.engine.http.Service;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.util.Context;

import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.sparql.jena.service.CacheAcqua;
import eu.larkc.csparql.sparql.jena.service.OpServiceCache;
import eu.larkc.csparql.sparql.jena.service.QueryIterServiceCache;
import eu.larkc.csparql.sparql.jena.service.maintenance.policies.MaintenancePolicy;

public class QueryIterServiceMaintainedCache extends QueryIterRepeatApply {

	private static Logger logger = LoggerFactory.getLogger(QueryIterServiceCache.class);
	private CacheAcqua serviceCache;
	private OpService opService;
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
		logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+i);
		//System.out.println(results.keySet().size());
		return results;
	}
	
	public QueryIterServiceMaintainedCache(QueryIterator input, OpServiceCache opService, ExecutionContext context){
		super(input, context) ;
		serviceCache = opService.getCache();
		this.opService = opService ;	
		Context ec = context.getContext();
		/*QueryIterServiceMaintainedCache.width = Integer.parseInt(ec.getAsString(Symbol.create("acqua:width")));
		QueryIterServiceMaintainedCache.slide = Integer.parseInt(ec.getAsString(Symbol.create("acqua:slide")));
		QueryIterServiceMaintainedCache.tnow = Long.parseLong(ec.getAsString(Symbol.create("acqua:tnow")));
		*/currentBindingsInWindow = getCurrentBindingsInWindow(input);
		logger.debug("filled currentbinding array with window content. size is "+currentBindingsInWindow.size());
		outerContentIterator = new QueryIterPlainWrapper(currentBindingsInWindow.keySet().iterator());
		
	}

	public void executePolicy() {
		logger.debug(">>>>>>>>>>>>>>>>>>>"+currentBindingsInWindow.size());
		electedList = this.mypolicy.updatePolicy(this, Config.INSTANCE.getBudget());
		Set<Binding> resultToUpdateInCache = new HashSet<Binding>();
		int totalTripleUpdatedInCache = 0;
		
		
		for (Binding b : electedList) {
			Set<Binding> tempResults = MaintainKey(b);
			resultToUpdateInCache.addAll(tempResults);
			Set<Binding> tempBsforUpdate = serviceCache.get(b);

			if (tempBsforUpdate.size() != tempResults.size()) {
				throw new RuntimeException("querying results number is not equal to the results number in local");
			}
			
			//serviceCache.updateBBT(b, tnow);

			serviceCache.put(b, tempResults);
			totalTripleUpdatedInCache += tempResults.size();

		}	

	}
	

	private Set<Binding> MaintainKey(Binding outerBinding) {
		Op op = QC.substitute(opService, outerBinding) ;
        QueryIterator qIter = Service.exec((OpService)op, getExecContext().getContext()) ;

        Set<Binding> values = new HashSet<Binding>();
        while(qIter.hasNext()){
        	Binding b = qIter.nextBinding();
        	values.add(serviceCache.getValueBinding(b));
        }
        return values;
	}
	protected QueryIterator getInput() {
		return outerContentIterator;
	}
	@Override
	protected QueryIterator nextStage(Binding outerBinding) {
		//check if the outerBinding hits the cache
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
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
	        
		} else logger.debug(key+" windows entry found matching entry in cache");
		
		Set<Binding> ret = serviceCache.get(key);
		QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;

	}
}

