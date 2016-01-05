package eu.larkc.csparql.sparql.jena.service.maintenance;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingBase;
import com.hp.hpl.jena.sparql.engine.binding.TimestampedBindingHashMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterCommonParent;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import eu.larkc.csparql.sparql.jena.service.OpServiceCache;
import eu.larkc.csparql.sparql.jena.service.QueryIterServiceCache;

public abstract class QueryIterServiceWSJMaintenance extends QueryIterServiceCache {

	private static Logger logger = LoggerFactory.getLogger(QueryIterServiceWSJMaintenance.class);
	protected Map<Binding, Long> currentBindingsInWindow;
	protected QueryIterator outerContentIterator = null;

	public QueryIterServiceWSJMaintenance(QueryIterator input, OpServiceCache opService, ExecutionContext context) {
		super(input, opService, context);
		currentBindingsInWindow = getCurrentBindingsInWindow(input);
		outerContentIterator = new QueryIterPlainWrapper(currentBindingsInWindow.keySet().iterator());
		maintain();
	}
	private Map<Binding, Long> getCurrentBindingsInWindow(QueryIterator input) {
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
		logger.debug("the window contains {} elements", i);
		//System.out.println(results.keySet().size());
		return results;
	}
	
	protected QueryIterator getInput() {
		return outerContentIterator;
	}
	
	protected abstract void maintain();
	
	@Override
	protected QueryIterator nextStage(Binding outerBinding) {
		Binding key = serviceCache.getKeyBinding(outerBinding);
		Set<Binding> ret = serviceCache.get(key);
		QueryIterator qIter = new QueryIterPlainWrapper(ret.iterator());			
		QueryIterator qIter2 = new QueryIterCommonParent(qIter, outerBinding, getExecContext()) ;
		return qIter2 ;
	}
	
}
