package   com.hp.hpl.jena.sparql.engine.binding;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.binding.TimestampedBindingHashMap;

public class TimestampedBindingHashMap extends BindingHashMap {
//	protected Set<Long> timestamps = new HashSet<Long>();
	protected Long min, max;
//	protected long timestamp;
	
	public TimestampedBindingHashMap(BindingHashMap hashMap){
		super(hashMap.parent);
		map = hashMap.map;
		if(hashMap.parent instanceof TimestampedBindingHashMap){
			min=((TimestampedBindingHashMap) hashMap.parent).getMinTimestamp();
			max=((TimestampedBindingHashMap) hashMap.parent).getMaxTimestamp();
		}
		else{
			min = Long.MAX_VALUE;
			max = 0l;
		}
	}
	
	public void addTimestamp(long timestamp){
//		this.timestamp = timestamp;
		if(max < timestamp)
			max=timestamp;
		if(min > timestamp)
			min=timestamp;
	}
	
	public long getMinTimestamp(){
		return min;
	}
	
	public long getMaxTimestamp(){
		return max;
	}
	
//	public long getTimestamp(){
//		return timestamp;
//	}
	
	@Override
	public String toString() {
		return super.toString() + 
				"<"+min+","+
//				timestamp+","+
				max+">";
	}
}
