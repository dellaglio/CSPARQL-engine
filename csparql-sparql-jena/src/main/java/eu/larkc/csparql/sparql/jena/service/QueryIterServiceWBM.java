package eu.larkc.csparql.sparql.jena.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.atlas.io.IndentedWriter;

import com.hp.hpl.jena.query.QueryExecException;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRepeatApply ;
import com.hp.hpl.jena.sparql.engine.http.Service ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryIterServiceWBM extends QueryIterRepeatApply {
	public static Logger logger = LoggerFactory.getLogger(QueryIterServiceWBM.class);
	
	//variables 
	private HashMap<Binding, Long> cacheBBT;
	private HashMap<Binding,Integer> changeRate;
	//statistic variables
	public static int callCount=0;
	public static long totalTimeNext=0;
	public static long totalTimeCons=0;
	//window variables
	private OpService opService ;
	private long tnow;
	private Set<Binding> outerContent = null;
	private HashMap<Binding,Double> outerContentL=null;
	private QueryIterator outerContentIterator = null;
	private Set<Binding> electedList = new HashSet<Binding>();
	
	public QueryIterServiceWBM(QueryIterator input, OpService opService, ExecutionContext context)
	{
		super(input, context);
		callCount=0;
		totalTimeNext=0;
		totalTimeCons=0;
		long start=System.currentTimeMillis();
		
		/*if ( context.getContext().isFalse(Service.serviceAllowed) )
			throw new QueryExecException("SERVICE not allowed") ; 
		this.opService = opService ;
		

		int slide=0,width=0;
		//fill the window content
		//computing L value
*/		
		
	}

	@Override
	protected QueryIterator nextStage(Binding arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
