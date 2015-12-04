package eu.larkc.csparql.utils;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class TestGeneratorFromInput extends RdfStream{
	private long[] timestamps;
	
	private final String subj = "http://example.org/S";
	private final String pred = "http://example.org/P";
	private final String obj = "http://example.org/O";
	
	public TestGeneratorFromInput(String iri, long[] timestamps){
		super(iri);
		this.timestamps = timestamps;
	}
	
	public void run(){
		int i = 1;
		for(Long timestamp : timestamps){
			String c;
			/*if(i<10)
				c="0"+i++;
			else*/
				c=""+i++;
				
			RdfQuadruple tempQ = new RdfQuadruple(subj+c, pred+c, obj+c, timestamp);
			this.put(tempQ);
		}
	}
}

