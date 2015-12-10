package eu.larkc.csparql.cep.api;

import java.util.Random;

public class TestGeneratorFromInput extends RdfStream{
	private long[] timestamps;

	private final String subj = "http://example.org/S";
	private final String[] pred = new String[] {"http://example.org/like","http://example.org/mentioned"};

	private final String obj = "http://example.org/O";
	public TestGeneratorFromInput(String iri, long[] timestamps){
		super(iri);
		this.timestamps = timestamps;
	}
	public long[] getTimeStamps(){
		return timestamps;
	}
	public void run(){

		int i = 0;
		for(Long timestamp : timestamps){
			String c;
			/*if(i<10)
				c="0"+i++;
			else*/
			c=""+i;

			RdfQuadruple tempQ = new RdfQuadruple(subj+((i%2)+1)+"_"+c, pred[i%2], obj+c, timestamp);
			System.out.println("streamed>>> "+subj+((i%2)+1)+"_"+c+ pred[i%2]+ obj+c+ timestamp);
			this.put(tempQ);
			i++;
		}		
	}
	//this function intends to produce streams where the object can match the subject of a fuseki server content
	public void runCacheTestOneQueryMultipleSERVICEOneStream(){
		Random r= new Random();
		int i = 0;
		for(Long timestamp : timestamps){
			String c;
			/*if(i<10)
				c="0"+i++;
			else*/
			c=""+i;

			RdfQuadruple tempQ = new RdfQuadruple(subj+((i%2)+1)+"_"+c, pred[i%2], subj+(r.nextInt(2)+1)+"_"+c, timestamp);
			System.out.println("streamed>>> "+subj+((i%2)+1)+"_"+c+ pred[i%2]+ "_"+subj+(r.nextInt(2)+1)+"_"+c+ timestamp);
			this.put(tempQ);
			i++;
		}		
	}
}

