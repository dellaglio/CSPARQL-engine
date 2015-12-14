package eu.larck.csparql.ui;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Properties;

import org.apache.jena.fuseki.EmbeddedFusekiServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.mem.GraphMem;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;

import eu.larkc.csparql.cep.api.TestGeneratorFromInput;
import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.core.ResultFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.utils.ResultTable;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CacheTestMultipleQueryOneSERVICEOneStream {
	private static Logger logger = LoggerFactory.getLogger(CacheTestMultipleQueryOneSERVICEOneStream.class);
	
	public static class TestRDFTupleResults extends RDFTuple{
		public TestRDFTupleResults(String... values) {
			super.addFields(values);
		}
		public boolean equals(Object object){
			if(object instanceof TestRDFTupleResults || object instanceof RDFTuple){
				RDFTuple temp=((RDFTuple)object);

				for(int m=0;m< temp.toString().split("\t").length;m++){
					if (!temp.get(m).equalsIgnoreCase(this.get(m)))
						return false;
				}
				return true;

			} else {
				return false;
			}
		}
	}
	private static int numberOfInstances=2;
	private static int numberOfFusekiChange=1;//keep track of the object values for testing
	private static int FusekiServerDataSize=20;
	private static EmbeddedFusekiServer[] fuseki=new EmbeddedFusekiServer[numberOfInstances];
	private static DatasetAccessor[] accessor=new DatasetAccessor[numberOfInstances];
	private CsparqlEngine engine;
	private TestGeneratorFromInput streamGenerator;


	@BeforeClass public static void startupFuseki(){
		String[] preds=new String[]{"http://example.org/knows","http://example.org/collegue"};
		//we attach Y+1 to the end of subject and object to reveal the fuseki instance that it hosting them for testing purposes
		//to make sure that cache didn't mixup cache content of 2 queries
		for(int y=0;y<numberOfInstances;y++){
			Graph g = new GraphMem();
			for (int k=0;k<FusekiServerDataSize/2;k++){
				g.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI(preds[y]), 
						NodeFactory.createURI("http://example.org/k"+(y+1))));
				g.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI(preds[(y+1)%2]), 
						NodeFactory.createURI("http://example.org/unrelevant")));
				/*
				 * both fuseki servers conatin both predicates but predicates of first query in second fuseki has unrelevant value and vice versa
				 * so first fuseki have predicate of second query but with unrelevant values and seconf fuseki have predicates offirst query with unrelevant values
				 * and if queries share their caches for key=?S we would have both binding form first and second cache and the results of query 1 would have objects with k2 and 
				 * results of query 2 would have objects with k1 value and tests would fail  
				 */
				
			}

			fuseki[y] = EmbeddedFusekiServer.create(3031+y, DatasetGraphFactory.create(g), "test"+(y+1)); 
			accessor[y] = DatasetAccessorFactory.createHTTP("http://localhost:303"+(y+1)+"/test"+(y+1)+"/data");		
			fuseki[y].start();	
		}

	}

	@BeforeClass public static void initialConfig(){
		Properties prop = new Properties();
		prop.put("esper.externaltime.enabled", true);
		prop.put("jena.service.cache.enabled", true);
		prop.put("jena.service.cache.fillJenaServiceCacheAtStart",true);
		prop.put("jena.service.cache.size",FusekiServerDataSize);
		Config.INSTANCE.setConfigParams(prop);
	}

	@AfterClass public static void shutdownFuseki(){
		for(int y=0;y<fuseki.length;y++)
			fuseki[y].stop();
	}

	/*@Before public void restartFuseki() {
		accessor.getModel().removeAll();
	} */

	@Before public void setup(){
		engine = new CsparqlEngineImpl();
		engine.initialize();
		streamGenerator = new TestGeneratorFromInput("http://myexample.org/stream", input);
	}

	@After public void destroy(){
		//FIXME: concurrent exception
		//		engine.destroy();
	}

	private long[] input;
	private int width, slide;
	private List<List<List<TestRDFTupleResults>>> expected;
	//if we have many queries the expected should be a List of List Of List of RDFTuples--> each query each evaluation resulted in a list of RDFTuple
	//and also the input should be list of long[] if we have multiple streams

	public CacheTestMultipleQueryOneSERVICEOneStream(long[] input, int width, int slide, List<List<List<TestRDFTupleResults>>> expected){
		this.input = input;
		this.width = width;
		this.slide = slide;
		this.expected = new ArrayList();
		for(List<List<TestRDFTupleResults>> i : expected)
			this.expected.add(i);
	}


	@Parameterized.Parameters
	public static Iterable<?> data() {
		return Arrays.asList(
				new Object[][]{
					{//in this test cacse the actual object value in fuseki is (k+100) and in cache is k, so given that the results are k confirms that we use cache
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList( //one per query
								new ArrayList(Arrays.asList( //result ofQuery1 - one per evaluation
										new ArrayList(Arrays.asList( // evaluation 1 
												new TestRDFTupleResults("http://example.org/S1_2","http://example.org/k1")
												)),
										new ArrayList(Arrays.asList( //evaluation2
												new TestRDFTupleResults("http://example.org/S1_4","http://example.org/k1")
												)))),
								new ArrayList(Arrays.asList( //result ofQuery2 - one per evaluation
										new ArrayList(Arrays.asList( // evaluation 1
												new TestRDFTupleResults("http://example.org/S2_1","http://example.org/k2")
												)),
										new ArrayList(Arrays.asList( //evaluation2
												new TestRDFTupleResults("http://example.org/S2_3","http://example.org/k2"),
												new TestRDFTupleResults("http://example.org/S2_1","http://example.org/k2")
												))))))							
					},{//in this test cacse the actual object value in fuseki is (k+200) and in cache is k+100, so given that the results are k+100 confirms that we use cache
						new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList( Arrays.asList( //one per query
								new ArrayList(Arrays.asList( //result ofQuery1 - one per evaluation
										new ArrayList(Arrays.asList(// evaluation 1 
												)),
										new ArrayList(Arrays.asList(// evaluation 2
												new TestRDFTupleResults("http://example.org/S1_2","http://example.org/102_1"))),
										new ArrayList(Arrays.asList( //evaluation 3
												new TestRDFTupleResults("http://example.org/S1_4","http://example.org/104_1")
												)))),
								new ArrayList(Arrays.asList( //result ofQuery2 - one per evaluation
										new ArrayList(Arrays.asList( // evaluation 1
												new TestRDFTupleResults("http://example.org/S2_1","http://example.org/101_2"))),
										new ArrayList(Arrays.asList(// evaluation 2
												new TestRDFTupleResults("http://example.org/S2_3","http://example.org/103_2")
												)),
										new ArrayList(Arrays.asList(// evaluation 3
												new TestRDFTupleResults("http://example.org/S2_5","http://example.org/105_2")
												))))))
					}
				});
	}
	
	
	
	@Test public void mainTestsMultipleCachesForOneStream() {//


		String queryGetAll1 = 
				"REGISTER QUERY PIPPO AS SELECT ?S ?O2 FROM STREAM <http://myexample.org/stream> "
						+ "[RANGE 1s STEP 1s]  "
						+ "WHERE { ?S <http://example.org/like> ?O "
						+"SERVICE <http://localhost:3031/test1/sparql> {?S <http://example.org/knows> ?O2}"
						+ "}";
		String queryGetAll2 = 
				"REGISTER QUERY PIPPO AS SELECT ?S ?O2 FROM STREAM <http://myexample.org/stream> "
						+ "[RANGE 1s STEP 1s]  "
						+ "WHERE { ?S <http://example.org/mentioned> ?O "
						+"SERVICE <http://localhost:3032/test2/sparql> {?S <http://example.org/collegue> ?O2}"
						+ "}";
		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;
		CsparqlQueryResultProxy c2 = null;

		try {
			c1 = engine.registerQuery(queryGetAll1, false);
			//cache will be filled and queried with these vars ?s ?o form server "localhost:3031/test1" for query queryGetAll1
			c2 = engine.registerQuery(queryGetAll2, false);
			//cache will be filled and queried with these vars ?s ?o ?O3 form server "localhost:3032/test2" for query queryGetAll2			
			changeFusekisContent(); 
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c1.addObserver(new ResultFormatter()  {	
			@Override
			public void update(Observable o, Object arg) {
				for(Iterator<RDFTuple> it = ((RDFTable)arg).getTuples().iterator();it.hasNext();){
					RDFTuple tuple = it.next();
					System.out.println("Q1-->"+tuple);
				}
				System.out.println();
			}
		});
		c2.addObserver(new ResultFormatter() {	
			@Override
			public void update(Observable o, Object arg) {
				for(Iterator<RDFTuple> it = ((RDFTable)arg).getTuples().iterator();it.hasNext();){
					RDFTuple tuple = it.next();
					System.out.println("Q2-->"+tuple);
				}
				System.out.println();
			}
		});
		ResultTable formatter1 = new ResultTable();
		ResultTable formatter2 = new ResultTable();
		c1.addObserver(formatter1);
		c2.addObserver(formatter2);
		streamGenerator.run();//stream generator will start to produce triples that are of intereste for both queryGetAll1 and queryGetAll2
		//each will attract their relevant data from stream and process query over it
		List<List<List<RDFTuple>>> actual = new ArrayList<>(Arrays.asList(formatter1.getResults(),formatter2.getResults()));
		//if we change the fuseki content here for the next testcase it will be caching the changed content and the expected results will be different for the second test case
		//System.out.println("output >>>>>>> "+actual);
		for (int j=0;j<expected.size();j++){//per query
			System.out.println("checking query "+j);
			List<List<RDFTuple>> currentQResult=actual.get(j);
			for(int i = 0; i<currentQResult.size(); i++)//per evaluation
			{
				System.out.println("checking Evaluation "+i);
				List<RDFTuple> tempA=currentQResult.get(i);
				List<TestRDFTupleResults> tempE=expected.get(j).get(i);
				for(int k=0;k<tempA.size();k++)
					assertEquals(tempE.get(k), tempA.get(k));
			}
		}
		System.out.println("|||||||||||||||||||a test case finished");
		

	}
	
	private static void changeFusekisContent() {
		String[] preds=new String[]{"http://example.org/knows","http://example.org/collegue"};
		for(int y=0;y<numberOfInstances;y++){
			accessor[y].getModel().removeAll();
			Graph g2 = new GraphMem();
			for (int k=0;k<FusekiServerDataSize/2;k++){
				g2.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI(preds[y]), 
						NodeFactory.createURI("http://example.org/"+(k+numberOfFusekiChange*100)+"_"+(y+1))));
				g2.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI(preds[(y+1)%2]), 
						NodeFactory.createURI("http://example.org/unrelevant")));
			}
			accessor[y].putModel(ModelFactory.createModelForGraph(g2));		
		}
		numberOfFusekiChange++;
	}
}
