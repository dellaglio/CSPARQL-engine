package eu.larck.csparql.ui;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.jena.fuseki.EmbeddedFusekiServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.mem.GraphMem;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;

import eu.larck.csparql.ui.CacheTests.TestRDFTupleResults;
import eu.larkc.csparql.cep.api.TestGeneratorFromInput;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.utils.ResultTable;

@RunWith(Parameterized.class)
public class CacheTestOneQueryMultipleSERVICEOneStream {

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
	private static EmbeddedFusekiServer[] fuseki=new EmbeddedFusekiServer[numberOfInstances];
	private static DatasetAccessor[] accessor=new DatasetAccessor[numberOfInstances];
	private CsparqlEngine engine;
	private TestGeneratorFromInput streamGenerator;


	@BeforeClass public static void startupFuseki(){
		//we attend Y+1 to the end of subject and object to reveal the fuseki instance that it hosting them for testing purposes
		//to make sure that cache didn't mixup cache content of 2 queries
		for(int y=0;y<numberOfInstances;y++){
			Graph g = new GraphMem();
			for (int k=0;k<20;k++){
				g.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI("http://example.org/followerCount"), 
						NodeFactory.createURI("http://example.org/k")));
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
	private List<List<TestRDFTupleResults>> expected;//each evaluation results a list of RDFTuple
	
	public CacheTestOneQueryMultipleSERVICEOneStream(long[] input, int width, int slide, List<List<TestRDFTupleResults>> expected){
		this.input = input;
		this.width = width;
		this.slide = slide;
		this.expected = new ArrayList();
		for(List<TestRDFTupleResults> i : expected)
			this.expected.add(i);
	}


	@Parameterized.Parameters
	public static Iterable<?> data() {//this  test output is true for cases when the cache size is very small and none of the stream members would be included in the initial cache
		//however if the cache is large enough to include the stream elements from the begining their expected value should be the value of initial cache which is k
		return Arrays.asList(
				new Object[][]{
					{//in this test cacse the actual follower count in fuseki is (k+100) and in cache is k, so given that the results are k confirms that we use cache
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList( //one per query
								new ArrayList(Arrays.asList( //result ofQuery1 - one per evaluation
										new ArrayList(Arrays.asList( // evaluation 1 
												new TestRDFTupleResults("http://example.org/S1_2","http://example.org/followerCount","http://example.org/k")
												//,new TestRDFTupleResults("http://example.org/S1_1","http://example.org/followerCount","http://example.org/k")
												)),
										new ArrayList(Arrays.asList( //evaluation2
												new TestRDFTupleResults("http://example.org/S1_4","http://example.org/followerCount","http://example.org/k")
												//,new TestRDFTupleResults("http://example.org/S1_3","http://example.org/followerCount","http://example.org/k")
												)))),
								new ArrayList(Arrays.asList( //result ofQuery2 - one per evaluation
										new ArrayList(Arrays.asList( // evaluation 1
												new TestRDFTupleResults("http://example.org/S2_1","http://example.org/followerCount","http://example.org/k")
												//,new TestRDFTupleResults("http://example.org/S1_1","http://example.org/followerCount","http://example.org/k")
												)),
										new ArrayList(Arrays.asList( //evaluation2
												new TestRDFTupleResults("http://example.org/S2_3","http://example.org/followerCount","http://example.org/k"),
												new TestRDFTupleResults("http://example.org/S2_1","http://example.org/followerCount","http://example.org/k")
												))))))							
					}
				});
	}

	/*public static void FusekiOneInstancemain2(String[] args) {

		startupFuseki();
		changeFusekisContent();
		String query = 
				"SELECT ?S1 ?S2 ?P1 ?P2 ?O1 ?O2 "
						+ "WHERE { "
						+"SERVICE <http://localhost:3031/test1/sparql> {?S1 ?P1 ?O1}"
						+"SERVICE <http://localhost:3032/test2/sparql> {?S2 ?P2 ?O2}"
						+ "}";

		QueryExecution qe = QueryExecutionFactory.create(query, ModelFactory.createDefaultModel());
		ResultSet rs = qe.execSelect();

		while(rs.hasNext()){
			System.out.println(rs.next());
		}

		shutdownFuseki();
	}*/
	@Test public void mainTestsMultipleCachesForOneStream() {//
/* usecase
 * S follows O
 * S knows Y
 * O followercount X
 * */
		
		String query = "REGISTER QUERY PIPPO AS SELECT ?S  ?O ?O1 ?O2 FROM STREAM <http://myexample.org/stream> "
				+ "[RANGE 1s STEP 1s]  "
				+ "WHERE { ?S ?P ?O "
						+"SERVICE <http://localhost:3031/test1/sparql> {?S ?P1 ?O1}"
						+"SERVICE <http://localhost:3032/test2/sparql> {?O ?P2 ?O2}"
						+ "}";
		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;
		
		try {
			c1 = engine.registerQuery(query, false);
			//cache will be filled and queried with these vars ?S1 ?S2 ?P1 ?P2 ?O1 ?O2 form server "localhost:3031/test1" and "localhost:3032/test2" for query 					
		} catch (ParseException e) {
			e.printStackTrace();
		}
		/*c1.addObserver(new ResultFormatter()  {	
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
		});*/
		ResultTable formatter1 = new ResultTable();
		c1.addObserver(formatter1);
		changeFusekisContent(); 
		streamGenerator.runCacheTestOneQueryMultipleSERVICEOneStream();//stream generator will start to produce triples that are of intereste for both queryGetAll1 and queryGetAll2
		//each will attract their relevant data from stream and process query over it
		List<List<RDFTuple>> actual =formatter1.getResults();
		//if we change the fuseki content here for the next testcase it will be caching the changed content and the expected results will be different for the second test case
		System.out.println("output >>>>>>> "+actual);
		for(int i = 0; i<actual.size(); i++)//per evaluation
			{
				System.out.println("checking Evaluation "+i);
				List<RDFTuple> tempA=actual.get(i);
				List<TestRDFTupleResults> tempE=expected.get(i);
				for(int k=0;k<tempA.size();k++)
					assertEquals(tempE.get(k), tempA.get(k));
			}
		
		System.out.println("|||||||||||||||||||a test case finished");
		//shutdownFuseki();

	}
	/*public static void main(String[] args) {//TestsMultipleCachesFormultipleStreams

		startupFuseki();
		CsparqlEngine engine = new CsparqlEngineImpl();
		engine.initialize();
		TestGeneratorFromInput[] streams=new TestGeneratorFromInput[3];
		streams[0] = new TestGeneratorFromInput("http://myexample.org/stream1", 
				new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001});
		streams[1] = new TestGeneratorFromInput("http://myexample.org/stream2", 
				new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001});

		streams[2] = new TestGeneratorFromInput("http://myexample.org/stream3", 
				new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001});

		MultiStreamGeneratorFromInput streamGenerator = new MultiStreamGeneratorFromInput(streams);

		String queryGetAll1 = 
				"REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream1> "
						+ "[RANGE 1s STEP 1s]  "
						+ "WHERE { ?S ?P ?O "
						+"SERVICE <http://localhost:3031/test1/sparql> {?S ?P2 ?O2}"
						+ "}";
		String queryGetAll2 = 
				"REGISTER QUERY PIPPO AS SELECT ?S1 ?P3 ?O3 FROM STREAM <http://myexample.org/stream2> "
						+ "[RANGE 1s STEP 1s]  "
						+ "WHERE { ?S1 ?P ?O "
						+"SERVICE <http://localhost:3031/test2/sparql> {?S1 ?P3 ?O3}"
						+ "}";
		//				"REGISTER QUERY PIPPO AS SELECT ?O FROM STREAM <http://myexample.org/stream> [RANGE 4s STEP 4s]  WHERE { ?S ?P ?O } ORDER BY ?O";

		//		TestGeneratorFromFile tg = new TestGeneratorFromFile("http://myexample.org/stream", "src/test/resources/sample_input.txt");
		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;

		try {
			c1 = engine.registerQuery(queryGetAll1, false);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c1.addObserver(new ResultFormatter() {	
			@Override
			public void update(Observable o, Object arg) {
				for(Iterator<RDFTuple> it = ((RDFTable)arg).getTuples().iterator();it.hasNext();){
					RDFTuple tuple = it.next();
					System.out.println(tuple);
				}
				System.out.println();
			}
		});


		changeFusekisContent();
		streamGenerator.run();

		shutdownFuseki();

	}*/
	private static void changeFusekisContent() {
		for(int y=0;y<numberOfInstances;y++){
			accessor[y].getModel().removeAll();
			Graph g2 = new GraphMem();
			for (int k=0;k<20;k++){
				g2.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI("http://example.org/followerCount"), 
						NodeFactory.createURI("http://example.org/"+(k+numberOfFusekiChange*100)+"_"+(y+1))));
			}
			accessor[y].putModel(ModelFactory.createModelForGraph(g2));		
		}
		numberOfFusekiChange++;
	}

	/*public static void shouldMatchResult(String[] args) {
		
		startupFuseki();
		changeFusekisContent();
		String query = 
				"SELECT ?S1 ?S2 ?P1 ?P2 ?O1 ?O2 "
						+ "WHERE { "
						+"SERVICE <http://localhost:3031/test1/sparql> {?S1 ?P1 ?O1}"
						+"SERVICE <http://localhost:3032/test2/sparql> {?S2 ?P2 ?O2}"
						+ "}";

		QueryExecution qe = QueryExecutionFactory.create(query, ModelFactory.createDefaultModel());
		ResultSet rs = qe.execSelect();

		while(rs.hasNext()){
			System.out.println(rs.next());
		}

		shutdownFuseki();
	}*/
	
}
