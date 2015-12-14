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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static Logger logger = LoggerFactory.getLogger(CacheTestOneQueryMultipleSERVICEOneStream.class);
	
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

//generate server data so that have s10-s20 common between 2 servers
	@BeforeClass public static void startupFuseki(){
		//we attend Y+1 to the end of subject and object to reveal the fuseki instance that it hosting them for testing purposes
		//to make sure that cache didn't mixup cache content of 2 queries
		String[] preds=new String[]{"http://example.org/knows","http://example.org/collegue"};
		for(int y=0;y<numberOfInstances;y++){
			Graph g = new GraphMem();
			for (int k=0;k<FusekiServerDataSize;k++){
				g.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y*10+k)), 
						NodeFactory.createURI(preds[y]), 
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
	public static Iterable<?> data() {
		return Arrays.asList(
				new Object[][]{
					{//in this test cacse the actual object value in fuseki is (k+100) and in cache is k, so given that the results are k confirms that we use cache
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(
								new ArrayList(Arrays.asList( // evaluation 1 
												new TestRDFTupleResults("http://example.org/S10","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												)),
										new ArrayList(Arrays.asList( //evaluation2
												new TestRDFTupleResults("http://example.org/S12","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k"),
												new TestRDFTupleResults("http://example.org/S11","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												))))					
					},{//in this test cacse the actual object value in fuseki is (k+200) and in cache is k+100, so given that the results are k+100 confirms that we use cache
						new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(// evaluation 1 												
										new ArrayList(/*Arrays.asList(// evaluation 2
												new TestRDFTupleResults("http://example.org/S1_2","http://example.org/102_1"))*/),
										new ArrayList(Arrays.asList( //evaluation 3
												new TestRDFTupleResults("http://example.org/S11","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												,new TestRDFTupleResults("http://example.org/S10","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												)),
										new ArrayList(Arrays.asList( //evaluation 3
												new TestRDFTupleResults("http://example.org/S12","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												,new TestRDFTupleResults("http://example.org/S13","http://example.org/knows","http://example.org/k","http://example.org/collegue","http://example.org/k")
												))))
					}
				});
	}

	
	@Test public void shouldWarnAndRunQueryWithOutCaching(){
		String queryGetAll = "REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 ?P3 ?O3 FROM STREAM <http://myexample.org/stream> [RANGE "+width+"s STEP "+slide+"s]"
				+ "  WHERE { ?S ?P ?O SERVICE <http://localhost:3031/test1/sparql> {?S ?P2 ?O2}"
				+ " SERVICE <http://localhost:3032/test2/sparql> {?S ?P3 ?O3}"
				+ "}";
		logger.debug(queryGetAll);

		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;
		try {
			c1 = engine.registerQuery(queryGetAll, false);
			
			 			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		ResultTable formatter = new ResultTable();
		c1.addObserver(formatter);
		streamGenerator.runCacheTestOneQueryMultipleSERVICEOneStream();
		List<List<RDFTuple>> actual = formatter.getResults();
		//if we change the fuseki content here for the next testcase it will be caching the changed content and the expected results will be different for the second test case
		
		logger.debug(actual.toString());
		logger.debug(expected.toString());
		for (int j=0;j<actual.size();j++){//per evaluation
			List<RDFTuple> currentQResult=actual.get(j);
			for(int k=0;k<currentQResult.size();k++)
					assertEquals(expected.get(j).get(k),currentQResult.get(k));
			
		}
		
	}
	
	private static void changeFusekisContent() {
		String[] preds=new String[]{"http://example.org/knows","http://example.org/collegue"};
		for(int y=0;y<numberOfInstances;y++){
			accessor[y].getModel().removeAll();
			Graph g2 = new GraphMem();
			for (int k=0;k<FusekiServerDataSize/2;k++){
				g2.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y*10+k)), 
						NodeFactory.createURI(preds[y]), 
						NodeFactory.createURI("http://example.org/"+(k+numberOfFusekiChange*100)+"_"+(y+1))));
				
			}
			accessor[y].putModel(ModelFactory.createModelForGraph(g2));		
		}
		numberOfFusekiChange++;
	}
	
	
}
