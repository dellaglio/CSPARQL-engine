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
public class RandomMaintenanceTest {
	private static Logger logger = LoggerFactory.getLogger(RandomMaintenanceTest.class);
	private static int numberOfInstances=1;//number of remote service providers 
	private static int numberOfFusekiChange=1;//this is intended to keep track of the object values for testing
	private static int FusekiServerDataSize=20;
	private static EmbeddedFusekiServer[] fuseki=new EmbeddedFusekiServer[numberOfInstances];
	private static DatasetAccessor[] accessor=new DatasetAccessor[numberOfInstances];
	private CsparqlEngine engine;
	private TestGeneratorFromInput streamGenerator;


	@BeforeClass public static void startupFuseki(){
		//we attach Y+1 to the end of subject and object to reveal the fuseki instance that it hosting them for testing purposes
		//to make sure that cache didn't mixup cache content of 2 queries
		for(int y=0;y<numberOfInstances;y++){
			Graph g = new GraphMem();
			for (int k=0;k<FusekiServerDataSize;k++){
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
	/*
	 * if we disbale caching below test will fail because caching is not enabled and the result will be directly retrived form remote fuseki, 
	 * if we enable caching and enable fillJenaServiceCacheAtStart the below test should pass (without cache replacement i.e., cache size is larger than FusekiServerDataSize)
	 * if cache size is smaller than FusekiServerDataSize there are chances that this setting fails for above data (because cache dynamically fetch not existing data and refresh its conent while the test output is designed for cacses that cache content will never change)
	 * if we enable caching but disbale fillJenaServiceCacheAtStart the below test should fail
	 * */

	@BeforeClass public static void initialConfig(){
		Properties prop = new Properties();
		prop.put("esper.externaltime.enabled", true);
		prop.put("jena.service.cache.enabled", true);
		prop.put("jena.service.cache.fillJenaServiceCacheAtStart",true);
		prop.put("jena.service.cache.size",FusekiServerDataSize);
		//maintenance policy parameters
		prop.put("jena.service.cache.maintenance.enabled", true);		
		prop.put("jena.service.cache.maintenance.budget", 2);
		/*
		 * if we set the update budget to 1 only first element of each evaluation will be synchronized with remote
		 * and the rest will be according to previous window evlauation
		 */
		prop.put("jena.service.cache.maintenance.type", "wsj-random");
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
	public RandomMaintenanceTest(long[] input, int width, int slide, List<List<TestRDFTupleResults>> expected){
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
					{
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1_2","http://example.org/followerCount","http://example.org/102_1"),
										new TestRDFTupleResults("http://example.org/S1_1","http://example.org/followerCount","http://example.org/101_1"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1_4","http://example.org/followerCount","http://example.org/104_1"),
										new TestRDFTupleResults("http://example.org/S1_3","http://example.org/followerCount","http://example.org/103_1")
										))))
					},{
						new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1_1","http://example.org/followerCount","http://example.org/201_1"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1_3","http://example.org/followerCount","http://example.org/203_1"),
										new TestRDFTupleResults("http://example.org/S1_2","http://example.org/followerCount","http://example.org/202_1"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1_4","http://example.org/followerCount","http://example.org/204_1"),
										new TestRDFTupleResults("http://example.org/S1_5","http://example.org/followerCount","http://example.org/205_1")
										))))
					}
				});
	}

	@Test public void shouldMatchQueryResultUsingOneQueryOneSERVICEoneStream(){
		String queryGetAll = "REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream> [RANGE "+width+"s STEP "+slide+"s]"
				+ "  WHERE { ?S ?P ?O SERVICE <http://localhost:3031/test1/sparql> {?S ?P2 ?O2}"
				+ "}";
		logger.debug(queryGetAll);

		engine.registerStream(streamGenerator);
		
		CsparqlQueryResultProxy c1 = null;
		try {
			c1 = engine.registerQuery(queryGetAll, false);
			changeFusekisContent(); 
			/*
			 * cache intialization happens during query registeration, so we change the fuseki content after query registeration */

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		ResultTable formatter = new ResultTable();
		c1.addObserver(formatter);
		streamGenerator.run();
		List<List<RDFTuple>> actual = formatter.getResults();
		//if we change the fuseki content here for the next testcase it will be caching the changed content and the expected results will be different for the second test case

		logger.debug(actual.toString());
		logger.debug(">>>.."+expected);
		for(int i = 0; i<actual.size(); i++)
		{
			List<RDFTuple> tempA=actual.get(i);
			List<TestRDFTupleResults> tempE=expected.get(i);
			for(int j=0;j<tempA.size();j++)
				assertEquals(tempE.get(j), tempA.get(j));
		}
	}

	private static void changeFusekisContent() {
		for(int y=0;y<numberOfInstances;y++){
			accessor[y].getModel().removeAll();
			Graph g2 = new GraphMem();
			for (int k=0;k<FusekiServerDataSize;k++){
				g2.add(new Triple(
						NodeFactory.createURI("http://example.org/S"+(y+1)+"_"+k), 
						NodeFactory.createURI("http://example.org/followerCount"), 
						NodeFactory.createURI("http://example.org/"+(k+numberOfFusekiChange*100)+"_"+(y+1))));
			}
			accessor[y].putModel(ModelFactory.createModelForGraph(g2));		
		}
		numberOfFusekiChange++;
	}
}
