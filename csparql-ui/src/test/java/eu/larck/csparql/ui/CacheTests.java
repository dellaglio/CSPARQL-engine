package eu.larck.csparql.ui;

import static org.junit.Assert.assertEquals;

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

import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.reasoner.rulesys.builtins.Equal;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.mem.GraphMem;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.core.ResultFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.utils.ResultTable;
import eu.larkc.csparql.utils.TestGeneratorFromInput;
import junit.framework.Assert;


@RunWith(Parameterized.class)
public class CacheTests {
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
	private static EmbeddedFusekiServer fuseki;
	private static DatasetAccessor accessor;

	private CsparqlEngine engine;
	private TestGeneratorFromInput streamGenerator;


	@BeforeClass public static void startupFuseki(){
		Graph g = new GraphMem();
		for (int k=0;k<50;k++){
			g.add(new Triple(
					NodeFactory.createURI("http://example.org/S"+k), 
					NodeFactory.createURI("http://example.org/followerCount"), 
					NodeFactory.createURI("http://example.org/k")));
		}

		fuseki = EmbeddedFusekiServer.create(3031, DatasetGraphFactory.create(g), "test"); 
		accessor = DatasetAccessorFactory.createHTTP("http://localhost:3031/test/data");
		fuseki.start();
	}

	@BeforeClass public static void initialConfig(){
		Properties prop = new Properties();
		prop.put("esper.externaltime.enabled", true);
		prop.put("jena.service.cache.enabled", true);
		Config.INSTANCE.setConfigParams(prop);
	}

	@AfterClass public static void shutdownFuseki(){
		fuseki.stop();
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
	private List<List<TestRDFTupleResults>> expected;

	public CacheTests(long[] input, int width, int slide, List<List<TestRDFTupleResults>> expected){
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
					{
						new long[]{1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S2","http://example.org/followerCount","http://example.org/102"),
										new TestRDFTupleResults("http://example.org/S1","http://example.org/followerCount","http://example.org/101"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S4","http://example.org/followerCount","http://example.org/104"),
										new TestRDFTupleResults("http://example.org/S3","http://example.org/followerCount","http://example.org/103")
										))))
					},{
						new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001}, 
						1, 1, new ArrayList(Arrays.asList(
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S1","http://example.org/followerCount","http://example.org/101"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S2","http://example.org/followerCount","http://example.org/102"),
										new TestRDFTupleResults("http://example.org/S3","http://example.org/followerCount","http://example.org/103"))),
								new ArrayList(Arrays.asList(
										new TestRDFTupleResults("http://example.org/S5","http://example.org/followerCount","http://example.org/105"),
										new TestRDFTupleResults("http://example.org/S4","http://example.org/followerCount","http://example.org/104")
										))))
					}
				});
	}


	@Test public void shouldMatchQueryResult(){
		String queryGetAll = "REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream> [RANGE "+width+"s STEP "+slide+"s]"
				+ "  WHERE { ?S ?P ?O SERVICE <http://localhost:3031/test/sparql> {?S ?P2 ?O2}"
				+ "}";
		System.out.println(queryGetAll);

		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;
		changeFusekiContent();
		try {
			c1 = engine.registerQuery(queryGetAll, false);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		ResultTable formatter = new ResultTable();
		c1.addObserver(formatter);
		streamGenerator.run();
		List<List<RDFTuple>> actual = formatter.getResults();
		
		/*System.out.println(actual);
		System.out.println(">>>.."+expected);*/
		for(int i = 0; i<actual.size(); i++)
		{
			List<RDFTuple> tempA=actual.get(i);
			List<TestRDFTupleResults> tempE=expected.get(i);
			for(int j=0;j<tempA.size();j++)
				assertEquals(tempE.get(j), tempA.get(j));
		}
	}




	//for manual checking purposes
	public static void main2(String[] args) {
		Graph g = new GraphMem();
		for (int k=0;k<50;k++){
			g.add(new Triple(
					NodeFactory.createURI("http://example.org/S"+k), 
					NodeFactory.createURI("http://example.org/followerCount"), 
					NodeFactory.createURI("http://example.org/k")));
		}
		EmbeddedFusekiServer fuseki = EmbeddedFusekiServer.create(3031, DatasetGraphFactory.create(g), "test"); 

		fuseki.start();


		String query = 
				"SELECT ?S ?P2 ?O2 "
						+ "WHERE { "
						+"SERVICE <http://localhost:3031/test/sparql> {?S ?P2 ?O2}"
						+ "}";

		QueryExecution qe = QueryExecutionFactory.create(query, ModelFactory.createDefaultModel());
		ResultSet rs = qe.execSelect();

		while(rs.hasNext()){
			System.out.println(rs.next());
		}

		fuseki.stop();
	}
	public static void main(String[] args) {
		
		Graph g = new GraphMem();
		for (int k=0;k<3;k++){
			g.add(new Triple(
					NodeFactory.createURI("http://example.org/S"+k), 
					NodeFactory.createURI("http://example.org/followerCount"), 
					NodeFactory.createURI("http://example.org/k"+k)));
		}
		EmbeddedFusekiServer fuseki = EmbeddedFusekiServer.create(3031, DatasetGraphFactory.create(g), "test"); 
		accessor = DatasetAccessorFactory.createHTTP("http://localhost:3031/test/data");
		fuseki.start();	

		CsparqlEngine engine = new CsparqlEngineImpl();
		engine.initialize();

		TestGeneratorFromInput streamGenerator = new TestGeneratorFromInput("http://myexample.org/stream", 
				new long[]{600, 1000, 1340, 2000, 2020, 3000, 3001});

		String queryGetAll = 
				"REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream> "
						+ "[RANGE 1s STEP 1s]  "
						+ "WHERE { ?S ?P ?O "
						+"SERVICE <http://localhost:3031/test/sparql> {?S ?P2 ?O2}"
						+ "}";
		//				"REGISTER QUERY PIPPO AS SELECT ?O FROM STREAM <http://myexample.org/stream> [RANGE 4s STEP 4s]  WHERE { ?S ?P ?O } ORDER BY ?O";

		//		TestGeneratorFromFile tg = new TestGeneratorFromFile("http://myexample.org/stream", "src/test/resources/sample_input.txt");
		engine.registerStream(streamGenerator);
		CsparqlQueryResultProxy c1 = null;

		try {
			c1 = engine.registerQuery(queryGetAll, false);
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
		changeFusekiContent();
		streamGenerator.run();

		fuseki.stop();

	}

	private static void changeFusekiContent() {
		accessor.getModel().removeAll();
		Graph g2 = new GraphMem();
		for (int k=0;k<50;k++){
			g2.add(new Triple(
					NodeFactory.createURI("http://example.org/S"+k), 
					NodeFactory.createURI("http://example.org/followerCount"), 
					NodeFactory.createURI("http://example.org/"+(k+100))));
		}
		accessor.putModel(ModelFactory.createModelForGraph(g2));		
	}

}
