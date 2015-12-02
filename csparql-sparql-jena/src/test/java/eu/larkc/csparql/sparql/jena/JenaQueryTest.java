package eu.larkc.csparql.sparql.jena;

import static org.junit.Assert.*;

import org.junit.Test;

public class JenaQueryTest {
	@Test public void shouldParseSelectQuery(){
		String query = "SELECT ?s FROM <http://example.org/example> WHERE{?s ?p ?o}";
		JenaQuery jq =  new JenaQuery(query);
		assertEquals(true, jq.isSelectQuery());
		assertEquals(false, jq.isGraphQuery());
		assertEquals(false, jq.isAskQuery());
	}
	
	@Test public void shouldParseConstructQuery(){
		String query = "PREFIX ex: <http://example.org#> CONSTRUCT {?s ex:p2 ?o} WHERE{?s ex:p1 ?o}";
		JenaQuery jq =  new JenaQuery(query);
		assertEquals(false, jq.isSelectQuery());
		assertEquals(true, jq.isGraphQuery());
		assertEquals(false, jq.isAskQuery());
	}

	@Test public void shouldParseAskQuery(){
		String query = "PREFIX ex: <http://example.org#> ASK {?s ex:p1 ?o}";
		JenaQuery jq =  new JenaQuery(query);
		assertEquals(false, jq.isSelectQuery());
		assertEquals(false, jq.isGraphQuery());
		assertEquals(true, jq.isAskQuery());
	}
}
