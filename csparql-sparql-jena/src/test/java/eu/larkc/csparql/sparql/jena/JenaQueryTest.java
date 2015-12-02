/*******************************************************************************
 * Copyright 2014 DEIB -Politecnico di Milano
 *   
 *  Soheila Dehghanzadeh
 *  Shen Gao
 *  Daniele Dell'Aglio
 *   
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *   
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *   
 *  Acknowledgements:
 *  
 *  This work was partially supported by the European project LarKC (FP7-215535)
 ******************************************************************************/

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
