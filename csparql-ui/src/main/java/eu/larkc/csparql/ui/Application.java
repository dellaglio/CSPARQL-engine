/*******************************************************************************
 * Copyright 2014 DEIB -Politecnico di Milano
 *   
 *  Marco Balduini (marco.balduini@polimi.it)
 *  Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 *  Davide Barbieri
 *  Soheila Dehghanzadeh (soheila.dehghanzadeh@insight-centre.org)
 *  Shen Gao (shengao@ifi.uzh.ch)
 *  Daniele Dell'Aglio (daniele.dellaglio@polimi.it)
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
package eu.larkc.csparql.ui;

import java.io.FileInputStream;
import java.text.ParseException;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import eu.larkc.csparql.cep.api.RDFStreamAggregationTestGenerator;
import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.cep.api.TestGenerator;
import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;

public final class Application {

   /**
    * @param args
    */
	public static void restartFuseki() {
		try {
				String UQ = "DELETE    { ?a ?b ?c } where{?a ?b ?c}; ";
				UpdateRequest query = UpdateFactory.create(UQ);
				UpdateProcessor qexec = UpdateExecutionFactory.createRemoteForm(query, "http://localhost:3030/test/update");
				qexec.execute();
				String serviceURI = "http://localhost:3030/test/data";
				DatasetAccessor accessor;
				accessor = DatasetAccessorFactory.createHTTP(serviceURI);
				Model model = ModelFactory.createDefaultModel();
				model.read(new FileInputStream("/home/soheila/git/githubCSPARQL/CSPARQL-engine/testRDF_L.ttl"), null,
						"TTL");

				accessor.putModel(model);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
   public static void main(final String[] args) {

	   restartFuseki();
	   //final String queryGetAll = "REGISTER QUERY PIPPO AS SELECT ?S ?P ?O FROM STREAM <http://www.glue.com/stream> [RANGE 5s STEP 1s] WHERE { ?S ?P ?O }";
	   final String queryGetAll = "REGISTER QUERY PIPPO AS SELECT ?S ?P ?O FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 10] WHERE { ?S ?P ?O }";

	   final String querySERVICE = "REGISTER QUERY PIPPO AS SELECT ?S ?P2 ?O2 FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 10] WHERE { ?S ?P ?O "
	   		+"SERVICE <http://localhost:3030/test/sparql> {?S ?P2 ?O2}"
			+ "}";

	   final String queryGetEverythingFromBothStream = "REGISTER QUERY PIPPO AS SELECT ?S ?P ?O FROM STREAM <http://www.glue.com/stream> [RANGE TRIPLES 1] FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 1] WHERE { ?S ?P ?O }";
	   
	   final String queryAnonymousNodes = "REGISTER QUERY PIPPO AS CONSTRUCT {                        [] <http://ex.org/by> ?s  ;  <http://ex.org/count> ?n . } FROM STREAM <http://www.larkc.eu/defaultRDFInputStream> [RANGE TRIPLES 10]                        WHERE {                                { SELECT ?s ?p (count(?o) as ?n)                                  WHERE { ?s ?p ?o }                                  GROUP BY ?s }                              }";
	   
	   
	   final String queryNoCount = "REGISTER QUERY PIPPO AS "
				+ "SELECT ?p "
				+ " FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 1] "
				+ " FROM <http://dbpedia.org/resource/Castello_Sforzesco> "
				+ "WHERE { ?s ?p ?o }";

	  final String queryCount = "REGISTER QUERY PIPPO AS "
			+ "SELECT ?t (count(?t) AS ?conto)" + " FROM STREAM <http://www.glue.com/stream> [RANGE TRIPLES 30] WHERE { ?s <http://rdfs.org/sioc/ns#topic> ?t } "
			+ "GROUP BY ?t ";
	  
	  final String querySimpleCount = "REGISTER QUERY PIPPO AS "
			+ "SELECT ?s (COUNT(?s) AS ?conto) FROM STREAM <http://www.glue.com/stream> [RANGE TRIPLES 1] WHERE { ?s ?p ?o } GROUP BY ?s";
	  
	  final String queryGetKB = "REGISTER QUERY PIPPO AS "
			+ "SELECT ?s ?p ?o FROM <http://rdfs.org/sioc/ns>\n FROM STREAM <http://www.glue.com/stream> [RANGE TRIPLES 1] WHERE { ?s ?p ?o }";
	  
	  final String queryGetAll2 = "REGISTER QUERY PIPPO AS "
		  + "CONSTRUCT { <http://www.streams.org/s> <http://www.streams.org/s> ?n }" +
		 " FROM STREAM <http://myexample.org/stream> [RANGE TRIPLES 2] " +
		 " WHERE {" +
		 "  { SELECT (count(?o) as ?n) " +
		 "  { ?s ?p ?o }" +
		 "   GROUP BY ?p } " +
		 "} ";
	  
      final CsparqlEngine engine = new CsparqlEngineImpl();
      engine.initialize();

//      final RDFStreamAggregationTestGenerator tg = new RDFStreamAggregationTestGenerator("http://www.larkc.eu/defaultRDFInputStream");
//      final GlueStreamGenerator tg = new GlueStreamGenerator();
      TestGenerator tg = new TestGenerator("http://myexample.org/stream");
      
      RdfStream rs = engine.registerStream(tg);
      engine.unregisterStream(rs.getIRI());
      engine.registerStream(tg);
      //engine.registerStream(tg2);
      final Thread t = new Thread(tg);
      t.start();
      
      CsparqlQueryResultProxy c1 = null;
      final CsparqlQueryResultProxy c2 = null;

      try {
         c1 = engine.registerQuery(querySERVICE, false);
      } catch (final ParseException ex) {
         System.out.println("errore di parsing: " + ex.getMessage());
      }
      if (c1 != null) {
         c1.addObserver(new ConsoleFormatter());
      }
   }
   private Application() {
      // hidden constructor
   }

}