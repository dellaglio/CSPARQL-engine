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

import java.util.List;
import java.util.UUID;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.core.DatasetDescription;
import com.hp.hpl.jena.sparql.syntax.Template;

import eu.larkc.csparql.sparql.api.SparqlQuery;

public class JenaQuery implements SparqlQuery {

	private final String id;
	private Query query;
	private Op rootOp;

	public String getId() {
		// TODO implement SparqlQuery.getIdentifier
		return this.id;
	}

	@Deprecated
	public String getQueryCommand() {
		return query.serialize();
	}

	public JenaQuery(final String cmd) {
		this.id = UUID.randomUUID().toString();
		query = QueryFactory.create(cmd);
		optimizeQuery();		
	}
	
	public Op getRootOp(){
		return rootOp;
	}
	
	public void optimizeQuery(){
		Query oldQuery =  query;

		Op op = Algebra.compile(oldQuery);
		rootOp = Algebra.optimize(op);
//		query = OpAsQuery.asQuery(op);
		
//		if(oldQuery.isAskType())
//			query.setQueryAskType();
//		else if(oldQuery.isDescribeType())
//			query.setQueryDescribeType();
//		else if(oldQuery.isConstructType()){
//			query.setQueryConstructType();
//			query.setConstructTemplate(oldQuery.getConstructTemplate());
//		}
//		
//		query.setPrefixMapping(oldQuery.getPrefixMapping());
//		
//		DatasetDescription dd = new DatasetDescription(oldQuery.getGraphURIs(), oldQuery.getNamedGraphURIs());
//		
//		if(oldQuery.getGraphURIs()!=null)
//			query.getDatasetDescription().addAllDefaultGraphURIs(oldQuery.getGraphURIs());
//		if(oldQuery.getNamedGraphURIs()!=null)
//			query.getDatasetDescription().addAllNamedGraphURIs(oldQuery.getNamedGraphURIs());
//		System.out.println(query);
	}

//	public Query getQuery(){
//		return query;
//	}

	public boolean isAskQuery() {
		return query.isAskType();
	}


	public boolean isGraphQuery() {
		return query.isDescribeType() || query.isConstructType();
	}

	public boolean isSelectQuery() {
		return query.isSelectType();
	}

	public Object getGraphURIs() {
		return query.getGraphURIs();
	}

	public List<String> getResultVars() {
		return query.getResultVars();
	}

	public boolean isDescribeQuery() {
		return query.isDescribeType();
	}

	public Template getConstructTemplate() {
		return query.getConstructTemplate();
	}
}
