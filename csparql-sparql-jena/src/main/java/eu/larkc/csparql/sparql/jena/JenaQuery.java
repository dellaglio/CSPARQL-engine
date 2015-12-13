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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.TransformBase;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTopN;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.Template;

import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.sparql.api.SparqlQuery;
import eu.larkc.csparql.sparql.jena.service.OpServiceCache;

public class JenaQuery implements SparqlQuery {

	private static Logger logger = LoggerFactory.getLogger(JenaQuery.class);
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
		
		if(Config.INSTANCE.isJenaUsingServiceCaching()){
			Transform tb = new TransformCopy(){
				@Override
				public Op transform(OpService opService, Op subOp) {
					return new OpServiceCache(opService.getService(), subOp, opService.getServiceElement(), opService.getSilent(),computeCacheKeyVars(),computeCacheValueVars());
				}
			};
			rootOp = Transformer.transform(tb, rootOp);
			
//			rootOp.output(IndentedWriter.stdout);
		}		
		
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
	public Set<Var> computeCacheValueVars(){
		final List<OpService> os = new ArrayList<OpService>();
		//removes the SERVICES clauses from original query and put it in os
		Op reminderQueryWithOutService = Transformer.transform(new TransformCopy(){
			public Op transform(OpService opService, Op subOp){
				os.add(opService);
				return OpNull.create();
			}
		}, Algebra.compile(query));

		if (os.size()==0) return null;
		final Set<Var> serviceVars = new HashSet<Var>();
		final Set<Var> otherVars = new HashSet<Var>();
		for(int i=0;i<os.size();i++){			
		OpWalker.walk(os.get(i).getSubOp(),
				// For each element...
				new OpVisitorBase() {
			// ...when it's a SERVICE block 
			public void visit(OpBGP es){
				Iterator<Triple> triples = es.getPattern().getList().iterator();
				while (triples.hasNext()) {
					Triple temp = triples.next();
					if(temp.getObject() instanceof Var)
						serviceVars.add((Var)temp.getObject());
					if(temp.getSubject() instanceof Var)
						serviceVars.add((Var)temp.getSubject());
					if(temp.getPredicate() instanceof Var)
						serviceVars.add((Var)temp.getPredicate());
				}
			}
		});	
		}
		OpWalker.walk(reminderQueryWithOutService,
				// For each element...
				new OpVisitorBase() {
			public void visit(OpJoin es){
				//System.out.println("a service clause");
				if(!es.getLeft().getClass().equals(OpNull.class)){
					Iterator<Triple> it = ((OpBGP)es.getLeft()).getPattern().getList().iterator();
					while (it.hasNext()) {
						Triple temp = it.next();
						if(temp.getObject() instanceof Var)
							otherVars.add((Var)temp.getObject());
						if(temp.getSubject() instanceof Var)
							otherVars.add((Var)temp.getSubject());
						if(temp.getPredicate() instanceof Var)
							otherVars.add((Var)temp.getPredicate());
					}
				}if(!es.getRight().getClass().equals(OpNull.class)){
					Iterator<Triple> it = ((OpBGP)es.getRight()).getPattern().getList().iterator();
					while (it.hasNext()) {
						Triple temp = it.next();
						if(temp.getObject() instanceof Var)
							otherVars.add((Var)temp.getObject());
						if(temp.getSubject() instanceof Var)
							otherVars.add((Var)temp.getSubject());
						if(temp.getPredicate() instanceof Var)
							otherVars.add((Var)temp.getPredicate());
					}
				}
			}	
		});			
		
		
		Set<Var> intersection = new HashSet<Var>(serviceVars); // use the copy constructor
		intersection.retainAll(otherVars);
		serviceVars.removeAll(intersection);
		return serviceVars;			
	}

	private Set<Var> computeVars(Op operator){
		final Set<Var> varibales = new HashSet<Var>();
		OpWalker.walk(operator,
				// For each element...
				new OpVisitorBase() {
			// ...when it's a SERVICE block 
			public void visit(OpBGP es){
				Iterator<Triple> triples = es.getPattern().getList().iterator();
				while (triples.hasNext()) {
					Triple temp = triples.next();
					if(temp.getObject() instanceof Var)
						varibales.add((Var)temp.getObject());
					if(temp.getSubject() instanceof Var)
						varibales.add((Var)temp.getSubject());
					if(temp.getPredicate() instanceof Var)
						varibales.add((Var)temp.getPredicate());
				}
			}
		});
		return varibales;
	}
	
	public Set<Var> computeCacheKeyVars(){
		final List<OpService> os = new ArrayList<OpService>();
		//removes the SERVICES clauses from original query and put it in os
		Op reminderQueryWithOutService = Transformer.transform(new TransformCopy(){
			public Op transform(OpService opService, Op subOp){
				os.add(opService);
				return OpNull.create();
			}
		}, Algebra.compile(query));

		if (os.size()==0) return null;
		final Set<Var> serviceVars = new HashSet<Var>();
		final Set<Var> otherVars = new HashSet<Var>();
		//filling serviceVars with variables in SERVICE clauses
		for(int i=0;i<os.size();i++){
		OpWalker.walk(os.get(i).getSubOp(),
				// For each element...
				new OpVisitorBase() {
			// ...when it's a SERVICE block 
			public void visit(OpBGP es){
				Iterator<Triple> triples = es.getPattern().getList().iterator();
				while (triples.hasNext()) {
					Triple temp = triples.next();
					if(temp.getObject() instanceof Var)
						serviceVars.add((Var)temp.getObject());
					if(temp.getSubject() instanceof Var)
						serviceVars.add((Var)temp.getSubject());
					if(temp.getPredicate() instanceof Var)
						serviceVars.add((Var)temp.getPredicate());
				}
			}
		});	
		}
		//filling otherVars with variables not in SERVICE clauses
		OpWalker.walk(reminderQueryWithOutService,
				// For each element...
				new OpVisitorBase() {
			public void visit(OpJoin es){
				//System.out.println("a service clause");
				if(!es.getLeft().getClass().equals(OpNull.class)){
					Iterator<Triple> it = ((OpBGP)es.getLeft()).getPattern().getList().iterator();
					while (it.hasNext()) {
						Triple temp = it.next();
						if(temp.getObject() instanceof Var)
							otherVars.add((Var)temp.getObject());
						if(temp.getSubject() instanceof Var)
							otherVars.add((Var)temp.getSubject());
						if(temp.getPredicate() instanceof Var)
							otherVars.add((Var)temp.getPredicate());
					}
				}if(!es.getRight().getClass().equals(OpNull.class)){
					Iterator<Triple> it = ((OpBGP)es.getRight()).getPattern().getList().iterator();
					while (it.hasNext()) {
						Triple temp = it.next();
						if(temp.getObject() instanceof Var)
							otherVars.add((Var)temp.getObject());
						if(temp.getSubject() instanceof Var)
							otherVars.add((Var)temp.getSubject());
						if(temp.getPredicate() instanceof Var)
							otherVars.add((Var)temp.getPredicate());
					}
				}
			}	
		});						
		Set<Var> intersection = new HashSet<Var>(serviceVars); // use the copy constructor
		intersection.retainAll(otherVars);
		return intersection;
	}

}
