package eu.larkc.csparql.sparql.jena.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.atlas.io.IndentedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.TransformCopy;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.vocabulary.RDF;

import eu.larkc.csparql.sparql.jena.service.maintenance.QueryIterServiceMaintainedCache;

public class QueryRunner {
	private Model model;

	private Query query;
	private Op parsedQuery;
	private Op optimizedQuery;
	private List<OpService> serviceList;
	private int serviceCount;
	private Set<Var> keys;
	private Set<Var> values;
	private static Logger logger = LoggerFactory.getLogger(QueryRunner.class);

	public QueryRunner(String queryString, Model localData){		
		query = QueryFactory.create(queryString);
		model = localData;
		parsedQuery = Algebra.compile(query);
		/*System.out.println("Query (compiled):");
		parsedQuery.output(IndentedWriter.stdout);*/
		optimizedQuery = Algebra.optimize(parsedQuery);
		/*System.out.println("Query (optimized):");
		optimizedQuery.output(IndentedWriter.stdout);*/
		serviceCount= extractServiceClauses();//serviceLsit is initialized and filled in this function based on parsedQuery
		distinctKeysValueVars();
	}

	private void distinctKeysValueVars() {
		this.keys=new HashSet<Var>();
		this.values=new HashSet<Var>();
		//common variables  among service and stream are key vars
		final List<OpService> os = new ArrayList<OpService>();
		//removes the SERVICES clauses from original query and put it in os
		Op reminderQueryWithOutService = Transformer.transform(new TransformCopy(){
			public Op transform(OpService opService, Op subOp){
				os.add(opService);
				return OpNull.create();
			}
		}, parsedQuery);

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
		//System.out.println("reminderQueryWithOutService >>>> "+reminderQueryWithOutService);
		//filling otherVars with variables not in SERVICE clauses
		OpWalker.walk(reminderQueryWithOutService,
				// For each element...
				new OpVisitorBase() {
			public void visit(OpJoin es){
				//System.out.println("a service clause");
				if(!es.getLeft().getClass().equals(OpNull.class)){
					if(es.getLeft() instanceof OpBGP){
						Iterator<Triple> it = ((OpBGP)es.getLeft()).getPattern().getList().iterator();
						while (it.hasNext()) {
							Triple temp = it.next();
							if(temp.getObject() instanceof Var)
								otherVars.add((Var)temp.getObject());
							if(temp.getSubject() instanceof Var)
								otherVars.add((Var)temp.getSubject());
							if(temp.getPredicate() instanceof Var)
								otherVars.add((Var)temp.getPredicate());
						}}else visit(((OpJoin)es.getLeft()));
				}if(!es.getRight().getClass().equals(OpNull.class)){
					if(es.getRight() instanceof OpBGP){
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
					}else visit(((OpJoin)es.getRight()));
				}
			}
		}
				);						
		Set<Var> intersection = new HashSet<Var>(serviceVars); // use the copy constructor
		intersection.retainAll(otherVars);
		this.keys.addAll(intersection);
		logger.debug("____________keys vars of query "+keys);
		serviceVars.removeAll(intersection);
		this.values.addAll(serviceVars);	
		logger.debug("___________value vars of query "+values);

	}

	public Query getQuery(){
		return query;
	}

	public Set<Var> computeCacheValueVars(){
		return this.values;
	}

	public Set<Var> computeCacheKeyVars(){		
		return this.keys;
	}


	public int extractServiceClauses() {
		serviceList=new ArrayList<>();
		//removes the SERVICES clauses from original query and put it in os
		Transformer.transform(new TransformCopy(){
			public Op transform(OpService opService, Op subOp){
				serviceList.add(opService/*.getService().toString()*/);
				return OpNull.create();
			}
		}, parsedQuery);
		return serviceList.size();
	}

	public QueryIterator execute(){
		QueryIterator it = Algebra.exec(optimizedQuery, model);

		return it;		
	}


	public static void main(String[] args) {

		//local data:
		Model m = ModelFactory.createDefaultModel();
		m.add(m.createResource("http://dbpedia.org/resource/Star_Wars_(film)"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://dbpedia.org/resource/The_Empire_Strikes_Back"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://es.dbpedia.org/resource/Star_Wars:_Episode_VI_-_Return_of_the_Jedi"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://dbpedia.org/resource/Star_Wars_Episode_I:_The_Phantom_Menace"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://dbpedia.org/resource/Star_Wars_Episode_II:_Attack_of_the_Clones"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://dbpedia.org/resource/Star_Wars_Episode_III:_Revenge_of_the_Sith"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));
		m.add(m.createResource("http://dbpedia.org/resource/Star_Wars:_The_Force_Awakens"),
				RDF.type, 
				m.createResource("http://example.org/StarWarsMovie"));

		//query
		String queryString = 
				"SELECT ?movie "
						+ "WHERE { "
						+ "?movie a <http://example.org/StarWarsMovie> "
						+ "SERVICE <http://dbpedia.org/sparql> {"
						+ "?movie a <http://dbpedia.org/ontology/Film> . "
						+ "?movie <http://dbpedia.org/ontology/director> <http://dbpedia.org/resource/George_Lucas> ."
						+ "} "
						+ "} "
						//				+ "LIMIT 20"
						;

		//query execution		
		Query query = QueryFactory.create(queryString);

		Op parsedQuery = Algebra.compile(query);
		System.out.println("Query (compiled):");
		parsedQuery.output(IndentedWriter.stdout);

		Op optimizedQuery = Algebra.optimize(parsedQuery);
		System.out.println("Query (optimized):");
		optimizedQuery.output(IndentedWriter.stdout);

		QueryIterator it = Algebra.exec(optimizedQuery, m);

		System.out.println("Result");
		while(it.hasNext()){
			Binding b = it.nextBinding();
			System.out.println(b.get(Var.alloc("movie")));
		}		

	}

	public List<OpService> getSERVICEEndpointURI() {
		// TODO Auto-generated method stub
		return serviceList;
	}

	public Binding getKeyBinding(Binding outerBinding) {
		Iterator<Var> keyIt = this.keys.iterator();
		BindingMap keyBm = BindingFactory.create();
		while(keyIt.hasNext()){
			Var TempKey=keyIt.next();
			Node tempValue = outerBinding.get(TempKey);
			if(tempValue!=null)
				keyBm.add(TempKey, tempValue);
		}
		return keyBm;		
	}

	public Binding getValueBinding(Binding solb) {
		Iterator<Var> valueIt = this.values.iterator();
		BindingMap valueBm = BindingFactory.create();
		while(valueIt.hasNext()){
			Var TempKey=valueIt.next();
			Node tempValue = solb.get(TempKey);
			if(tempValue!=null)
				valueBm.add(TempKey, tempValue);
		}
		return valueBm;
	}
}
