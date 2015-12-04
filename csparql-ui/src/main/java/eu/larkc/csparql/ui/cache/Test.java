package eu.larkc.csparql.ui.cache;

import org.apache.jena.atlas.io.IndentedWriter;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.vocabulary.RDF;

import eu.larkc.csparql.sparql.jena.service.OpExecutorFactoryAcqua;

public class Test {
	public static void main(String[] args) {
		QC.setFactory(ARQ.getContext(), new OpExecutorFactoryAcqua());
		
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
		
		Op op = Algebra.compile(query);
		System.out.println("Query (compiled):");
		op.output(IndentedWriter.stdout);

		op = Algebra.optimize(op);
		System.out.println("Query (optimized):");
		op.output(IndentedWriter.stdout);
		
		QueryIterator it = Algebra.exec(op, m);
		
		System.out.println("Result");
		while(it.hasNext()){
			Binding b = it.nextBinding();
			System.out.println(b.get(Var.alloc("movie")));
		}
	}
}
