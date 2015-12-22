package eu.larkc.csparql.sparql.jena.ext;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.StageGenerator;

public class AcquaTSStageGenerator implements StageGenerator
{
    StageGenerator above = null ;

    public AcquaTSStageGenerator (StageGenerator original)
    { above = original ; }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt)
    {
    	return QueryIterBlockTSTriples.create(input, pattern, execCxt) ;
    }

}
