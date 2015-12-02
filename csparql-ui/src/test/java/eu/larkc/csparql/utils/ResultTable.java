package eu.larkc.csparql.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Observable;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;

public class ResultTable extends ResultFormatter {
private List<RDFTuple> resultTable; 
	
	public ResultTable() {
		resultTable = new ArrayList<RDFTuple>();	
	}
	
	public List<RDFTuple> getResults(){
		return resultTable;
	}

	@Override
	public void update(Observable o, Object arg) {
		Collection<RDFTuple> t = ((RDFTable)arg).getTuples();
		resultTable.addAll(t);
	}
}
