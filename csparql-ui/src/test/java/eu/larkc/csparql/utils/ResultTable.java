package eu.larkc.csparql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;

public class ResultTable extends ResultFormatter {
private List<List<RDFTuple>> resultTable; 
	
	public ResultTable() {
		resultTable = new ArrayList();	
	}
	
	public List<List<RDFTuple>> getResults(){
		return resultTable;
	}

	@Override
	public void update(Observable o, Object arg) {
		List<RDFTuple> t = (List<RDFTuple>) ((RDFTable)arg).getTuples();
		resultTable.add(t);
	}
}
