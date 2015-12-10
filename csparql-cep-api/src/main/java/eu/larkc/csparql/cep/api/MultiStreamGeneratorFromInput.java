package eu.larkc.csparql.cep.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;



public class MultiStreamGeneratorFromInput extends RdfStream{
	private TestGeneratorFromInput[] streams;
	
	private final String subj = "http://example.org/S";
	private final String pred = "http://example.org/P";
	private final String obj = "http://example.org/O";
	public MultiStreamGeneratorFromInput(TestGeneratorFromInput[] streams){
		super("");
		this.streams=streams;
	}
	public RdfStream[] getStreams(){
		return streams;
	}
	public void run(){
		long[][] timestamps=new long[streams.length][];
		int[] pointers=new int[streams.length];
		for(int y=0;y<streams.length;y++){
			timestamps[y] = streams[y].getTimeStamps();
			pointers[y]=0;
		}
		int c=0;
		while(true){
		long minTime= Long.MAX_VALUE;
		int minStream=0;
		for(int x=0;x<streams.length;x++){
			if(timestamps[x][pointers[x]]<minTime)
			{
				minTime=timestamps[x][pointers[x]];
				minStream=x;
			}
		}
		if(pointers[minStream]<timestamps[minStream].length-1)
			pointers[minStream]++;
		else timestamps[minStream][pointers[minStream]]=Long.MAX_VALUE;
		if(minTime==Long.MAX_VALUE) return;
		String postfix=(minStream+1)+"_"+pointers[minStream];
		RdfQuadruple tempQ = new RdfQuadruple(subj+postfix, pred+postfix, obj+c, minTime);
		streams[minStream].put(tempQ);			
		}		
	}
}
