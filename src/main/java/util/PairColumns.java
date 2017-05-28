package util;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class PairColumns<T> implements PairFunction<Row, String, List<String>> {
	
	int [] values_idx;
	int key_idx;
	public PairColumns(int k, int [] v)
	{
		this.values_idx=v;
		key_idx=k;
	}
	
	public Tuple2<String, List<String>> call(Row r) throws Exception {
	
		  T [] parts= (T[]) new Object[values_idx.length];
		  for (int i=0; i< parts.length; i++)
			  parts[i]= (T) String.valueOf(r.get(values_idx[i]));
		  		//parts[i]= (T) r.get(values_idx[i]);
		  ArrayList<String> res = new ArrayList<String>();
		  for (T t: parts){
			  String s = (String) t;
			  if (s.startsWith("WrappedArray")){
				  //System.out.println("wrappedArray handler" + WrappedArrayHandler.unWrap(s));
				  res.addAll(WrappedArrayHandler.unWrap(s));
		  	  }else 
				  res.add(s);
			  
		  }
			  
		  
		  return new Tuple2<String, List<String>>(
				  String.valueOf(r.get(key_idx)),  res); 
	}
	
	
}
