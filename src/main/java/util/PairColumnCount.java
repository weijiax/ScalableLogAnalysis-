package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class PairColumnCount implements PairFunction<Row, String, HashMap<String, Integer>> {

	int values_idx;
	int key_idx;
	public PairColumnCount(int k, int  v)
	{
		this.values_idx=v;
		key_idx=k;
	}

	public Tuple2<String, HashMap<String, Integer>> call(Row row) throws Exception {
		HashMap<String, Integer> hm = new HashMap<String, Integer>();
		//  T [] parts= (T[]) new Object[values_idx.length];
		String s =String.valueOf(row.get(values_idx));
		 /* if (s.startsWith("WrappedArray")){
				 // System.out.println("wrappedArray handler");
				  ArrayList<String> as = WrappedArrayHandler.unWrap(s);
				  for (String e: as)
					  hm.put(e, 1);	  
		  	  }else */
		  		  hm.put(s, 1);
		return new Tuple2<String, HashMap<String, Integer>>(
				String.valueOf(row.get(key_idx)), hm); 
	}
	

}
