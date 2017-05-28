package util;

import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import scala.collection.mutable.WrappedArray;
import scala.collection.mutable.WrappedArray.ofRef;



public class StringAt implements Function<Row, String> {
	 /*
	   * executor class to get unique values per row by an given index. 
	   * Method to load 
	   */
	  int index=0;
		  public StringAt(int i)
		  {
			  index=i;
		  }

		public String call(Row r) throws Exception {
			/*
			Object o = r.get(index);
			if (! o.getClass().getName().equalsIgnoreCase("java.lang.String"))
				System.out.println(o.getClass().getName());
			if (o instanceof Collection){
				StringBuffer  buff = new StringBuffer();
				for (Object e : ((Collection) o))
					buff.append(String.valueOf(e)+"|");
				return buff.toString();
					
			} else if (o instanceof Object[]){
				StringBuffer  buff = new StringBuffer();
				for (Object e : ((Object[]) o))
					buff.append(String.valueOf(e)+"|");
				return buff.toString();				
			} else if (o.getClass().getName().equalsIgnoreCase("scala.collection.mutable.WrappedArray$ofRef")){
				WrappedArray w =(WrappedArray) o;
				return "Wrapped Array";
			}
			
				*/
			return String.valueOf(r.get(index));
			//return r.getString(index);
		}
	 


}
