package xalt;

import org.apache.spark.api.java.function.Function;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

public class StringArrayAt implements Function<Row, String[]> {
	 /*
	   * executor class to get unique values per row by an given index. 
	   * Method to load 
	   */
	  int index=0;
	  public StringArrayAt(int i)
	  {
			  index=i;
	  }


	public String[] call(Row row) throws Exception {
		// TODO Auto-generated method stub
		String[] r =null;
		Object o = row.get(index);
		if (o instanceof String ){
			r = new String[0];
			r[0]=(String) o;
		}else if (o instanceof Number ){
			r = new String[0];
			r[0]= String.valueOf(o);
		}else if (o instanceof List){
			r= new String[((List) o).size()];
			int i=0;
			for (Object c : ((List) o)){
				r[i++]=String.valueOf(c);
			}
		}else {
			System.out.println("Unhandled data types: "+ o.getClass().getName());
		}
				
		return r;
	}
	
	

}
