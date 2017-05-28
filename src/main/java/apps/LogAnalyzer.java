package apps;

import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import util.ModelTransformation;
import util.PairColumnCount;
import util.PairColumns;
import util.StringAt;

public class LogAnalyzer implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	SparkSession spark; 
	Dataset<Row> logs;
	String out_path="~/";
	
	public LogAnalyzer(SparkSession spark, Dataset<Row> data){
		this(spark, data, "~/");
	}
		
	public LogAnalyzer(SparkSession spark, Dataset<Row> data, String output){
		this.spark=spark; 
		this.logs=data;
		this.out_path=output;
	}
	
	
	private LogAnalyzer()
	{
	}

	public RuntimeConfig getConf() {
		return spark.conf(); 
	}

	public SparkContext getSParkContext() {
		return spark.sparkContext();
	}
	
	//@deprecated
	public SQLContext getSqlContext() {
		return spark.sqlContext();
	}

	public Dataset<Row> getLogs() {
		return logs;
	}

	public void setLogs(Dataset<Row> logs) {
		this.logs = logs;
	}

	public String getOut_path() {
		return out_path;
	}

	public void setOut_path(String out_path) {
		this.out_path = out_path;
	}

	
/*
 * Need fix sequential pattern analysis for general purpose. 
	public static void runSeqPattern(Dataset<Row> selected, double min_supp, int max_len)
	{
		//aggregate user_exec sequences;
		JavaPairRDD<String, List<JobRecord>> pairs = selected.toJavaRDD().mapToPair(new PairFunction<Row, String, List<JobRecord>>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, List<JobRecord>> call(Row r) {
				  return new Tuple2<String, List<JobRecord>>(
						  r.getString(3), Arrays.asList(
								  new JobRecord(r.getString(2), r.getString(0), r.getString(1), r.getString(3)))); }
			});


		JavaPairRDD<String, List<JobRecord>> execByUser = pairs.reduceByKey(
				new Function2<List<JobRecord>, List<JobRecord>, List<JobRecord>>()
				{
					private static final long serialVersionUID = 1L;

					public List<JobRecord> call(List<JobRecord> one, List<JobRecord> two) {
						ArrayList <JobRecord> l1= new ArrayList<JobRecord>();
						l1.addAll(one);
						l1.addAll(two);
							return l1;
							}
				});
		
		JavaRDD<List<List<String>>> sequences = execByUser.values().map( new Function<List<JobRecord>, List<List<String>>>()
				{
				private static final long serialVersionUID = 1L;

					public List<List<String>> call(List<JobRecord> Jobs) throws Exception {
						// TODO Auto-generated method stub
						Collections.sort(Jobs);
						ArrayList <List<String>> execs= new ArrayList<List<String>>(Jobs.size());
						//remove continues repeated runs. 
						String prev=Jobs.get(0).getExec_path();
						execs.add(Arrays.asList(prev));
						for (int i=1; i<Jobs.size(); i++)
						{
							String s =Jobs.get(i).getExec_path();
							if (!s.equalsIgnoreCase(prev))
							{
									execs.add(Arrays.asList(s));
									prev=s;
							}
						}
						return execs;
					}
			
				});
		
		PrefixSpan prefixSpan = new PrefixSpan()
				  .setMinSupport(min_supp)
				  .setMaxPatternLength(max_len);
				PrefixSpanModel<String> model = prefixSpan.run(sequences);
				for (PrefixSpan.FreqSequence<String> freqSeq: model.freqSequences().toJavaRDD().collect()) {
				  System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
				}
		
	}
*/
    
	public static FPGrowthModel<String>  runAssociationAnalysis(JavaRDD<List<String>> transactions, double min_supp, double min_Conf)
    {
    	
    	 //= prepareTransactions(filtered_row, int [] selections, int by);
    	
    	FPGrowth fpg = new FPGrowth()
    			  .setMinSupport(min_supp)
    			  .setNumPartitions(10);
    			FPGrowthModel<String> model = fpg.run(transactions);
    			
    			System.out.println();
    			for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
    			  System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
    			}

    			double minConfidence = min_Conf;
    			for (AssociationRules.Rule<String> rule
    			    : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
    			  System.out.println(
    			    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    			}
    	return model;
  
    }
	
	//functions to get statistics about log.
	public String getStatistics()
	{
		long size=logs.count();
		String [] cols= logs.columns();
		StringBuffer buffer = new StringBuffer();
		buffer.append("Number of rows:\t"+ size +"\n");
		buffer.append("NUmber of columns:\t" + cols.length+"\n");
		buffer.append("column names:\t");
		for (String s: logs.columns())
			  buffer.append(s+",");
		buffer.append("\n");
		logs.printSchema();
		//logs.show();
		/*
		Row row = logs.collectAsList().get(15);
		for (int i=0; i<cols.length; i++){
			Object o = row.get(i);
			if (o!=null)
				buffer.append(i +" column: ("+o.getClass().getName()+") "+String.valueOf(o)+" \n");
		}
		*/
		return buffer.toString();
		
	}
	
	//static methods
    //existing method to return a column directly? 
	  public static List <String> getDistinctValues(JavaRDD<Row> rows, int i)
	  {
		  List<String> uniques= rows.map(new StringAt(i)).distinct().collect();
		  return uniques;
	  }

	  public static JavaRDD<List<String>> prepareTransactions (JavaRDD<Row> filtered_row, int [] selections, int by)
	  {
	//			JavaPairRDD<String, List<String>> pairs = filtered_row.mapToPair(new PairColumns(17, new int[]{5, 6}));
		  System.out.println("Begin Prepare Transactions.");
			JavaPairRDD<String, List<String>> pairs = filtered_row.mapToPair(new PairColumns<String>(by, selections));
			

			JavaPairRDD<String, List<String>> execByUser = pairs.reduceByKey(
					new Function2<List<String>, List<String>, List<String>>()
					{
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						public List<String> call(List<String> one, List<String> l2) {
							ArrayList <String> l1= new ArrayList<String>();
							l1.addAll(one);
							for (String s2: l2)
							{
								if (s2 ==null) break;
								boolean found=false;
								for (String s1: l1)
								{
									if (!found && s1 !=null && s1.equalsIgnoreCase(s2))
									{
										found=true;
										break;
									}
								}
								if(!found) l1.add(s2);
								else found=false;
							}
								return l1;
								}
					});
			 System.out.println("Finish Prepare Transactions.");
			return execByUser.values();
			
		}  
	  
	  public static String aggreator_query(StructType schema, String tn, String [] key_cols, String [] val_cols){
		  String query="";
		  //validataion 
		  for (String s: key_cols){
			  if (schema.apply(s) == null){
				  System.out.println("Error: invalid column name "+s);
				  return null;
			  }
		  }
		  for (String s: val_cols){
			  if (schema.apply(s) == null){
				  System.out.println("Error: invalid column name "+s);
				  return null;
			  }
		  }
		  String key_type = schema.apply(key_cols[0]).dataType().typeName(); 
		  if (key_type.equalsIgnoreCase("array")){
			  query ="SELECT elem, ";
			  for (int i=1; i<key_cols.length; i++)
				  query+=key_cols[i]+", ";
			  for (String s : val_cols)
				  query += "SUM("+s+") as sum_"+s+", ";
			  query += " count(*) as size";
			  query += " FROM "+tn +" LATERAL VIEW explode("+key_cols[0]+") tmpTable as elem ";
			  query += " GROUP BY elem" ;
			  for (int i=1; i<key_cols.length; i++)
				  query+=", "+key_cols[i];  
		  }else {
			  query ="SELECT "; 
			  for (String s: key_cols)
				  query += s+ ", ";
			  for (String s : val_cols)
				  query += "SUM("+s+") as sum_"+s+", ";
			  query += " count(*) as size";
			  query += " FROM "+tn;
			  query += " GROUP BY "+key_cols[0]; 
			  for (int i=1; i<key_cols.length; i++)
				  query+=", "+key_cols[i];  
		  }
		
		  return query;
	  }
	  
	  public static String getDistributionMatrix(JavaRDD<Row> filtered_row, int row, int col, String [] col_names)
	  {	//note the WrappedArray is not yet working here. 
		  JavaPairRDD<String, HashMap<String, Integer>> pairs = filtered_row.mapToPair(new PairColumnCount(row, col));
		  JavaPairRDD<String, HashMap<String, Integer>> execByUser = pairs.reduceByKey(
					new Function2<HashMap<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>>()
					{
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						public HashMap<String, Integer> call(HashMap<String, Integer> one, HashMap<String, Integer> l2) {
							HashMap<String, Integer>  l1= new HashMap<String, Integer>();
							l1.putAll(one);;
							for (String s2: l2.keySet())
							{
								boolean found=false;
								if (l1.containsKey(s2)){
									Integer count=l2.get(s2)+l1.get(s2);
									l1.put(s2, count);
								}else
									l1.put(s2, l2.get(s2));
								
							}
								return l1;
						}});
		 List<Tuple2<String, HashMap<String, Integer>>> result= execByUser.collect();
		 
		 StringBuffer buffer = new StringBuffer();
		 buffer.append("name ");
		 for (String s: col_names)
			 buffer.append("\t"+s);
		 buffer.append("\n");
		 
		 for (Tuple2 t : result)
		 {
			 String name=(String) t.productElement(0);
			 HashMap <String, Integer>hm =(HashMap<String, Integer>) t.productElement(1);
			 buffer.append(t.productElement(0));
			 for (String s : col_names)
			 {
				 if (hm.containsKey(s))
					 buffer.append("\t"+ hm.get(s));
				 else 
					 buffer.append("\t0");
			 }
			 buffer.append("\n");
			 			 
		 }
		 
		 System.out.println(buffer.toString());
		 return (buffer.toString());
		
		 
	  }
	  
	  public static class filterBySize implements Function<List<String>, Boolean>, Serializable{
		  int length;
		private static final long serialVersionUID = 1L;
		  
		  public filterBySize(int length)
		  {
			 this.length=length;
		  }

		public Boolean call(List<String> l) throws Exception {
			// TODO Auto-generated method stub
			return l.size()>length;
		}

	  }
	//get Unique values from a column
	public int getUniqueValues(JavaRDD<Row>rows, int i){
		return getDistinctValues(rows, i).size();
	}
	public void getDistributionMatrix(JavaRDD<Row> rows, int row, int col){
		String result=getDistributionMatrix(rows, row, col, getDistinctValues(rows, col).toArray(new String[]{}));
		 try{
			 String fs = System.getProperty("file.separator");
				String op= out_path+fs+"distribution.txt";
			 FileWriter fw = new FileWriter(op);
			 fw.write(result);
			 fw.flush();
			 fw.close();
		 }catch(Exception e)
		 {
			 System.out.println(e.toString());
		 }
	}
	
	/*
	 * rows			input data @javaRDD<Row>
	 * values 		index[] for values to be selected. 
	 * bucket 		column  index for aggregation. 
	 * min_size 	minimum of size per transaction, eg. 2 to avoid all bucket only has one item. 
	 * support 		minimum	support value for frequent item set
	 * confidence 	minimum confidence level for inference rules 
	 */
	public void associationAnalysis(JavaRDD<Row> rows, int[] values, int bucket, int min_size, double support, double confidence){
		JavaRDD<List<String>>trans = prepareTransactions(rows, values, bucket).filter(new filterBySize(min_size));
		FPGrowthModel<String> model=runAssociationAnalysis(trans, support, confidence);
		String fs = System.getProperty("file.separator");
		String op= out_path+fs+"rules.pmml.xml";
		ModelTransformation.writeStringtoFile(op, ModelTransformation.toPMML(model, trans.count(), support, confidence));

	}
	
	public Dataset<Row> getDataFrame(String query){
		return this.spark.sql(query);
	}
	
/*	
	//
	//case specific functions. 
	//
	
	public void runAnalysis()
	{
		//System.out.println(filtered_row.collect().get(0).toString());
		// [null,A00371462,unknown,unknown,2014-07-07 15:32:47,a.out,Advanced Scientific Computing (ASC),stampede,3636927,
		//WrappedArray([null,/lib64/ld-2.12.so], ...),
		//unknown,null,16,1,1,1.66,1.40476516779E9,U00816301]
		
			//run Assoication analysis. 
			
			JavaRDD<Row> filtered_row = logs.javaRDD();
	
			JavaRDD<List<String>> trans = null;
			//aggregate job_id, exec_path per job_id, Jan 18 2016. 
			trans = prepareTransactions(filtered_row, new int[]{5, 8}, 8);
		
			//run analysis on rows with more than one executable only 
			runAssociationAnalysis(trans.filter(new filterBySize(2)), 0.001, 0.5);
			//runAssociationAnalysis(trans, 0.001, 0.5);
			
			System.out.println("total trans:" + trans.count());
			int [] counts = new int[10];
			for (List<String> l : trans.collect())
			{
				if (Math.random()<0.0001) {
					for (String s : l)
						System.out.print(s+",");
				System.out.println();
				}
				if (l.size()<counts.length && !l.get(1).equalsIgnoreCase("unknown"))
					counts[l.size()-1]++;
				else 
					System.out.println("size of the list is too big "+ l.size()+","+l);
			}
		
			for (int i =0; i< counts.length; i++)
				if (counts[i]>0) System.out.println(i+","+counts[i]);
			System.out.println();
			//end of Jan 18, 2016
			
	}
	*/	
	public void aggreator(String [] key_col, String[] val_cols, String output) {
		// TODO Auto-generated method stub
		String tn ="dt";
		String query = LogAnalyzer.aggreator_query(this.logs.schema(), tn, key_col, val_cols);
		logs.createOrReplaceTempView(tn);
		Dataset<Row> res = spark.sql(query);
		res.show();
		res.write().format("csv").option("header", "true").save(output+"_csv");
		res.write().format("parquet").save(output+"_parquet");
		
	}	
	public static void main (String [] args){
		
		 //setting up log output settings. 
		  Logger.getLogger("org").setLevel(Level.OFF);
		  Logger.getLogger("akka").setLevel(Level.OFF);
		  


	}

	

}

