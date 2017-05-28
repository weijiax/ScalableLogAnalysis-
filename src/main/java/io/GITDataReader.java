package io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.DataFrames;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructType;

import apps.LogAnalyzer;

import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import org.apache.spark.api.java.function.*;
import static org.apache.spark.sql.functions.col;

/*
 * class to ingest data as data frames. 
 * The data may comes as data, and schema files. 
 * Text preproccesing steps may be added. 
 * 
 */
public class GITDataReader {
	
	public static Dataset<Row> loadDataFromJson2(SparkSession spark,  JavaSparkContext sc, String data){
		
		Dataset<GITData> records = spark.read().json(data).flatMap(
				new  FlatMapFunction<Row, GITData> () {
					public Iterator<GITData> call(Row objs) throws Exception {
						ArrayList<GITData> res = new ArrayList<GITData>();
						for (Object obj : objs.getList(0) ){
						//System.out.println(obj);
							String [] wa = new String[((WrappedArray) obj).length()];
							((WrappedArray) obj).copyToArray(wa, 0);
							res.add(new GITData(wa));
						}
						return res.iterator();
					}}
		, Encoders.bean(GITData.class)); 
		return records.toDF();
	}

	public static Dataset<Row> loadDataFromJson(SparkSession spark,  JavaSparkContext sc, String data){
		
		JavaRDD<GITData> records = spark.read().json(data).toJavaRDD().flatMap(
				new  FlatMapFunction<Row, Object> () {
					public Iterator<Object> call(Row objs) throws Exception {
						return objs.getList(0).iterator();
					}}
		).map(
				new Function<Object, GITData>(){
					@Override
					public GITData call(Object obj) throws Exception {
						String [] wa = new String[((WrappedArray) obj).length()];
						((WrappedArray) obj).copyToArray(wa, 0);
						return new GITData(wa);
					}
					
				}); 
		return spark.createDataFrame(records, GITData.class);
	}


	public static Dataset<Row> loadDataFromParquet(SparkSession spark,  String data){
		System.out.println("Reading data from parquet files");
		return spark.read().load(data);
		
	}
	public static Dataset<Row> loadData(SparkSession spark, String schema, String data){
		
		
		StructType st = readMySQLSchema(schema);
		Dataset<Row> df = spark.read().format("com.databricks.spark.csv")
				.option("header", "false").option("quote", "'").option("nullValue", "NULL")
				.schema(st).csv(data);
		df.printSchema();

		System.out.println("Records with missing columns: "+ df.select(col("jobid")).filter(col("pmem_req").isNull()).count());
		return df.filter(col("pmem_req").isNotNull());	
	}
	
	
	public static JavaRDD<Row> readMySQLDump(String file, JavaSparkContext sc){
		
		JavaRDD<String> textFile = sc.textFile(file);
		JavaRDD<Row> rows = (JavaRDD<Row>) textFile.map(
				new  Function<String, Row> () {
					public Row call(String line) throws Exception {
						return RowFactory.create(Arrays.asList(line.split(",")));
					}}
		);
		return rows; 
	}
	
	/*
	 *  Read mysql dump schema from text files 
	 *  mysql> desc Jobs
    -> ;
+--------------+------------------+------+-----+----------+-------+
| Field        | Type             | Null | Key | Default  | Extra |
+--------------+------------------+------+-----+----------+-------+
| jobid        | varchar(32)      | NO   | PRI | NULL     |       |
| system       | varchar(32)      | YES  | MUL | NULL     |       |
| username     | varchar(32)      | YES  | MUL | NULL     |       |
| groupname    | varchar(32)      | YES  | MUL | NULL     |       |
...

Assume all line not start with |  could be ignored and the schema file always have a header, 
	 */
	public static StructType readMySQLSchema(String file){
		
		StructType schema = new StructType(); 
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    String [] header =null; 
		    while ((line = br.readLine()) != null) {
		       // process the line.
		    	
		    	if (line.startsWith("|"))
		    	{
		    		
		    		if (header == null)
		    			header = line.split("\\|"); 
		    		else {
		    			String [] cols =  line.split("\\|"); 
		    			//the first column should be empty and only use the 1 (name), 2 (type), 3 (nullable?) column, 
		    			schema=schema.add(cols[1].trim(), getDataTypeFromString(cols[2].trim()), cols[3].trim().equalsIgnoreCase("YES"));	
		    		}
		    	}else 
		    		continue; 
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return schema;
		
	}
	
	//note: 
	//need to treat int(11) as timestamp, 
	//need to treat time type as String 
	//TODO: parse time interval correctly later? 
	public static DataType getDataTypeFromString(String str){
		return DataTypes.StringType; 
		/*
		System.out.println(str);
		DataType dt = DataTypes.StringType; 
		if (str.startsWith("int(10)"))
			dt = DataTypes.IntegerType;
		else if (str.startsWith("date"))
			dt = DataTypes.DateType;
		else if (str.startsWith("time"))
			dt = DataTypes.StringType;
		else if (str.startsWith("int(11)"))
			dt = DataTypes.IntegerType;
		else 
			dt = DataTypes.StringType; 
		return dt; 
		*/
	}


	public static void main (String []args){
		 //SparkConf conf = new SparkConf().setAppName("XALTLogAnalyzer");
		 //conf.setMaster("local");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		 
		 SparkSession spark = SparkSession
				  .builder()
				  .appName("Loa Analysis ").master("local[4]")
				  .getOrCreate();		
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		String fn ="/Users/xwj/Documents/Xalt/GIT/schema"; 

		//Dataset<Row> ds = loadData(spark, fn, "/Users/xwj/Documents/Xalt/GIT/synthetic.data.gatech.v1");
		Dataset<Row> ds = loadDataFromJson(spark, sc, "/Users/xwj/Documents/Xalt/GIT/test2.json");
		ds.printSchema();
		ds.createOrReplaceTempView("logs");
		spark.sql("select * from logs");
		String [] vals = {"cput", "mem_kb"};
		String query = LogAnalyzer.aggreator_query(ds.schema(), "logs", new String[]{"groupname"}, vals); 
		System.out.println(query); 
		spark.sql(query).show(); 
		
		query = LogAnalyzer.aggreator_query(ds.schema(), "logs", new String[]{"module_array"}, vals); 
		System.out.println(query); 
		//spark.sql(query).show(); 
		
		//System.out.println("number of valid records: "+ ds.count());
		//ds.registerTempTable("logs");
		//ds.show(); 
		/*
		//Seq records =  ds.head().getSeq(0);
		JavaRDD<Row> records = sc.parallelize(ds.head().getList(0)).map(
				new  Function<Object, Row> () {
					public Row call(Object obj) throws Exception {
						//System.out.println(((WrappedArray) obj).toList());
						//return Row.fromSeq(((WrappedArray) obj).toSeq());
						String [] wa = new String[((WrappedArray) obj).length()];
						((WrappedArray) obj).copyToArray(wa, 0);
						//return wa;
						return RowFactory.create(wa);
					}}
		);; 
		
		System.out.println(records.count());
		System.out.println(records.first());
		//for (Object s: records.first()) System.out.println(s);
		
		StructType st = readMySQLSchema("/Users/xwj/Documents/Xalt/GIT/schema2");
		Dataset<Row> df = spark.createDataFrame(records, st);
		df.show();
		df.createOrReplaceTempView("logs");
		Dataset<Row> df2 = spark.sql("select jobid, groupname, nproc, nodes, queue, cast(submit_ts as bigint), to_date(submit_date) as submit_date, cast(start_ts as bigint), "
				+ "to_date(start_date) as start_date, cast(end_ts as bigint), to_date(end_date) as end_date, "
				+ "cast(cput as int), cast(walltime as int), cast(mem_kb as int), cast(vmem_kb as int), software,hostlist,exit_status, split(script, 'module load') as module_list, sw_app from logs");
		df2.printSchema();
		df2.show();
		List<Row> modules = df2.select(col("module_list")).collectAsList();
		for( Row r : modules){
			List l = r.getList(0);
			for (int i=1; i<l.size()-1; i++)
				System.out.print(l.get(i)+"\t");
			System.out.println();
		}
		*/
		
		//System.out.println(ds.select("records").select("element").count());
		
		//ds.sample(true, 0.5).show();
		
/*		SparkConf conf = new SparkConf().setAppName("XALTLogAnalyzer");
		conf.setMaster("local");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 
		 SparkSession spark = SparkSession
				  .builder()
				  .appName("Loa Analysis ")
				  .getOrCreate();
		  
		String fn ="/Users/xwj/Documents/Xalt/GIT/schema"; 
		StructType st = readMySQLSchema(fn);
		//System.out.println(st.simpleString());
		st.printTreeString();
		
		JavaRDD<Row> rows = readMySQLDump("/Users/xwj/Documents/Xalt/GIT/synthetic.data.gatech.v1", sc);
		System.out.println(rows.count());
		Iterator iter = rows.collect().iterator();
		while (iter.hasNext())
			System.out.println(((Row) iter.next()).length());
		
		Dataset<Row> df = spark.read().format("com.databricks.spark.csv")
				.option("header", "false").option("quote", "'").option("nullValue", "NULL")
				.schema(st).csv("/Users/xwj/Documents/Xalt/GIT/synthetic.data.gatech.v1");
		df.printSchema();
		
		//df.sample(true, 0.5).show();
		df.select(col("jobid"), col("system"), col("username"), col("groupname"), col("nproc")).filter(col("nproc").gt(1)).show();
		System.out.println("Records with missing columns: "+ df.select(col("jobid")).filter(col("pmem_req").isNull()).count());
		df.filter(col("pmem_req").isNotNull()).sample(true, 0.5).show();
*/
		
	}
}
