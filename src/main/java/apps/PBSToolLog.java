package apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import io.GITDataReader;
import apps.LogAnalyzer;

public class PBSToolLog {

	public static void help(){
		System.out.println("usage:");
		System.out.println("-f (required) path to the data file(s)");
		System.out.println("-b (required) the col index for aggreagting values");
		System.out.println("-v (required) the col index(s) of values to be aggregated, seperated by comma");
		System.out.println("-a Run association analysis");
		System.out.println("-s Run sequential pattern analysis");
		System.out.println("-d Get distribution matrix between bucket index and first value index");
		System.out.println("-sup minimum support value used in association analysis and sequential pattern analysis");
		System.out.println("-conf minimum confidence value used in association analysis");
		System.out.println("-size minimum size of items per transaction in association analysis");
		System.out.println("-len maximum sequences to consider in sequential pattern analysis");
		System.out.println("-out the output directory");
		System.out.println("-debug the level for debug information, 0 is off");

		System.out.println("-h Show help messages");
	}
	
	//in	a comma seperated list of indexes)
	public static int [] getIndexs(String in){
		String [] sa =in.split(",");
		int [] v = new int[sa.length];
		for (int i=0; i<sa.length; i++)
			v[i]=Integer.parseInt(sa[i]);
		return v;
	}
	
	public static void main (String [] args) throws ParseException{
		
		String dataPath="/Users/xwj/Documents/Xalt/GIT/json/";
		//String dataPath="/Users/xwj/Documents/Xalt/GIT/out/dataframe_parquet/";
		
		//to ignore private executable. 
		//String query ="SELECT * from Logs where LENGTH(exec_path)<20 and exec_path IS NOT NULL order by date";
		
		int bucket=4;//2 is username, 3 group name;//Integer.MIN_VALUE;
		int [] values=new int []{9, 19};//Integer.MAX_VALUE;
		boolean GET_STATISTICS=true;
		boolean RUN_DISTRIBUTION =false;
		boolean RUN_ASSOCIATION=false;
		boolean RUN_SEQPATTERN=false;
		boolean RUN_AGGREGATOR =false;
		boolean EXPORT=false; //Weather or not to save the dataframe as Parquet file. 
		boolean PARQUET=false; 
		
		String filter_col="submit_date";
		int debug_level=0;
		String begin_date="";
		String end_date="";
		double support=0.1;
		double confidence=0.5;
		int min_size=2;
		int max_len=5;
		String [] key_col ={"hostlist_array", "groupname", "queue"};
		String [] val_cols={"cput", "walltime", "mem_kb"};
		String output="/Users/xwj/Documents/Xalt/GIT/out";
		if (debug_level==0){
			  Logger.getLogger("org").setLevel(Level.OFF);
			  Logger.getLogger("akka").setLevel(Level.OFF);
		}
			
		for (int i=0; i<args.length; i++)
		{
			try{
				if (args[i].equalsIgnoreCase("-f"))
					dataPath=args[++i];
				else if (args[i].equalsIgnoreCase("-s"))
					RUN_SEQPATTERN=RUN_SEQPATTERN?false:true;
				else if (args[i].equalsIgnoreCase("-d"))
					RUN_DISTRIBUTION=RUN_DISTRIBUTION?false:true;
				else if (args[i].equalsIgnoreCase("-a"))
					RUN_ASSOCIATION=RUN_ASSOCIATION?false:true;	
				else if (args[i].equalsIgnoreCase("-export"))
					EXPORT=EXPORT?false:true;	
				else if (args[i].equalsIgnoreCase("-agg"))
					RUN_AGGREGATOR=RUN_AGGREGATOR?false:true;
				else if (args[i].equalsIgnoreCase("-b"))
					bucket=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-v"))
					values=getIndexs(args[++i]);
				else if (args[i].equalsIgnoreCase("-sup"))
					support=Double.parseDouble(args[++i]);
				else if (args[i].equalsIgnoreCase("-conf"))
					confidence=Double.parseDouble(args[++i]);
				else if (args[i].equalsIgnoreCase("-size"))
					min_size=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-len"))
					max_len=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-debug"))
					debug_level=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-out"))
					output=args[++i];
				else if (args[i].equalsIgnoreCase("-filter"))
					filter_col=args[++i];
				else if (args[i].equalsIgnoreCase("-beg"))
					begin_date=args[++i];
				else if (args[i].equalsIgnoreCase("-end"))
					end_date=args[++i];
				else if (args[i].equalsIgnoreCase("-key"))
					key_col=args[++i].split(",");
				else if (args[i].equalsIgnoreCase("-val"))
					val_cols=args[++i].split(",");
				else if (args[i].equalsIgnoreCase("-parquet"))
					PARQUET = PARQUET?false:true;	
				else if (args[i].equalsIgnoreCase("-h"))
					help();
				else {
					System.out.println("Invalid Option: "+args[i]+" use -h for help");
					System.exit(-1);
				}
			}catch(Exception e)
			{
			 	System.out.println("Unrecongnized Option: "+args[i]+" use -h for help");
			 	System.exit(-1);
			}
		}
		
		//System.out.println("Finish parsing the parameters");
		
		if (dataPath==null){
			System.out.println("Require at least one data file.");
			help();
			System.exit(-1);
		}
		//Master URL is set with JVM options -Dspark.master=local[*]
		 SparkSession spark = SparkSession
				  .builder()
				  .appName("Loa Analysis ")
				  .getOrCreate();		
		 JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		 
		//version 1 data format 
		   //String schema_fn = dataPath+"/schema"; 
		   //String data_fn = dataPath+"/synthetic.data.gatech.v1";
		   //Dataset<Row> ds = GITDataReader.loadData(spark, schema_fn, data_fn);
		 
		//version 2 data format
		 Dataset<Row> df = null;
		 if (PARQUET)
			 df = GITDataReader.loadDataFromParquet(spark, dataPath);
		 else 
			 df = GITDataReader.loadDataFromJson(spark, sc, dataPath);
		 	
		System.out.println("number of valid records: "+ df.count());
		//df.select(col("hostlist_array")).show(false);
		//df.select(col("module_array")).show(false);

		if (!begin_date.isEmpty() || !end_date.isEmpty())
			if (begin_date.isEmpty())
				df = df.filter(col(filter_col).lt(end_date));
			else if (end_date.isEmpty())
				df = df.filter(col(filter_col).gt(begin_date));
			else 
				df = df.filter(col(filter_col).between(begin_date, end_date));
		
		LogAnalyzer analyzer=new LogAnalyzer(spark, df, output);

		//df.show();
		
		if (GET_STATISTICS)
			System.out.println(analyzer.getStatistics());
		if (RUN_DISTRIBUTION)
			analyzer.getDistributionMatrix(df.javaRDD(), values[0], bucket);
		if (RUN_ASSOCIATION)
			analyzer.associationAnalysis(df.javaRDD(), values, bucket, min_size, support, confidence);
		if (EXPORT)
			df.write().format("parquet").save(output+"/dataframe_parquet");
		if (RUN_AGGREGATOR)
			analyzer.aggreator( key_col, val_cols, output+"/aggregation");
		if (RUN_SEQPATTERN){
			//@TODO
			//Dataset<Row> selected = analyzer.sqlContext.sql("SELECT exec_path, field_of_science, date, user "
			//		+ "from Logs where LENGTH(exec_path)<20 and exec_path IS NOT NULL order by date" );
			//LogAnalyzer.runSeqPattern(selected, support, max_len);
			
		}
	}
}
