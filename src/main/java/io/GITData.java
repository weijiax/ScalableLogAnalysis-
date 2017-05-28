package io;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

public class GITData implements Serializable {
	String jobid; 
	String groupname; 
	int nproc; 
	String nodes;
	String queue;
	long submit_ts; 
	String submit_date; 
	long start_ts;
	String start_date;
	long end_ts; 
	String end_date; 
	long cput; 
	long walltime;
	long mem_kb;
	long vmem_kb;
	String software; 
	String hostlist; 
	String exit_status; 
	String script; 
	String sw_app; 
	
	String [] hostlist_array; 
	String [] module_array;
	
	static DateFormat format = new SimpleDateFormat("yyyy-mm-dd", Locale.ENGLISH);
	
	public GITData(String [] val){
		for (String s: val){
		//	System.out.println(s);
		}
	  try {
		this.jobid = val[0]; 
		this.groupname = val[1]; 
		this.nproc = Integer.parseInt(val[2]); 
		this.nodes = val[3];
		this.queue = val[4];
		this.submit_ts = Long.parseLong(val[5]); 
		//this.submit_date = format.parse(val[6].trim());
		this.submit_date =val[6].trim();
		this.start_ts = Long.parseLong(val[7]); 
		//this.start_date = format.parse(val[8]);
		this.start_date = val[8];
		this.end_ts = Long.parseLong(val[9]);  
		//this.end_date = format.parse(val[10].trim());
		this.end_date = val[10].trim();
		this.cput = Long.parseLong(val[11]); 
		this.walltime = Long.parseLong(val[12]);
		this.mem_kb = Long.parseLong(val[13]);
		this.vmem_kb = Long.parseLong(val[14]);
		this.software = val[15]; 
		this.hostlist = val[16]; 
		this.exit_status = val[17]; 
		this.script =val[18]; 
		this.sw_app=val[19]; 
		hostlist_array = getHosts(hostlist);
		module_array = getModules(script);
	  } catch (Exception e) {
			// TODO Auto-generated catch block
		  for (String s: val){
				System.out.println(s);
			}
			e.printStackTrace();
	  } 
	}
	
	public static String [] getModules(String val){
		String [] lines = val.split("\\r?\\n"); 
		//System.out.println("number of lines:"+ lines.length);
		ArrayList<String> res = new ArrayList<String>();
		for (String line: lines){
			if (line ==null || line.isEmpty() || line.trim().indexOf("module load") != 0)
				continue;
			String [] tem = line.split("module load"); 
			if (tem!=null){
				for (int i=1; i< tem.length; i++)
					for (String s: tem[i].trim().split(" "))
						res.add(s);
			}
			
		}
		return res.toArray(new String[]{});
		//return null;
	}
	
	public static String [] getHosts(String val){
		//expected format
		//iw-c39-34-l.pace.gatech.edu/2-3,10-11
		//updated so only return ip address,
		ArrayList<String> hosts = new ArrayList<String>();
		String [] hostlist = val.split("\\+");
		for (String host : hostlist){
			String [] parts = host.split("\\/");
			hosts.add(parts[0]);
		}
		return hosts.toArray(new String[1]);
	}
	
	public static String [] getAllHosts(String val){
		//expected format
		//iw-c39-34-l.pace.gatech.edu/2-3,10-11
		//expand the complete list. 
		ArrayList<String> hosts = new ArrayList<String>();
		String [] hostlist = val.split("\\+");
		for (String host : hostlist){
		String [] parts = host.split("\\/");
		String [] range = parts[1].split(","); 
		for ( String s: range){
			
			String [] seq = s.split("-"); 
			if (seq.length==1)
				hosts.add(parts[0]+"/"+seq[0]);
			if (seq.length==2){
				int beg = Integer.parseInt(seq[0].trim());
				int end = Integer.parseInt(seq[1].trim()); 
				for (int i=beg; i<=end; i++)
					hosts.add(parts[0]+"/"+i);
			}
				
		}
		}
		return hosts.toArray(new String[1]);
	}
	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getGroupname() {
		return groupname;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public int getNproc() {
		return nproc;
	}

	public void setNproc(int nproc) {
		this.nproc = nproc;
	}

	public String getNodes() {
		return nodes;
	}

	public void setNodes(String nodes) {
		this.nodes = nodes;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public long getSubmit_ts() {
		return submit_ts;
	}

	public void setSubmit_ts(long submit_ts) {
		this.submit_ts = submit_ts;
	}

	public String getSubmit_date() {
		return submit_date;
	}

	public void setSubmit_date(String submit_date) {
		this.submit_date = submit_date;
	}

	public long getStart_ts() {
		return start_ts;
	}

	public void setStart_ts(long start_ts) {
		this.start_ts = start_ts;
	}

	public String getStart_date() {
		return start_date;
	}

	public void setStart_date(String start_date) {
		this.start_date = start_date;
	}

	public long getEnd_ts() {
		return end_ts;
	}

	public void setEnd_ts(long end_ts) {
		this.end_ts = end_ts;
	}

	public String getEnd_date() {
		return end_date;
	}

	public void setEnd_date(String end_date) {
		this.end_date = end_date;
	}

	public long getCput() {
		return cput;
	}

	public void setCput(long cput) {
		this.cput = cput;
	}

	public long getWalltime() {
		return walltime;
	}

	public void setWalltime(long walltime) {
		this.walltime = walltime;
	}

	public long getMem_kb() {
		return mem_kb;
	}

	public void setMem_kb(long mem_kb) {
		this.mem_kb = mem_kb;
	}

	public long getVmem_kb() {
		return vmem_kb;
	}

	public void setVmem_kb(long vmem_kb) {
		this.vmem_kb = vmem_kb;
	}

	public String getSoftware() {
		return software;
	}

	public void setSoftware(String software) {
		this.software = software;
	}

	public String getHostlist() {
		return hostlist;
	}

	public void setHostlist(String hostlist) {
		this.hostlist = hostlist;
	}

	public String getExit_status() {
		return exit_status;
	}

	public void setExit_status(String exit_status) {
		this.exit_status = exit_status;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getSw_app() {
		return sw_app;
	}

	public void setSw_app(String sw_app) {
		this.sw_app = sw_app;
	}

	public String[] getHostlist_array() {
		return hostlist_array;
	}

	public void setHostlist_array(String[] hostlist_array) {
		this.hostlist_array = hostlist_array;
	}

	public String[] getModule_array() {
		return module_array;
	}

	public void setModule_array(String[] module_array) {
		this.module_array = module_array;
	}

	public static DateFormat getFormat() {
		return format;
	}

	public static void setFormat(DateFormat format) {
		GITData.format = format;
	}

	static void test_getModules(String input){
		String [] res = getModules(input); 
		System.out.println("Input is: "+ input); 
		System.out.println("output is: ");
		for (String s: res)
			System.out.print(s+", "); 
		System.out.println();
	}
	
	static void test_getHosts(String input){
		String [] res = getHosts(input); 
		System.out.println("Input is: "+ input); 
		System.out.println("output is: ");
		for (String s: res)
			System.out.print(s+", "); 
		System.out.println();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test1 ="#PBS -q prometheus \n#PBS -N test\n#PBS -j oe\n#PBS -o cherry.test\n#PBS -l nodes=2:ppn=4\n#PBS -l walltime=15:00\n\ncd $PBS_O_WORKDIR\nmodule purge\nmodule load intel/15.0\nmodule load mvapich2/2.1\nmodule load abaqus gcc/4.7.2 mvapich2/2.1 hdf5/1.8.14 netcdf/4.3.3 mkl/11.2 fftw/3.3.4 abinit/6.12.3\nmodule list\n\nmpicc -o test test.c        \n\nmpirun -np 8 ./test\n";
		test_getModules(test1);
		
		String test2 = "iw-c39-34-l.pace.gatech.edu/2-3,10-11";
		String test3 = "iw-c39-34-l.pace.gatech.edu/5-8";
		String test4 = "iw-c39-34-l.pace.gatech.edu/12,13";
		String test5 = "iw-c39-34-l.pace.gatech.edu/12";
		String test6 = "iw-h31-14.pace.gatech.edu/52,57,59+iw-h31-15.pace.gatech.edu/54";
		test_getHosts(test2);
		test_getHosts(test3);
		test_getHosts(test4);
		test_getHosts(test5);
		test_getHosts(test6);

	}

}
