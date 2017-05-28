package xalt;
import java.io.Serializable;
import java.util.Date;

public class JobRecord implements Comparable, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String exec_path;
	String time_string;
	String user;
	String field;
	
	public JobRecord(String t, String e, String f,  String u )
	{
		exec_path=e;
		time_string=t;
		user=u;
		field=f;
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return compareTo((JobRecord) o);
	}
	
	public int compareTo(JobRecord o) {
		// TODO Auto-generated method stub
		return time_string.compareTo(o.getTime_string());
		
	}

	public String getExec_path() {
		return exec_path;
	}

	public void setExec_path(String exec_path) {
		this.exec_path = exec_path;
	}

	public String getTime_string() {
		return time_string;
	}

	public void setTime_string(String time_string) {
		this.time_string = time_string;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	
}
