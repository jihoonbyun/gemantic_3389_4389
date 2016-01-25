package path;

public class Convergence_Data {

	public String touchdatakey;
	public long elapsedtime = 0;
	public int totalcount = 0;
	
	Convergence_Data(String key, long elapsedtime, int totalcount){
		touchdatakey = key;
		this.elapsedtime = elapsedtime;
		this.totalcount = totalcount;
	
	}
	
	Convergence_Data(){
		touchdatakey = "";
		this.elapsedtime = 0;
		this.totalcount = 0;
		
	}
	
}
