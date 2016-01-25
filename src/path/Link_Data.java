package path;

public class Link_Data {

	String link_name;	//태그의 이름. url+태그번호
	String touchdata_key;	//연관된 터치데이터묶음의 키 번호
	
	int destination_average_time;
	long timehost_average_time;
	long timepage_average_time;
	long events_average_time;
	
	int destination_totalcount;
	int timehost_totalcount;
	int timepage_totalcount;
	int events_totalcount;
	
	
	Link_Data(){
		destination_average_time = -1;
		timehost_average_time = -1;
		timepage_average_time = -1;
		events_average_time = -1;
		
		destination_totalcount=0;
		timehost_totalcount=0;
		timepage_totalcount=0;
		events_totalcount=0;
		
	}

}

