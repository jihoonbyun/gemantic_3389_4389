package path;

public class Path_Analysis {

	public Result_Path anlaysis(String key, String date, Link_Data[] linkdatas){
		
		Result_Path result = new Result_Path(date);
		
		int length = linkdatas.length;
		
		for(int i=0;i<length;i++){
			//System.out.println("start point = " + linkdatas[i].link_name);
			if(i != length-1)
				result.push(linkdatas[i+1].link_name,linkdatas[i].link_name,1);		
			
			if(linkdatas[i].destination_average_time != -1){//목표가 있는 링크라면
				result.destination_set(linkdatas[i].touchdata_key, linkdatas[i].destination_average_time, linkdatas[i].destination_totalcount);	
				System.out.println("out of dest = " + linkdatas[i].touchdata_key);
			}
			if(linkdatas[i].timehost_average_time != -1){//목표가 있는 링크라면
				result.timehost_set(linkdatas[i].touchdata_key, linkdatas[i].timehost_average_time, linkdatas[i].timehost_totalcount);	
				System.out.println("out of timshost = " + linkdatas[i].touchdata_key);
			}
			if(linkdatas[i].timepage_average_time != -1){//목표가 있는 링크라면
				result.timepages_set(linkdatas[i].touchdata_key, linkdatas[i].timepage_average_time, linkdatas[i].timepage_totalcount);	
				System.out.println("out of timshost = " + linkdatas[i].touchdata_key);
			}	
			
			if(linkdatas[i].events_average_time != -1){//목표가 있는 링크라면
				result.events_set(linkdatas[i].touchdata_key, linkdatas[i].events_average_time, linkdatas[i].events_totalcount);	
				System.out.println("out of event = " + linkdatas[i].touchdata_key);
			}
		}
		
		
		if(length == 1){//단 하나, 이탈
			System.out.println("bounce!");
			System.out.println(linkdatas[0].touchdata_key);
			
			String[] list = linkdatas[0].touchdata_key.split("\\|");
			String[] url_key;
			
			for(String keylist : list){
				System.out.println("url-key = " + keylist);
				url_key = keylist.split(",");
				if(keylist.length() == 0)
					continue;
				System.out.println("in bounce key - " + url_key[1]);
				result.bounce_set(url_key[1]);
			}
			
			//result.bounce_set(linkdatas[0].touchdata_key);
		}
		else{
			System.out.println("close!");
			System.out.println(linkdatas[length-1].touchdata_key);
			String[] list = linkdatas[length-1].touchdata_key.split("\\|");
			String[] url_key;
			
			for(String keylist : list){
				System.out.println("url-key = " + keylist);
				if(keylist.length() == 0)
					continue;
				url_key = keylist.split(",");
				System.out.println("in close key - " + url_key[1]);
				result.close_set(url_key[1]);
			}
			
			//result.close_set(linkdatas[length-1].touchdata_key);
		}
		
		return result;
	}
	
}
