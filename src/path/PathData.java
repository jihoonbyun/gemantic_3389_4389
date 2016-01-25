package path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PathData implements Writable  {

	String site_url;	
	String date;
	String userid;
//	String country;
	Link_Data linkdata[];
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		int length;
		this.site_url = in.readUTF();
		this.userid = in.readUTF();
		this.date = in.readUTF();
//		this.country = in.readUTF();
		length = in.readInt();
		this.linkdata = new Link_Data[length];
		
		for(int i=0;i<length;i++){
			linkdata[i] = new Link_Data();
			linkdata[i].link_name = in.readUTF();
			linkdata[i].touchdata_key = in.readUTF();
			linkdata[i].destination_average_time = in.readInt();
			linkdata[i].timehost_average_time = in.readLong();
			linkdata[i].timepage_average_time = in.readLong();
			linkdata[i].events_average_time = in.readLong();
			
			linkdata[i].destination_totalcount= in.readInt();
			linkdata[i].timehost_totalcount= in.readInt();
			linkdata[i].timepage_totalcount= in.readInt();
			linkdata[i].events_totalcount= in.readInt();
			
			
		}
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		out.writeUTF(site_url);
		out.writeUTF(userid);
		out.writeUTF(date);
//		out.writeUTF(country);
		out.writeInt(linkdata.length);

		for(int i=0;i<linkdata.length;i++){
			if(linkdata[i] == null){
				
			}
			
			out.writeUTF(linkdata[i].link_name);
			out.writeUTF(linkdata[i].touchdata_key);
			out.writeInt(linkdata[i].destination_average_time);
			out.writeLong(linkdata[i].timehost_average_time);
			out.writeLong(linkdata[i].timepage_average_time);
			out.writeLong(linkdata[i].events_average_time);
			
			out.writeInt(linkdata[i].destination_totalcount);
			out.writeInt(linkdata[i].timehost_totalcount);
			out.writeInt(linkdata[i].timepage_totalcount);
			out.writeInt(linkdata[i].events_totalcount);
	
			
		}
	}
	
}
