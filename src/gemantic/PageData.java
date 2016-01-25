package gemantic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//[핵주의] ReadField와 write의 순서가 바뀌면 안된다!!!!

public class PageData implements Writable {

	String country;
	int date;
	String url;
	String device;
	String userID;
	int bounce;
	int close;
	int complete;
	
	//이것들은 전부 elapsedtime에 해당하는것들이다
	int destination;
	long timehost;
	long timepage;
	long events;
	
	//총횟수
	int destination_totalcount;
	int timehost_totalcount;
	int timepage_totalcount;
	int events_totalcount;
	
	Touch_Data[] touchdatas;
	
	PageData(){
		bounce = 0;
		close = 0;
		destination = 0;
		timepage = 0;
		timehost = 0;
		events = 0;
		
		
		destination_totalcount = 0;
		timehost_totalcount= 0;
		timepage_totalcount= 0;
		events_totalcount= 0;

	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		int length;
		this.country = in.readUTF();
		this.date = in.readInt();
		this.url = in.readUTF();
		this.device = in.readUTF();
		this.userID = in.readUTF();
		this.bounce = in.readInt();
		this.close = in.readInt();
		this.complete=in.readInt();
		
		this.destination = in.readInt();
		this.timehost = in.readLong();
		this.timepage = in.readLong();
		this.events = in.readLong();
				
		length = in.readInt();
		
		this.destination_totalcount = in.readInt();
		this.timehost_totalcount= in.readInt();
		this.timepage_totalcount= in.readInt();
		this.events_totalcount= in.readInt();

		

		
		this.touchdatas = new Touch_Data[length];

		for(int i=0;i<length ; i++){
			this.touchdatas[i] = new Touch_Data();
			this.touchdatas[i].client.x = in.readInt();
			this.touchdatas[i].client.y = in.readInt();
			this.touchdatas[i].page.x = in.readInt();
			this.touchdatas[i].page.y = in.readInt();
			this.touchdatas[i].gesture = in.readInt();
			this.touchdatas[i].type = in.readInt();
			this.touchdatas[i].time = in.readLong();
			this.touchdatas[i].time_mili = in.readInt();
			this.touchdatas[i].push_tag = in.readInt();			
		}
		
		

		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		out.writeUTF(country);
		out.writeInt(date);
		out.writeUTF(url);
		out.writeUTF(device);
		out.writeUTF(userID);
		out.writeInt(bounce);
		out.writeInt(close);
		out.writeInt(complete);
		
		out.writeInt(destination);
		out.writeLong(timehost);
		out.writeLong(timepage);
		out.writeLong(events);
		

		out.writeInt(touchdatas.length);
		
		
		out.writeInt(destination_totalcount);
		out.writeInt(timehost_totalcount);
		out.writeInt(timepage_totalcount);
		out.writeInt(events_totalcount);
		
		
		for(int i=0;i<touchdatas.length ; i++){
			out.writeInt(touchdatas[i].client.x);
			out.writeInt(touchdatas[i].client.y);
			out.writeInt(touchdatas[i].page.x);
			out.writeInt(touchdatas[i].page.y);
			out.writeInt(touchdatas[i].gesture);
			out.writeInt(touchdatas[i].type);
			out.writeLong(touchdatas[i].time);
			out.writeInt(touchdatas[i].time_mili);
			out.writeInt(touchdatas[i].push_tag);
		}
		
		
	}
	
}
