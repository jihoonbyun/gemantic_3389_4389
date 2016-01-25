package gemantic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.mortbay.util.ajax.JSON;

public class ExternalData  implements Writable {

	//처리에 필요한 데이터들
	int device_width;	//기기 화면 너비
	int device_height;	//기기 화면 높이
	int page_width;		//페이지 화면 너비
	int page_height;	//페이지 화면 높이
	int area_height;	//구간 높이
	int area_count;		//구간 수
	int[] tag_offset;	//구문 시작 위치
	int[] tag_height;	//구문 높이
	int tag_count;		//구문의 수
	double average_time;	//과거 평균 체류시간
	String url;
	String device;
	
	public ExternalData(){//생성자. 여기서 DB와 통신할 것
		area_height = 10;
	}

	public void input_area(int device_witdh, int device_height, int page_width, int page_height){
		this.device_width = device_witdh;
		this.device_height = device_height;
		this.page_width = page_height;
		this.page_height = page_height;
		
		this.area_count = this.page_height / area_height;
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~area_count = " + area_count + " page height = " + this.page_height );
		if(this.page_height % area_height != 0)
			area_count++;
		
	}
	
	@SuppressWarnings("rawtypes")
	public void input_Tag(String tags){
		
	//	System.out.println(tags);
		Object[] tgs = (Object[])JSON.parse(tags);
		
		this.tag_count = tgs.length;
		this.tag_offset = new int[tgs.length];
		this.tag_height = new int[tgs.length];
		
		for(int i=0;i<tgs.length;i++){
			this.tag_offset[i] = ((Long)((HashMap)tgs[i]).get("tops")).intValue();
			this.tag_height[i] = ((Long)((HashMap)tgs[i]).get("height")).intValue();
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		this.device_width = in.readInt();
		this.device_height = in.readInt();
		this.page_width = in.readInt();
		this.page_height = in.readInt();
		this.area_height = in.readInt();
		this.area_count = in.readInt();
		this.tag_count = in.readInt();
		this.average_time = in.readDouble();
		this.tag_offset = new int[this.tag_count];
		this.tag_height = new int[this.tag_count];
		for(int i=0;i<this.tag_count;i++){
			this.tag_offset[i] = in.readInt();
			this.tag_height[i] = in.readInt();			
		}

		this.url = in.readUTF();
		this.device = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO 자동 생성된 메소드 스텁
		out.writeInt(this.device_width);
		out.writeInt(this.device_height);
		out.writeInt(this.page_width);
		out.writeInt(this.page_height);
		out.writeInt(this.area_height);
		out.writeInt(this.area_count);
		out.writeInt(this.tag_count);
		out.writeDouble(this.average_time);
		for(int i=0;i<this.tag_count;i++){
			out.writeInt(this.tag_offset[i]);
			out.writeInt(this.tag_height[i]);			
		}
		out.writeUTF(url);
		out.writeUTF(device);
		
	}
	


	// 기능 : 링크의 개수를 받아 링크에 대한 정보를 저장하는 배열들을 할당한다.
	// 인자 : 링크의 개수
	// 리턴 : 없음
//	private void make_link(int num){
//		link_list = new int[num];
//	}
}
