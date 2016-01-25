package gemantic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.mortbay.util.ajax.JSON;

public class ExternalData  implements Writable {

	//ó���� �ʿ��� �����͵�
	int device_width;	//��� ȭ�� �ʺ�
	int device_height;	//��� ȭ�� ����
	int page_width;		//������ ȭ�� �ʺ�
	int page_height;	//������ ȭ�� ����
	int area_height;	//���� ����
	int area_count;		//���� ��
	int[] tag_offset;	//���� ���� ��ġ
	int[] tag_height;	//���� ����
	int tag_count;		//������ ��
	double average_time;	//���� ��� ü���ð�
	String url;
	String device;
	
	public ExternalData(){//������. ���⼭ DB�� ����� ��
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
		// TODO �ڵ� ������ �޼ҵ� ����
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
		// TODO �ڵ� ������ �޼ҵ� ����
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
	


	// ��� : ��ũ�� ������ �޾� ��ũ�� ���� ������ �����ϴ� �迭���� �Ҵ��Ѵ�.
	// ���� : ��ũ�� ����
	// ���� : ����
//	private void make_link(int num){
//		link_list = new int[num];
//	}
}
