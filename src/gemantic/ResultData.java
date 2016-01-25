package gemantic;

import java.awt.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.jedis.Jedis;

public class ResultData {
	/*
	��ġ�����͸� �м��� ������� ������ ���ϰ� �� �������� ��Ʈ�� �޼ҵ带 ���ϴ� Ŭ����	
	 */

	//�м��� �������

	
	private String url;			//�ּ�	o
	private String device;		//���	o
	private int day;			//��¥	o
	private String country;		//����	o
	private int time_sum;		//�� ü�� �ð�	o
	private int area_move;		//�� �̵� �Ÿ�
	private double area_speed;	//��� �̵� �ð�
	private int[] area_time;	//������ ü�� �ð�	o
	private double[] area_rate;	//������ ü�� ����	o
	private int[] area_spread;	//������ Ȯ�� Ƚ��	o
	private int[] area_reverse;	//������ ���� Ƚ��	o
	private int[] area_finish;	//������ ���� Ƚ��	o
	private int[] area_slow;	//������ ���� Ƚ��	o
	private int[] area_fast;	//������ ���� Ƚ��	o
	private ArrayList<Integer> area_loc;		//�ð��� ü�� ����	o
	private int[] area_flow;	//������ �̵� �帧 
	private int[] motion_count;	//��Ǻ� Ƚ��			o
	//private int[] tab_link;		//��ũ�� �� Ƚ��		o
	private int start_area;		//ù ��ġ�� ��ġ		o
	private long start_time;	//ù ��ġ�� �ð�		o
	private int finish_area;	//������ ��ġ�� ��ġ	o
	private long finish_time;	//������ ��ġ�� �ð�	o
	private int min_area;		//�ֻ��� ��ġ�� ��ġ	o
	private int max_area;		//������ ��ġ�� ��ġ	o
	private double complete_rate;	//�ϵ���				o
	private int[] tag_time;		//������ ü�� �ð�	o
	private double[] tag_rate;	//������ ü�� ����	o
	private int[] tag_reverse;	//������ ���� Ƚ��	o
	private int[] tag_copy;		//������ ���� Ƚ��	o
	private int[] drag_loc;		//�巡�� ���� ��ġ(��/��)	o
	private ArrayList<Point> fault_tab; 	//�߸��� ��	o	
	private ArrayList<Point> heat_map; 	//�߸��� ��	o	
	public int left_user;		//���� ����� ��	o
	public int right_user;		//������ ����� ��	o
	public int both_user;		//���� ����� ��	o
	public int user_cnt;		//�� ����� ��(but �����ǰ����ΰͰ���. user_cnt=1�س��� ����Ƚ����ŭ(��ġ�����ͱ���) ���������鼭 ��ģ��)	o
	public boolean bounce;		//��Ż ����
	public boolean close;		//���� ����
	public boolean complete;	//�ϵ� ����
	public boolean destination;	//��ǥ(����)�޼� ����
	public boolean timehost;	//��ǥ(ü��)�޼� ����
	public boolean timepage;	//��ǥ(ü��)�޼� ����
	public boolean events;		//��ǥ(����)�޼� ����
	public long complete_time;	//�ϵ��� �޼��� �ð�
	public long destination_time;	//�ϵ��� �޼��� �ð�
	public long timehost_time;	//�ϵ��� �޼��� �ð�
	public long timepage_time;	//�ϵ��� �޼��� �ð�
	public long events_time;	//�ϵ��� �޼��� �ð�
	
	public int destination_totalcount; //�Ѵ޼���Ƚ��
	public int timehost_totalcount;
	public int timepage_totalcount;
	public int events_totalcount;
	
	
	public int type;			//Ÿ�� (0-total, 1-bounce, 2-close, 3-complete)
	public int read_patterns[];	//��������.
	public int all_area_time;	//��� ���� ü���ð��� ��
	
	public TreeSet<String> pureuser; //���� ������ ���� ID
	public ArrayList<String> alluser; //��� ������ ID
	
	

	static int heatmap_size = 5;//��Ʈ���� �ڸ��� ����
	
	//��� : ������. �������� �ʱ�ȭ �Ѵ�.
	public ResultData() {
		// TODO �ڵ� ������ ������ ����
		url = "";
		device = "";
		day = -1;
		country = "";
		time_sum = -1;
		area_move = -1;
		area_speed = 0;
		area_time = null;
		area_rate = null;
		area_spread = null;
		user_cnt=1;
		left_user=0;
		right_user=0;
		both_user=0;
		min_area = -1;
		max_area = -1;
		complete_time = -1;
		destination_time = -1;
		timehost_time = -1;
		timepage_time = -1;
		events_time = -1;
		
		destination_totalcount = 0;
		timehost_totalcount = 0;
		timepage_totalcount= 0;
		events_totalcount = 0;
		
		
		all_area_time=0;

		motion_count = new int[7];
		for(@SuppressWarnings("unused") int n : motion_count){
			n=0;
		}
		drag_loc = new int [2];
		for(@SuppressWarnings("unused") int n : drag_loc)
			n=0;
		
	//	tab_link=null;
		start_area = -1;
		start_time = -1;
		finish_area = -1;
		finish_time = -1;
		complete_rate = -1;
		
		tag_copy = null;
		tag_time = null;
		tag_rate = null;
		bounce = false;
		close = false;
		complete = false;
		destination = false;
		timehost = false;
		timepage = false;
		events = false;
		
		
	}
	// ��� : ���� �α׸� ����ϴ� �Լ�
	// ���� : ������
	// ���� : ����
	
	//�� resultdata Ŭ������ �����͸� ��ģ��.
	//���⼭ ���������� �� �����شٴ� ����� �������.
	public ResultData combine(ResultData data1, ResultData data2 ){
		ResultData result = new ResultData();
		
	
		//Ű���� ��� ������ ���� ����
		if(data1.url.equals(data2.url) && data1.device.equals(data2.device) && data1.day == data2.day && data1.country.equals(data2.country)){
					
			//����
			result.make_Context(data1.url, data1.device, data1.country, data1.day, data1.area_time.length, data1.tag_time.length, 0, 0,"");
			
			//������ �ռ� - ����
			result.time_sum = data1.time_sum + data2.time_sum;
			result.area_speed = (data1.area_speed * data1.user_cnt + data2.area_speed * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.complete_time = (data1.complete_time * data1.user_cnt + data2.complete_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			
			//��ȯ�� ������������ �ִ�. ���� user_cnt�� �ƴ� totalcount�� ����ؾ��Ѵ�
			//���Ը��ؼ� �缮�̰� ������ user_cnt�� ���� ������� �ƴ϶� ����Ƚ���ΰͰ���.(��ġ������ ���ͼ� ������ ���� ����.�缮�� ��.��;; �̸��ް���������)
			//�׷��� �Ѽ��Ǵ� ��ȯ�޼��� �������ϼ��ִ�. ������ ��ü������ ���������� ���� ī��Ʈ�ϴ� ����� �����ִ°��̴�.
			//0�϶��� �����������Ƿ� ����ó���Ѵ�.
			//�׷��� �缮�� �ڵ带 �����ߴ�. 2015.04.11
			/*
			result.destination_time = (data1.destination_time * data1.user_cnt + data2.destination_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.timehost_time = (data1.timehost_time * data1.user_cnt + data2.timehost_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.timepage_time = (data1.timepage_time * data1.user_cnt + data2.timepage_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.events_time = (data1.events_time * data1.user_cnt + data2.events_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			*/
			if((data1.destination_totalcount + data2.destination_totalcount) == 0){
				result.destination_time = 0;
			}
			else{
				result.destination_time = (data1.destination_time * data1.destination_totalcount + data2.destination_time * data2.destination_totalcount) / (data1.destination_totalcount + data2.destination_totalcount);
			}
			
			
			
			
			if((data1.timehost_totalcount + data2.timehost_totalcount) == 0){
				result.timehost_time = 0;				
			}
			else{
				result.timehost_time = (data1.timehost_time * data1.timehost_totalcount + data2.timehost_time * data2.timehost_totalcount) / (data1.timehost_totalcount + data2.timehost_totalcount);
			}
			
			
			
			
			
			if((data1.timepage_totalcount + data2.timepage_totalcount) == 0){
				result.timepage_time = 0;	
			}
			else{
				result.timepage_time = (data1.timepage_time * data1.timepage_totalcount + data2.timepage_time * data2.timepage_totalcount) / (data1.timepage_totalcount + data2.timepage_totalcount);
			}
			
			
			
			if((data1.events_totalcount + data2.events_totalcount) == 0){
				result.events_time = 0;				
			}
			else{
				result.events_time = (data1.events_time * data1.events_totalcount + data2.events_time * data2.events_totalcount) / (data1.events_totalcount + data2.events_totalcount);
			}
			
			
			result.complete_rate = (data1.complete_rate * data1.user_cnt + data2.complete_rate * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.all_area_time = (data1.all_area_time * data1.user_cnt + data2.all_area_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.user_cnt = data1.user_cnt + data2.user_cnt;
			result.left_user = data1.left_user + data2.left_user;
			result.right_user = data1.right_user + data2.right_user;
			result.both_user = data1.both_user + data2.both_user;
			

			result.destination_totalcount = data1.destination_totalcount + data2.destination_totalcount;
			result.timehost_totalcount = data1.timehost_totalcount + data2.timehost_totalcount;
			result.timepage_totalcount = data1.timepage_totalcount + data2.timepage_totalcount;
			result.events_totalcount = data1.events_totalcount + data2.events_totalcount;
			
			
			//������ �ռ� - �迭
			result.area_rate = average_double(data1.area_rate, data2.area_rate, data1.user_cnt, data2.user_cnt);
			result.tag_rate = average_double(data1.tag_rate, data2.tag_rate, data1.user_cnt, data2.user_cnt);
			result.area_flow = average_int(data1.area_flow, data2.area_flow, data1.user_cnt, data2.user_cnt);
			result.area_spread = sum_int(data1.area_spread, data2.area_spread);
			result.area_reverse= sum_int(data1.area_reverse, data2.area_reverse);
			result.area_finish= sum_int(data1.area_finish, data2.area_finish);
			result.area_fast= sum_int(data1.area_fast, data2.area_fast);
			result.area_slow= sum_int(data1.area_slow, data2.area_slow);
			result.motion_count = sum_int(data1.motion_count, data2.motion_count);
			result.tag_reverse = sum_int(data1.tag_reverse, data2.tag_reverse);
			result.tag_copy = sum_int(data1.tag_copy, data2.tag_copy);
			result.drag_loc = sum_int(data1.drag_loc, data2.drag_loc);
			result.read_patterns = sum_int(data1.read_patterns, data2.read_patterns);
			
			for(Point t : data1.fault_tab)
				result.fault_tab.add(t);
			for(Point t : data2.fault_tab)
				result.fault_tab.add(t);

			for(Point t : data1.heat_map)
				result.heat_map.add(t);
			for(Point t : data2.heat_map)
				result.heat_map.add(t);
			

			for(String t : data1.pureuser)
				result.pureuser.add(t);
			for(String t : data2.pureuser)
				result.pureuser.add(t);

			for(String t : data1.alluser)
				result.alluser.add(t);
			for(String t : data2.alluser)
				result.alluser.add(t);
			
			result.type = data1.type;
		}
		else{
			System.out.println("�� ��ü�� �ٸ� Ű ���� ���ϰ� �ֽ��ϴ� - combine");
		}
		
		return result;
	}
	
	public ResultData copy(){
		ResultData result = new ResultData();
		result.make_Context(this.url, this.device, this.country, this.day, this.area_time.length, this.tag_time.length, 0, 0,"");
		
		//������ ���� - ����
		result.time_sum = this.time_sum;
		result.area_move = this.area_move;
		result.area_speed = this.area_speed;
		result.complete_rate = this.complete_rate;
		result.complete_time = this.complete_time;
		result.destination_time = this.destination_time;
		result.timehost_time = this.timehost_time;
		result.timepage_time = this.timepage_time;
		result.events_time = this.events_time;
		
		result.destination_totalcount = this.destination_totalcount;
		result.timehost_totalcount = this.timehost_totalcount;
		result.timepage_totalcount = this.timepage_totalcount;
		result.events_totalcount = this.events_totalcount;
		
		
		result.user_cnt = this.user_cnt;
		result.left_user = this.left_user;
		result.right_user = this.right_user;
		result.both_user = this.both_user;
		result.all_area_time = this.all_area_time;
		
		//������ ���� - �迭
		result.area_rate = average_double(this.area_rate, this.area_rate, 1, 0);
		result.tag_rate = average_double(this.tag_rate, this.tag_rate, 1, 0);
		result.area_flow = average_int(this.area_flow, this.area_flow, 1, 0);
		result.area_spread = average_int(this.area_spread, this.area_spread, 1, 0);
		result.area_reverse= average_int(this.area_reverse, this.area_reverse, 1, 0);
		result.area_finish= average_int(this.area_finish, this.area_finish, 1, 0);
		result.area_fast= average_int(this.area_fast, this.area_fast, 1, 0);
		result.area_slow= average_int(this.area_slow, this.area_slow, 1, 0);
		result.motion_count = average_int(this.motion_count, this.motion_count, 1, 0);
		result.tag_reverse = average_int(this.tag_reverse, this.tag_reverse, 1, 0);
		result.tag_copy = average_int(this.tag_copy, this.tag_copy, 1, 0);
		result.drag_loc = average_int(this.drag_loc, this.drag_loc, 1, 0);
		result.read_patterns = average_int(this.read_patterns, this.read_patterns, 1, 0);
		
		for(Point t : this.fault_tab)
			result.fault_tab.add(t);

		for(Point t : this.heat_map)
			result.heat_map.add(t);

		for(String t : this.pureuser)
			result.pureuser.add(t);

		for(String t : this.alluser)
			result.alluser.add(t);
		
		return result;
	}
	
	public int[] sum_int(int[] data1, int[] data2){

		int len = data1.length;
		if(len > data2.length)
			len = data2.length;
		
		int[] result = new int[len];
		
		for(int i=0;i<len;i++){
			result[i] = data1[i] + data2[i];
		}
		
		return result;
	}

	public int[] average_int(int[] data1, int[] data2, int data1_cnt, int data2_cnt){

		int len = data1.length;
		if(len > data2.length)
			len = data2.length;
		
		int[] result = new int[len];
		
		for(int i=0;i<len;i++){
			if(data1[i] != 0 && data2[i] != 0)
				result[i] = (int)((double)(data1[i]*data1_cnt + data2[i]*data2_cnt) / (data1_cnt + data2_cnt));
			else if(data1[i] == 0 && data2[i] != 0)
				result[i] =  data2[i];
			else if(data1[i] != 0 && data2[i] == 0)
				result[i] = data1[i];
			else 
				result[i] = 0;
							
		}
		
		return result;
	}
	
	public double[] average_double(double[] data1, double[] data2, int data1_cnt, int data2_cnt){

		int len = data1.length;
		if(len > data2.length)
			len = data2.length;
		
		double[] result = new double[len];
		
		for(int i=0;i<len;i++){
			if(data1[i] != 0 && data2[i] != 0)
				result[i] = ((data1[i]*data1_cnt + data2[i]*data2_cnt)) / (data1_cnt + data2_cnt);
			else if(data1[i] == 0 && data2[i] != 0)
				result[i] =  data2[i];
			else if(data1[i] != 0 && data2[i] == 0)
				result[i] = data1[i];
			else 
				result[i] = 0;
		}
		
		return result;
	}
	
	public static void errlog (String errs){
		System.out.println(errs);
	}

	// ��� : ������ ũ���� �迭�� �����ϰ� ����, ����, ��¥�� �޾� �����Ѵ�. ������ �������� ȣ��
	// ���� : ����, ����, ��¥
	// ���� : ����
	public void make_Context(String Url, String Device_name, String Contury_name, int Day, int area_count, int tag_count, int device_width, int device_height, String userid){	//�⺻ �����ϴ� �Լ�
		url = Url;
		device = Device_name;
		day = Day;
		country = Contury_name;
		read_patterns = new int [32];
		for(int i=0;i<=30;i++)
			read_patterns[i]=0;
		area_loc = new ArrayList<Integer>();
		fault_tab = new ArrayList<Point>();
		heat_map = new ArrayList<Point>();
		
		pureuser = new TreeSet<>();
		alluser = new ArrayList<String>();
		
		if(!"".equals(userid)){
		
			pureuser.add(userid);
			alluser.add(userid);
		}
		set_Page_data(area_count,tag_count);
	}

	// ��� : �������� ���� �����͸� �޾ƿ� ������ ���� ���� �����͸� �Ҵ��Ѵ�.
	// ���� : ����
	// ���� : ����
	private void set_Page_data(int area_count, int tag_count){
		//DB���� URL�� ������ �ҷ�����
	
		
		if(0 < area_count)		
			make_Area (area_count);
		else
			errlog("area_count�� ������� �մϴ� - Set_Page_data");
		
		if(0 < tag_count){
			make_tag(tag_count);
			//TODO �±� ������, ���� �޾ƿ���
			
		}
		else
			errlog("tag_count�� ������� �մϴ� - Set_Page_data");
		
//		if(0 < link_count)
//			make_link(area_count);
//		else
//			errlog("area_count�� ������� �մϴ� - Set_Page_data");
	}


	
	// ��� : ������ ������ �޾� ������ ���� ������ �����ϴ� �迭���� �Ҵ��Ѵ�.
	// ���� : ������ ����
	// ���� : ����
	private void make_Area(int num){
		//System.out.println("size ===========================" + num);
		area_spread = new int[num];
		area_time = new int[num];
		area_rate = new double[num];	
		area_reverse = new int[num];	
		area_finish = new int[num];
		area_fast = new int[num];
		area_slow = new int[num];
		area_flow = new int[num];
	}

	// ��� : ������ ������ �޾� ������ ���� ������ �����ϴ� �迭���� �Ҵ��Ѵ�.
	// ���� : ������ ����
	// ���� : ����
	private void make_tag(int num){
		tag_copy = new int[num];
		tag_time = new int[num];
		tag_reverse = new int[num];
		tag_rate = new double[num];
	}

//	// ��� : ��ũ�� ������ �޾� ��ũ�� ���� ������ �����ϴ� �迭���� �Ҵ��Ѵ�.
//	// ���� : ��ũ�� ����
//	// ���� : ����
//	private void make_link(int num){
//		tab_link = new int[num];
//	}

	// ��� : ������ �� ������ �� ��ŭ�� �ð��� ������ �ű��.
	// ���� : ù��° ������ �ε���, ������ ��, �ð�(����), �ð�(���� �ð�)
	// ���� : ����
	public void count_Area_time(int first_index, int size, int elapsed, long time){
		
		if(complete_time == -1){
			complete_time = time;
		}
		
		if(min_area == -1){
			min_area = first_index;
		}
		
		if(max_area == -1){
			max_area = first_index + size  ;
		}
		
		if(min_area > first_index){
			min_area = first_index;
			if(time > complete_time)
				complete_time = time;
		}
		
		if(max_area < first_index + size ){
			max_area = first_index + size  ;
			if(time > complete_time)
				complete_time = time;
		}
			
		
	//	System.out.println("size = " + size);
	//	System.out.println("len = " + size);
		for(int i = 0 ; i < size ; i++){
			
			if(0 <= i + first_index && first_index + i < area_time.length)
				area_time[first_index + i] += elapsed;	
		//	else
		//		errlog("index�� ������ ������ϴ� - count_Area_time");						
		}
	}

	// ��� : Ȯ���� ������ Ȯ�� ���� �ø���.
	// ���� : Ȯ���� ������ �ε���
	// ���� : ����
	public void count_Area_spread(int index){
		if(0 <= index && index < area_spread.length)	
			area_spread[index]++;	
		else
			errlog("index�� ������ ������ϴ� - count_Area_spread");
	}


	// ��� : �ð����� ��� ������ �־������� üũ�Ѵ�.
	// ���� : ���� �ִ� ������ �ε��� (�ֻ���)
	// ���� : ����
	public void count_Area_loc(int index){
		//System.out.println("A_L");
		if(0 <= index && index < area_time.length)
			area_loc.add(index);
	//	else
	//		errlog("index�� ������ ������ϴ� - count_Area_loc");
	}
	
	// ��� : �̵� �Ÿ��� �߰��Ѵ�.
	// ���� : �̵��� �Ÿ�
	// ���� : ����
	public void count_Area_move(int move){
		area_move += move;
	//	System.out.println("%%%%%%%%%%%%%%area_move : " + area_move + " , move : " + move);
	}

	// ��� : ����ڰ� �ǵ��ư� ������ üũ�Ѵ�.
	// ���� : ù��° ������ �ε���, ������ ��
	// ���� : ����
	public void count_Area_reverse(int first_index, int size){
		area_reverse[first_index] ++;
//		for(int i = 0 ; i < size ; i++){
//			if(0 <= i + first_index && first_index + i < area_time.length)
//				area_reverse[first_index + i] ++;	
//			else
//				errlog("index�� ������ ������ϴ� - count_Area_reverse");						
//		}
	}

	// ��� : �����İ� ���� �� ���� �̷���������� üũ�ϴ� �Լ�.
	// ���� : ������ �̸�
	// ���� : ����
	public void count_motion (int motion){
		if(1 <= motion && motion <= 7)
			motion_count[motion-1]++;
		else
			errlog("�������� �ʴ� ����Դϴ� - count_motion");
	}


	// ��� : ù �Է��� �̷���� ������ �ð��� üũ�Ѵ�. (�ֻ���)
	// ���� : ù �Է��� �̷�������� �ֻ��� ������ �ε���, �ð�
	// ���� : ����
	public void count_Area_start(int index, long time){
		start_time = time;
		
		if(0 <= index && index < area_time.length)
			start_area = index;
		else
			errlog("index�� ������ ������ϴ� - count_Area_start");
	}

	// ��� : ������ �Է��� �̷���� ������ �ð��� üũ�Ѵ�. (������)
	// ���� : ������ �Է��� �̷������ �� ������ ������ �ε���, �ð�
	// ���� : ����
	public void count_Area_finish(int index, long time){
		//�� time�� touchdatas �Ǹ������� �ƴ� �Ǹ����� ���� �巡�� �����͸� �������� �Ѵ�.
		if (start_time <= time)
			finish_time = time;
		else
			errlog("time�� Start_time ���� �۽��ϴ� - count_Area_finish");
		
		
		if(0 <= index && index < area_time.length)
			finish_area = index;

		else
			//System.out.println("�ǴϽø޸���Ÿ��!!!!!!!!!!!!" + area_time.length);//672
			//System.out.println(start_area);//675
			//System.out.println("�ǴϽ��ε���" +  index);//672
			
			errlog("index�� ������ ������ϴ�! - count_Area_finish");
			//finish_area = index;
	}

	// ��� : ���� ������ �̷���� ������ üũ�Ѵ�.
	// ���� : ���� ������ �̷���� ������ �ε���
	// ���� : ����
	public void count_Tag_copy(int index){

		if(0 <= index && index < tag_copy.length)
			tag_copy[index]++;
		else
			errlog("index�� ������ ������ϴ� - count_Tag_copy");

	}

	// ��� : �����鿡 ü���� �ð��� üũ�Ѵ�.
	// ���� : ü���� �������� �ε���, �ð�
	// ���� : ����
	public void count_Tag_time(ArrayList<Integer> indexes, int time){
		for(int index : indexes){

			if(0 <= index && index <= tag_time.length)
				tag_time[index] += time;
			else
				errlog("index�� ������ ������ϴ� - count_Tag_time");
		}
	}

	// ��� : ����ڰ� �ǵ��ư� ������ üũ�Ѵ�.
	// ���� : ü���� �������� �ε���, �ð�
	// ���� : ����
	public void count_Tag_reverse(ArrayList<Integer> indexes, int time){
		for(int index : indexes){

			if(0 <= index && index < tag_time.length)
				tag_reverse[index] += 1;
			else
				errlog("index�� ������ ������ϴ� - count_Tag_reverse");
		}
	}
	
	// ��� : �巡���� ���� ��ġ�� �������� ���������� üũ�Ѵ�.
	// ���� : �Է��� x ��ǥ(����̽� ��ǥ)
	// ���� : ����	
	public void count_Drag(int x_point, int device_width){
		if(x_point > device_width/2)
			drag_loc[1]++;		
		else
			drag_loc[0]++;
	}

	// ��� : ��ũ�� �ƴ� ���� ��ġ�� ���� ����Ѵ�.
	// ���� : ��ġ�� x,y ��ǥ(������)
	// ���� : ����
	public void count_Fault_tab(int x, int y, int page_width, int page_height){
		Point p = new Point(x,y);

		if(0 <= x && x <= page_width && 0<= y && y <= page_height)
			fault_tab.add(p);
		else
			errlog("index�� ������ ������ϴ� - count_Fault_tab");

	}

	// ��� : ��ġ�� ���� ����Ѵ�.
	// ���� : ��ġ�� x,y ��ǥ(������)
	// ���� : ����
	public void count_Heat_Map(int x, int y, int page_width, int page_height){
		Point p = new Point(x,y);

		if(0 <= x && x <= page_width && 0<= y && y <= page_height)
			heat_map.add(p);
		else
			errlog("index�� ������ ������ϴ� - count_Heat_Map");

	}

	// ��� : ���� ü�� �ð��� �������� ���� ������ ����Ѵ�. 
	// ���� : ����
	// ���� : ����
	public void merge_reading_pattern(double average_time){
		if(average_time == -1)
			average_time = all_area_time;
		
		double sum_time[] = new double[4];
		sum_time[0]=0; sum_time[1]=0; sum_time[2]=0; sum_time[3]=0; 
		int size = area_time.length;
		int i=0;
		for(;i<size/4;i++){
			sum_time[0] += area_time[i];
		}
		for(;i<size*2/4;i++){
			sum_time[1] += area_time[i];
		}
		for(;i<size*3/4;i++){
			sum_time[2] += area_time[i];
		}
		for(;i<size;i++){
			sum_time[3] += area_time[i];
		}
		
		
		double average_part = average_time/4;
		int read_pattern;
		

//		System.out.println("!!!!!!!!!!!!!!!!! sumtime : " + sum_time[0]);
//		System.out.println("!!!!!!!!!!!!!!!!! sumtime : " + sum_time[1]);
//		System.out.println("!!!!!!!!!!!!!!!!! sumtime : " + sum_time[2]);
//		System.out.println("!!!!!!!!!!!!!!!!! sumtime : " + sum_time[3]);
//		System.out.println("!!!!!!!!!!!!!!!!! sumtime_all : " + all_area_time);
//		System.out.println("!!!!!!!!!!!!!!!!! average_time : " + average_time);
//		System.out.println("!!!!!!!!!!!!!!!!! average_part : " + average_part);
		
		if(sum_time[3] == 0.0 && sum_time[2] == 0.0 && sum_time[1] == 0.0){
			read_pattern = 1;
			if(sum_time[0] < average_part)
				read_pattern += 1;
		}
		else if(sum_time[3] == 0.0 && sum_time[2] == 0.0 ){
			read_pattern = 3;
			if(sum_time[0] < average_part)
				read_pattern += 2;
			if(sum_time[1] < average_part)
				read_pattern += 1;
		}
		else if(sum_time[3] == 0.0){
			read_pattern = 7;
			if(sum_time[0] < average_part)
				read_pattern += 4;
			if(sum_time[1] < average_part)
				read_pattern += 2;
			if(sum_time[2] < average_part)
				read_pattern += 1;
		}
		else {
			read_pattern = 15;
			if(sum_time[0] < average_part)
				read_pattern += 8;
			if(sum_time[1] < average_part)
				read_pattern += 4;
			if(sum_time[2] < average_part)
				read_pattern += 2;
			if(sum_time[3] < average_part)
				read_pattern += 1;
			
		}

		System.out.println("!!!!!!!!!!!!!!!!! read_patterns : " + read_pattern);
		read_patterns[read_pattern]++;
		
	}

	// ��� : ������ ������ ü���� �� �ð��� ����ϰ� ������ ������ ü���� �ð� ������ ����Ѵ�. 
	// ���� : ����
	// ���� : ����
	public void merge_time() {
		if (area_time != null) {

			time_sum = (int) (finish_time - start_time);
			if (time_sum == 0) {// �� �� ���� ��ġ
				for (int i = 0; i < area_time.length; i++) {
					area_rate[i] = 0;
				}
				for (int i = 0; i < tag_time.length; i++) {
					tag_rate[i] = 0;
				}
				return;
			}

			for (int i = 0; i < area_time.length; i++) {
				all_area_time += area_time[i];
				area_rate[i] = (double) area_time[i] / (double) time_sum;
			}
			
			//��������, �������� ����
			int average_areatime = all_area_time / area_time.length;

			for (int i = 0; i < area_time.length; i++) {
				if(area_time[i] > average_areatime*1.5)
					area_fast[i]++;
				if(area_time[i] < average_areatime*0.5)
					area_slow[i]++;
				
			}

			for (int i = 0; i < tag_time.length; i++) {
				tag_rate[i] = (double) tag_time[i] / (double) time_sum;
			}
			
			//int maxline = 0;
			//�Ʒ������� �˻�
			System.out.println("�ǴϽö��� �ε���" + finish_area);
			area_finish[finish_area]++;
			area_speed = (double)area_move / time_sum;
			/*
			for(int i=area_finish.length-1;i>0;i--){
				if(area_time[i] != 0 && area_time[i-1] == 0){
					maxline = i;
					break;					
				}
			}
			area_finish[maxline]++;
			area_speed = (double)area_move / time_sum;
			*/
			//System.out.println("@@@@@@@@@@@@area move : " + area_move + " area_speed = " + area_speed);

			//System.out.println("@@@@@@@@@@@@timesum = " + time_sum);
		}
	}

	// ��� : ��Ʈ���� �������� ������.
	// ���� : ����
	// ���� : ����	
	public void merge_heat_map(){
		for(Point p : heat_map){
			p.x /= heatmap_size;
			p.y /= heatmap_size;
		}
	
	}
	
	// ��� : ������ �̵� ��θ� ����Ѵ�.
	// ���� : ����
	// ���� : ����
	public void merge_flow(int device_height, int area_height) {

//		System.out.println("area loc = " + area_loc.size());
//		for (int line : area_loc) {
//			System.out.println("area loc = " + line);
//		}

		if (area_flow != null) {
			
			//�ð��뺰 ��ġ ���� ���� �迭(�̶� �ð��� 50ms �������� ����,�߰�,���� ���� ��������ġ�� ����)
			//�� ������ ��ü area_loc���� ���δ�.
			int all_size = area_loc.size();
			//�� ������ ó������ ���������� �ε��� ����
			int flow_size = (int) (area_flow.length * complete_rate);
			
			//�׷��̰Ŵ� ��ġ���鰹��/���������� �ε��� ���� (area_loc �迭�� ����ִ� ������ ��ü���� ���° �ε������� �˱�����)
			double convert = (double) all_size / (flow_size);
			
			//��ũ��������Ʈ�� ���������� ���̸� 100%�� �ø����ڵ���
			// area_time.length�� ��ü �ε��� �����̰�
			// ��ü�ε������� - ȭ���ε��������� ȭ���� ������ ������ �ε��� ����
			// area_loc�� ���� ���� ȭ�� ��� �������̴ϱ� ȭ����̸�ŭ �÷��ٶ�� �ϴ°Ͱ���
			double increase_rate = (double) area_time.length/ (area_time.length - device_height / area_height);
			// System.out.println("all size = " + all_size);
			// System.out.println("flow size = " + flow_size);
			// System.out.println("convert = " + convert);
			// System.out.println("complete_rate = " + complete_rate);

			// System.out.println("area_loc size : " + all_size);
			// for(int a : area_loc){
			// System.out.println(a);
			// }

			if (all_size == 0) {
				errlog("��ġ�� �����ϴ� - merge_flow");
			}

			int index;
			if (area_flow.length < flow_size)
				flow_size = area_flow.length;
			//System.out.println("* = " + convert*(flow_size));
			// System.out.println("���� = " + area_time.length);
			// System.out.println("Ȯ��� = " + increase_rate);
			int num;
			for (int i = 0; i < flow_size; i++) {
				
				//area_loc�� ����ִ� ���ڰ� ��ü �ε������� ����� �ε������� ���Ҷ�� �ϴ°Ŵ�
				index = (int) ((double) i * convert);
				
				
				// System.out.println("index = " + index);
				if (index >= area_loc.size()) {
					System.out.println("�������� - " + index + "--merge_flow");
					index = area_loc.size() - 1;

				}
				
				/*
				 * ������ �Ͽ� ����ִ� ���� ��ü ������� �� �ø��°Ŵ�. ��ü������ ���߱�
				 * ����Ʈ�� ȭ�� ����� �����¸�ŭ �÷��ִ� �� ����
				 */
				num = (int) ((double) area_loc.get(index) * increase_rate);
				//�� ��ȣ�� ���� ��ȣ�̹Ƿ�, *10�� �ؾ� �´�.
				area_flow[i] = num*area_height;
				//System.out.println("i - > "+ i +" index = " + index + " flow = " + area_flow[i]);
			}

			//��ĭ ä���
			for (int i = flow_size; i < area_flow.length; i++) {
				//System.out.println("i = " + i + " -> " + area_flow[i - 1]);
				if (i - 1 > 0) {
					area_flow[i] = area_flow[i - 1];
				}
			}
//			int j=0;
//			System.out.println("flow size = " + flow_size + " " + area_flow.length);
//			for (int a : area_flow) {
//				System.out.print(a + " (" + j +")");
//				j++;
//			}
		}
		// time_sum = (int)(finish_time - start_time);
		//
		// for(int i=0;i < area_time.length ; i++){
		// area_rate[i] = (double)area_time[i] / (double)time_sum;
		// }
		//
		// for(int i=0;i < tag_time.length ; i++){
		// tag_rate[i] = (double)tag_time[i] / (double)time_sum;
		// }
	}

	// ��� : �ϵ���, �����̸� ����Ѵ�.
	// ���� : ����
	// ���� : ����	
	public void merge_Complete(){
		if(area_time != null){
		complete_rate = (double)(max_area - min_area) / area_time.length;
		
		if(complete_rate == 1){
			complete=true;
			complete_time = complete_time - start_time;
		}
		
		if(drag_loc[0] == 0 && drag_loc[1] == 0){
			
		}		
		else if(drag_loc[0] > (drag_loc[0] + drag_loc[1])*0.7)
			left_user++;
		else if(drag_loc[1] > (drag_loc[0] + drag_loc[1])*0.7)
			right_user++;
		else
			both_user++;
		}
	}

	
	// ��� : �� ���� get �Լ����� ���� �ش� ������ �����ϴ� ������ �Ѵ�.
	// ���� : ����
	// ���� : �ش� ������ ��
	public String get_url(){
		if(!url.isEmpty())
			return url;
		else
			return "";			
	}
	public String get_device(){
		if(!device.isEmpty())
			return device;
		else
			return "";	
	}
	public int get_day(){
		return day;
	}	
	public String get_country(){
		return country;
	}	
	public int get_time_sum(){
		return time_sum;
	}
	public int get_area_move(){
		return area_move;
	}
	public double get_area_speed(){
		return area_speed;
	}
	public int[] get_read_pattern(){
		return read_patterns;
	}	
	public int[] get_area_time(){
		return area_time;
	}	
	public int[] get_area_finish(){
		return area_finish;
	}	
	public int[] get_area_fast(){
		return area_fast;
	}	
	public int[] get_area_slow(){
		return area_slow;
	}	
	public int[] get_area_reverse(){
		return area_reverse;
	}	
	public double[] get_area_rate(){
		return area_rate;
	}
	public int[] get_area_spread(){
		return area_spread;
	}
	public ArrayList<Integer> get_area_loc(){
		return area_loc;
	}
	public int[] get_area_flow(){
		return area_flow;
	}
	public int[] get_motion_count(){
		return motion_count;
	}
//	public int[] get_tab_link(){
//		return tab_link;
//	}
	public int get_start_area(){
		return start_area;
	}
	public long get_start_time(){
		return start_time;
	}
	public int get_finish_area(){
		return finish_area;
	}
	public long get_finish_time(){
		return finish_time;
	}
	public double get_complete(){
		return complete_rate;
	}
	public long get_complete_time(){
		return complete_time;
	}
	public long get_destination_time(){
		return destination_time;
	}
	public long get_timehost_time(){
		return timehost_time;
	}
	public long get_timepage_time(){
		return timepage_time;
	}
	public long get_events_time(){
		return events_time;
	}
	
	public int get_destination_totalcount(){
		return destination_totalcount;
	}
	public int get_timehost_totalcount(){
		return timehost_totalcount;
	}
	public int get_timepage_totalcount(){
		return timepage_totalcount;
	}
	public int get_events_totalcount(){
		return events_totalcount;
	}
	
	
	
	public int[] get_tag_copy(){
		return tag_copy;
	}
	public int[] get_tag_time(){
		return tag_time;
	}
	public int[] get_tag_reverse(){
		return tag_reverse;
	}
	public double[] get_tag_rate(){
		return tag_rate;
	}
	public int[] get_drag_loc(){
		return drag_loc;
	}
	public ArrayList<Point> get_fault_tab(){
		return fault_tab;
	}

	public ArrayList<Point> get_heat_map(){
		return heat_map;
	}
	public ArrayList<String> get_all_user(){
		return alluser;
	}
	public Set<String> get_pure_user(){
		return pureuser;
	}

	// ��� : �� jedis���� ���� �����͸� �ҷ��� ���� �����Ѵ�.
	// ���� : ����
	// ���� : ������ resultdata ��ü
	public static ResultData loadmeta(ResultData result, Jedis jedis){
		ResultData meta = new ResultData();
		meta.make_Context(result.get_url(), result.get_device(), result.get_country(), result.get_day(), result.get_area_time().length, result.get_tag_time().length, 1, 1,"");

		
		String type = "";
		if(result.type == 1){
			meta.type = 1;
			type = ",bounce";			
		}
		else if(result.type == 2){
			meta.type = 2;
			type = ",close";			
		}
		else if(result.type == 3){
			meta.type = 3;
			type = ",complete";			
		}			
		else if(result.type == 4){
			meta.type = 4;
			type += ",destination";
		}
		else if(result.type == 5){
			meta.type = 5;
			type += ",timehost";
		}
		else if(result.type == 6){
			meta.type = 6;
			type += ",events";
		}
		else if(result.type == 7){
			meta.type = 7;
			type += ",timepage";
		}
		
		
		//���̷�Ʈ ����
		String key = result.get_url() + "," + result.get_device() + "," + result.get_day() + "," + result.get_country()+type;
		String key_area_time, key_area_flow, key_area_spread, key_area_reverse, key_area_finish,key_area_fast, key_area_slow;
		String key_tag_time, key_tag_copy, key_tag_reverse;
		String key_fault_tab, key_heat_map,key_read_pattern;
		String key_pure_user, key_all_user;
		
		//Ű ����
		key_area_time = key + "," + "area_time";
		key_area_flow = key + "," + "area_flow";
		key_area_spread = key + "," + "area_spread";
		key_area_reverse = key + "," + "area_reverse";
		key_area_finish = key + "," + "area_finish";
		key_area_fast = key + "," + "area_fast";
		key_area_slow = key + "," + "area_slow";
		key_tag_time = key + "," + "tag_time";
		key_tag_copy = key + "," + "tag_copy";
		key_tag_reverse = key + "," + "tag_reverse";
		key_fault_tab = key + "," + "fault_tab";
		key_heat_map = key + "," + "heat_map";
		key_read_pattern = key + "," + "read_pattern";
		key_pure_user = key + "," + "pure_user";
		key_all_user = key + "," + "all_user";

		String tmp;
		//���� ������ �ҷ�����
		jedis.select(2);
		tmp = jedis.hget(key,"time_sum");
		
	
		if(tmp == null){//���� ����� ���ٸ�
			return result;
		}
		
		meta.time_sum = Integer.parseInt(tmp);
		meta.area_speed = Double.parseDouble(jedis.hget(key,"area_speed"));
		meta.all_area_time = Integer.parseInt(jedis.hget(key,"all_area_time"));
		meta.drag_loc[0] = Integer.parseInt(jedis.hget(key,"drag_left"));
		meta.drag_loc[1] = Integer.parseInt(jedis.hget(key,"drag_right"));
		meta.motion_count[0] = Integer.parseInt(jedis.hget(key,"tab"));
		meta.motion_count[1] = Integer.parseInt(jedis.hget(key,"doubletab"));
		meta.motion_count[2] = Integer.parseInt(jedis.hget(key,"longtab"));
		meta.motion_count[3] = Integer.parseInt(jedis.hget(key,"drag"));
		meta.motion_count[4] = Integer.parseInt(jedis.hget(key,"pinch"));
		meta.motion_count[5] = Integer.parseInt(jedis.hget(key,"spread"));
		meta.motion_count[6] = Integer.parseInt(jedis.hget(key,"copy"));
		meta.complete_rate = Double.parseDouble(jedis.hget(key,"complete"));
		meta.user_cnt = Integer.parseInt(jedis.hget(key,"user_cnt"));
		meta.left_user = Integer.parseInt(jedis.hget(key,"left_user"));
		meta.right_user = Integer.parseInt(jedis.hget(key,"right_user"));
		meta.both_user = Integer.parseInt(jedis.hget(key,"both_user"));
		meta.complete_time = Long.parseLong(jedis.hget(key,"complete_time"));
		
		meta.destination_time = Long.parseLong(jedis.hget(key,"destination_time"));
		meta.timehost_time = Long.parseLong(jedis.hget(key,"timehost_time"));
		meta.timepage_time = Long.parseLong(jedis.hget(key,"timepage_time"));
		meta.events_time = Long.parseLong(jedis.hget(key,"events_time"));
		
		meta.destination_totalcount = Integer.parseInt(jedis.hget(key,"destination_totalcount"));
		meta.timehost_totalcount = Integer.parseInt(jedis.hget(key,"timehost_totalcount"));
		meta.timepage_totalcount = Integer.parseInt(jedis.hget(key,"timepage_totalcount"));
		meta.events_totalcount = Integer.parseInt(jedis.hget(key,"events_totalcount"));
		
			
		//�迭 ������ �ҷ�����
		Long len;
		jedis.select(3);
		
		//���� ���� �ҷ�����
		//System.out.println(key_area_time);
		len = jedis.llen(key_area_time) ;
		meta.area_rate = toDouble(jedis.lrange(key_area_time, 0, len), (int)(long)len);
		//System.out.println(key_area_flow);
		len = jedis.llen(key_area_flow) ;
		meta.area_flow = toInt(jedis.lrange(key_area_flow, 0, len), (int)(long)len);
		//System.out.println(key_area_spread);
		len = jedis.llen(key_area_spread) ;
		meta.area_spread = toInt(jedis.lrange(key_area_spread, 0, len), (int)(long)len);
		//System.out.println(key_area_reverse);
		len = jedis.llen(key_area_reverse) ;
		meta.area_reverse = toInt(jedis.lrange(key_area_reverse, 0, len), (int)(long)len);
		len = jedis.llen(key_area_finish) ;
		meta.area_finish = toInt(jedis.lrange(key_area_finish, 0, len), (int)(long)len);
		len = jedis.llen(key_area_fast) ;
		meta.area_fast = toInt(jedis.lrange(key_area_fast, 0, len), (int)(long)len);
		len = jedis.llen(key_area_slow) ;
		meta.area_slow = toInt(jedis.lrange(key_area_slow, 0, len), (int)(long)len);
		
		//���� ���� �ҷ�����
		len = 31L;
		meta.read_patterns = toInt(jedis.lrange(key_read_pattern, 0, len), (int)(long)len);
		
		//���� ���� �ҷ�����
		len = jedis.llen(key_tag_time) ;
		meta.tag_rate = toDouble(jedis.lrange(key_tag_time, 0, len), (int)(long)len);
		len = jedis.llen(key_tag_copy) ;
		meta.tag_copy = toInt(jedis.lrange(key_tag_copy, 0, len), (int)(long)len);
		len = jedis.llen(key_tag_reverse) ;
		meta.tag_reverse = toInt(jedis.lrange(key_tag_reverse, 0, len), (int)(long)len);

		//�߸��� �� �ҷ�����
		len = jedis.llen(key_fault_tab) ;
		meta.fault_tab = toPoint(jedis.lrange(key_fault_tab, 0, len), (int)(long)len);

		//�߸��� �� �ҷ�����
		len = jedis.llen(key_heat_map) ;
		meta.heat_map = toPoint(jedis.lrange(key_heat_map, 0, len), (int)(long)len);

		//���� ���� �ҷ�����
		Set<String> pureuser_tmp = jedis.smembers(key_pure_user);	
		for(String t : pureuser_tmp){
			meta.pureuser.add(t);
		}

		//��ü ���� �ҷ�����
		len = jedis.llen(key_all_user) ;		
		List<String> alluser_tmp = jedis.lrange(key_all_user, 0, len);		
		for(String t : alluser_tmp){
			meta.alluser.add(t);
		}
		
		//meta.alluser = jedis.lrange(key_all_user, 0, len);
		
		
		
		//���� ��ģ ���� ����
		return meta.combine(result, meta);
		
	}
	
	
	
	
	
	
	public static double[] toDouble(List<String> list, int len){
		double[] data = new double[len];
		
		for(int i=0;i<len;i++){
			data[i]=Double.parseDouble(list.get(i));
		//	System.out.print(" "+data[i]);
		}		
		//System.out.println();
		return data;
	}

	public static int[] toInt(List<String> list, int len){
		int[] data = new int[len];
		for(int i=0;i<len;i++){
			data[i]=Integer.parseInt(list.get(i));
		//	System.out.print(" "+data[i]);
		}		
		//System.out.println();
		return data;
	}

	public static ArrayList<Point> toPoint(List<String> list, int len){
		//System.out.println("len = "+len);
		
		ArrayList<Point> data = new ArrayList<Point>();
		String xy[] = new String[2];
		Point touch;
		for(int i=0;i<len;i++){
			touch = new Point();
			xy=list.get(i).split(",");
			if(xy.length != 2){
				errlog("�߸��� falut_tab - toPoint");
				continue;
			}
			touch.x = Integer.parseInt(xy[0]);
			touch.y = Integer.parseInt(xy[1]);
			data.add(touch);
		}		
		return data;
	}

}
