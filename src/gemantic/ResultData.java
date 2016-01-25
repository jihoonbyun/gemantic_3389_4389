package gemantic;

import java.awt.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.jedis.Jedis;

public class ResultData {
	/*
	터치데이터를 분석한 결과들을 변수로 지니고 그 변수들의 컨트롤 메소드를 지니는 클래스	
	 */

	//분석의 결과물들

	
	private String url;			//주소	o
	private String device;		//기기	o
	private int day;			//날짜	o
	private String country;		//국가	o
	private int time_sum;		//총 체류 시간	o
	private int area_move;		//총 이동 거리
	private double area_speed;	//평균 이동 시간
	private int[] area_time;	//구간별 체류 시간	o
	private double[] area_rate;	//구간별 체류 비율	o
	private int[] area_spread;	//구간별 확대 횟수	o
	private int[] area_reverse;	//구간별 역류 횟수	o
	private int[] area_finish;	//구간별 종료 횟수	o
	private int[] area_slow;	//구간별 느린 횟수	o
	private int[] area_fast;	//구간별 빠른 횟수	o
	private ArrayList<Integer> area_loc;		//시간별 체류 구간	o
	private int[] area_flow;	//구간별 이동 흐름 
	private int[] motion_count;	//모션별 횟수			o
	//private int[] tab_link;		//링크별 탭 횟수		o
	private int start_area;		//첫 터치의 위치		o
	private long start_time;	//첫 터치의 시간		o
	private int finish_area;	//마지막 터치의 위치	o
	private long finish_time;	//마지막 터치의 시간	o
	private int min_area;		//최상위 터치의 위치	o
	private int max_area;		//최하위 터치의 위치	o
	private double complete_rate;	//완독률				o
	private int[] tag_time;		//구문별 체류 시간	o
	private double[] tag_rate;	//구문별 체류 비율	o
	private int[] tag_reverse;	//구문별 역류 횟수	o
	private int[] tag_copy;		//구문별 복사 횟수	o
	private int[] drag_loc;		//드래그 시작 위치(좌/우)	o
	private ArrayList<Point> fault_tab; 	//잘못된 탭	o	
	private ArrayList<Point> heat_map; 	//잘못된 탭	o	
	public int left_user;		//왼쪽 사용자 수	o
	public int right_user;		//오른쪽 사용자 수	o
	public int both_user;		//균형 사용자 수	o
	public int user_cnt;		//총 사용자 수(but 세션의갯수인것같다. user_cnt=1해놓고 세션횟수만큼(터치데이터기준) 루프돌리면서 합친다)	o
	public boolean bounce;		//이탈 여부
	public boolean close;		//종료 여부
	public boolean complete;	//완독 여부
	public boolean destination;	//목표(도달)달성 여부
	public boolean timehost;	//목표(체류)달성 여부
	public boolean timepage;	//목표(체류)달성 여부
	public boolean events;		//목표(수행)달성 여부
	public long complete_time;	//완독을 달성한 시간
	public long destination_time;	//완독을 달성한 시간
	public long timehost_time;	//완독을 달성한 시간
	public long timepage_time;	//완독을 달성한 시간
	public long events_time;	//완독을 달성한 시간
	
	public int destination_totalcount; //총달성한횟수
	public int timehost_totalcount;
	public int timepage_totalcount;
	public int events_totalcount;
	
	
	public int type;			//타입 (0-total, 1-bounce, 2-close, 3-complete)
	public int read_patterns[];	//리딩패턴.
	public int all_area_time;	//모든 구간 체류시간의 합
	
	public TreeSet<String> pureuser; //순수 유저의 집합 ID
	public ArrayList<String> alluser; //모든 유저의 ID
	
	

	static int heatmap_size = 5;//히트맵을 자르는 기준
	
	//기능 : 생성자. 변수들을 초기화 한다.
	public ResultData() {
		// TODO 자동 생성된 생성자 스텁
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
	// 기능 : 에러 로그를 출력하는 함수
	// 인자 : 에러명
	// 리턴 : 없음
	
	//두 resultdata 클래스의 데이터를 합친다.
	//여기서 컨버전스를 다 더해준다는 사실을 명심하자.
	public ResultData combine(ResultData data1, ResultData data2 ){
		ResultData result = new ResultData();
		
	
		//키들이 모두 동일할 때만 가능
		if(data1.url.equals(data2.url) && data1.device.equals(data2.device) && data1.day == data2.day && data1.country.equals(data2.country)){
					
			//구성
			result.make_Context(data1.url, data1.device, data1.country, data1.day, data1.area_time.length, data1.tag_time.length, 0, 0,"");
			
			//데이터 합성 - 단일
			result.time_sum = data1.time_sum + data2.time_sum;
			result.area_speed = (data1.area_speed * data1.user_cnt + data2.area_speed * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			result.complete_time = (data1.complete_time * data1.user_cnt + data2.complete_time * data2.user_cnt) / (data1.user_cnt + data2.user_cnt);
			
			//전환은 여러번있을수 있다. 따라서 user_cnt가 아닌 totalcount로 계산해야한다
			//쉽게말해서 재석이가 설정한 user_cnt는 실제 사람수가 아니라 세션횟수인것같다.(터치데이터 들어와서 나간거 묶음 갯수.재석이 ㅡ.ㅡ;; 이름햇갈리게지음)
			//그러나 한세션당 전환달성이 여러번일수있다. 때문에 자체적으로 컨버전스가 따로 카운트하는 값들로 나눠주는것이다.
			//0일때는 나눌수없으므로 예외처리한다.
			//그래서 재석이 코드를 수정했다. 2015.04.11
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
			
			
			//데이터 합성 - 배열
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
			System.out.println("두 객체는 다른 키 값을 지니고 있습니다 - combine");
		}
		
		return result;
	}
	
	public ResultData copy(){
		ResultData result = new ResultData();
		result.make_Context(this.url, this.device, this.country, this.day, this.area_time.length, this.tag_time.length, 0, 0,"");
		
		//데이터 복제 - 단일
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
		
		//데이터 복제 - 배열
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

	// 기능 : 고정된 크기의 배열을 생성하고 기기명, 국가, 날짜를 받아 설정한다. 생성자 다음으로 호출
	// 인자 : 기기명, 국가, 날짜
	// 리턴 : 없음
	public void make_Context(String Url, String Device_name, String Contury_name, int Day, int area_count, int tag_count, int device_width, int device_height, String userid){	//기본 설정하는 함수
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

	// 기능 : 페이지에 대한 데이터를 받아와 구간과 구문 등의 데이터를 할당한다.
	// 인자 : 없음
	// 리턴 : 없음
	private void set_Page_data(int area_count, int tag_count){
		//DB에서 URL로 데이터 불러오기
	
		
		if(0 < area_count)		
			make_Area (area_count);
		else
			errlog("area_count는 양수여야 합니다 - Set_Page_data");
		
		if(0 < tag_count){
			make_tag(tag_count);
			//TODO 태그 오프셋, 높이 받아오기
			
		}
		else
			errlog("tag_count는 양수여야 합니다 - Set_Page_data");
		
//		if(0 < link_count)
//			make_link(area_count);
//		else
//			errlog("area_count는 양수여야 합니다 - Set_Page_data");
	}


	
	// 기능 : 구간의 개수를 받아 구간에 대한 정보를 저장하는 배열들을 할당한다.
	// 인자 : 구간의 개수
	// 리턴 : 없음
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

	// 기능 : 구문의 개수를 받아 구문에 대한 정보를 저장하는 배열들을 할당한다.
	// 인자 : 구문의 개수
	// 리턴 : 없음
	private void make_tag(int num){
		tag_copy = new int[num];
		tag_time = new int[num];
		tag_reverse = new int[num];
		tag_rate = new double[num];
	}

//	// 기능 : 링크의 개수를 받아 링크에 대한 정보를 저장하는 배열들을 할당한다.
//	// 인자 : 링크의 개수
//	// 리턴 : 없음
//	private void make_link(int num){
//		tab_link = new int[num];
//	}

	// 기능 : 구간에 그 구간을 본 만큼의 시간을 점수로 매긴다.
	// 인자 : 첫번째 구간의 인덱스, 구간의 수, 시간(간격), 시간(현재 시간)
	// 리턴 : 없음
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
		//		errlog("index가 범위를 벗어났습니다 - count_Area_time");						
		}
	}

	// 기능 : 확대한 구간에 확대 수를 올린다.
	// 인자 : 확대한 구간의 인덱스
	// 리턴 : 없음
	public void count_Area_spread(int index){
		if(0 <= index && index < area_spread.length)	
			area_spread[index]++;	
		else
			errlog("index가 범위를 벗어났습니다 - count_Area_spread");
	}


	// 기능 : 시간별로 어느 구간에 있었는지를 체크한다.
	// 인자 : 현재 있는 구간의 인덱스 (최상위)
	// 리턴 : 없음
	public void count_Area_loc(int index){
		//System.out.println("A_L");
		if(0 <= index && index < area_time.length)
			area_loc.add(index);
	//	else
	//		errlog("index가 범위를 벗어났습니다 - count_Area_loc");
	}
	
	// 기능 : 이동 거리를 추가한다.
	// 인자 : 이동한 거리
	// 리턴 : 없음
	public void count_Area_move(int move){
		area_move += move;
	//	System.out.println("%%%%%%%%%%%%%%area_move : " + area_move + " , move : " + move);
	}

	// 기능 : 사용자가 되돌아간 구간을 체크한다.
	// 인자 : 첫번째 구간의 인덱스, 구간의 수
	// 리턴 : 없음
	public void count_Area_reverse(int first_index, int size){
		area_reverse[first_index] ++;
//		for(int i = 0 ; i < size ; i++){
//			if(0 <= i + first_index && first_index + i < area_time.length)
//				area_reverse[first_index + i] ++;	
//			else
//				errlog("index가 범위를 벗어났습니다 - count_Area_reverse");						
//		}
	}

	// 기능 : 제스쳐가 각각 몇 번씩 이루어졌는지를 체크하는 함수.
	// 인자 : 제스쳐 이름
	// 리턴 : 없음
	public void count_motion (int motion){
		if(1 <= motion && motion <= 7)
			motion_count[motion-1]++;
		else
			errlog("존재하지 않는 모션입니다 - count_motion");
	}


	// 기능 : 첫 입력이 이루어진 구간과 시간을 체크한다. (최상위)
	// 인자 : 첫 입력이 이루어졌을때 최상위 구간의 인덱스, 시간
	// 리턴 : 없음
	public void count_Area_start(int index, long time){
		start_time = time;
		
		if(0 <= index && index < area_time.length)
			start_area = index;
		else
			errlog("index가 범위를 벗어났습니다 - count_Area_start");
	}

	// 기능 : 마지막 입력이 이루어진 구간과 시간을 체크한다. (최하위)
	// 인자 : 마지막 입력이 이루어졌을 때 최하위 구간의 인덱스, 시간
	// 리턴 : 없음
	public void count_Area_finish(int index, long time){
		//로 time은 touchdatas 맨마지막이 아닌 맨마지막 전의 드래그 데이터를 기준으로 한다.
		if (start_time <= time)
			finish_time = time;
		else
			errlog("time이 Start_time 보다 작습니다 - count_Area_finish");
		
		
		if(0 <= index && index < area_time.length)
			finish_area = index;

		else
			//System.out.println("피니시메리어타임!!!!!!!!!!!!" + area_time.length);//672
			//System.out.println(start_area);//675
			//System.out.println("피니시인덱스" +  index);//672
			
			errlog("index가 범위를 벗어났습니다! - count_Area_finish");
			//finish_area = index;
	}

	// 기능 : 복사 행위가 이루어진 구문을 체크한다.
	// 인자 : 복사 행위가 이루어진 구문의 인덱스
	// 리턴 : 없음
	public void count_Tag_copy(int index){

		if(0 <= index && index < tag_copy.length)
			tag_copy[index]++;
		else
			errlog("index가 범위를 벗어났습니다 - count_Tag_copy");

	}

	// 기능 : 구문들에 체류한 시간을 체크한다.
	// 인자 : 체류한 구문들의 인덱스, 시간
	// 리턴 : 없음
	public void count_Tag_time(ArrayList<Integer> indexes, int time){
		for(int index : indexes){

			if(0 <= index && index <= tag_time.length)
				tag_time[index] += time;
			else
				errlog("index가 범위를 벗어났습니다 - count_Tag_time");
		}
	}

	// 기능 : 사용자가 되돌아간 구문을 체크한다.
	// 인자 : 체류한 구문들의 인덱스, 시간
	// 리턴 : 없음
	public void count_Tag_reverse(ArrayList<Integer> indexes, int time){
		for(int index : indexes){

			if(0 <= index && index < tag_time.length)
				tag_reverse[index] += 1;
			else
				errlog("index가 범위를 벗어났습니다 - count_Tag_reverse");
		}
	}
	
	// 기능 : 드래그의 시작 위치가 좌측인지 우측인지를 체크한다.
	// 인자 : 입력의 x 좌표(디바이스 좌표)
	// 리턴 : 없음	
	public void count_Drag(int x_point, int device_width){
		if(x_point > device_width/2)
			drag_loc[1]++;		
		else
			drag_loc[0]++;
	}

	// 기능 : 링크가 아닌 곳을 터치한 것을 기록한다.
	// 인자 : 터치의 x,y 좌표(페이지)
	// 리턴 : 없음
	public void count_Fault_tab(int x, int y, int page_width, int page_height){
		Point p = new Point(x,y);

		if(0 <= x && x <= page_width && 0<= y && y <= page_height)
			fault_tab.add(p);
		else
			errlog("index가 범위를 벗어났습니다 - count_Fault_tab");

	}

	// 기능 : 터치한 것을 기록한다.
	// 인자 : 터치의 x,y 좌표(페이지)
	// 리턴 : 없음
	public void count_Heat_Map(int x, int y, int page_width, int page_height){
		Point p = new Point(x,y);

		if(0 <= x && x <= page_width && 0<= y && y <= page_height)
			heat_map.add(p);
		else
			errlog("index가 범위를 벗어났습니다 - count_Heat_Map");

	}

	// 기능 : 구간 체류 시간을 바탕으로 리딩 패턴을 계산한다. 
	// 인자 : 없음
	// 리턴 : 없음
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

	// 기능 : 구간과 구문에 체류한 총 시간을 계산하고 구문과 구간에 체류한 시간 비율을 계산한다. 
	// 인자 : 없음
	// 리턴 : 없음
	public void merge_time() {
		if (area_time != null) {

			time_sum = (int) (finish_time - start_time);
			if (time_sum == 0) {// 단 한 번의 터치
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
			
			//빠른구간, 느린구간 측정
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
			//아래서부터 검색
			System.out.println("피니시라인 인덱스" + finish_area);
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

	// 기능 : 히트맵을 구간별로 나눈다.
	// 인자 : 없음
	// 리턴 : 없음	
	public void merge_heat_map(){
		for(Point p : heat_map){
			p.x /= heatmap_size;
			p.y /= heatmap_size;
		}
	
	}
	
	// 기능 : 구간별 이동 경로를 계산한다.
	// 인자 : 없음
	// 리턴 : 없음
	public void merge_flow(int device_height, int area_height) {

//		System.out.println("area loc = " + area_loc.size());
//		for (int line : area_loc) {
//			System.out.println("area loc = " + line);
//		}

		if (area_flow != null) {
			
			//시간대별 위치 값을 담은 배열(이때 시간은 50ms 기준으로 시작,중간,끝에 전부 시작점위치를 담음)
			//한 세션의 전체 area_loc으로 보인다.
			int all_size = area_loc.size();
			//이 세션의 처음부터 끝날때까지 인덱스 개수
			int flow_size = (int) (area_flow.length * complete_rate);
			
			//그럼이거는 위치값들갯수/읽은데까지 인덱스 개수 (area_loc 배열에 들어있는 값들이 전체에서 몇번째 인덱스인지 알기위해)
			double convert = (double) all_size / (flow_size);
			
			//인크리스레이트가 인위적으로 길이를 100%로 늘리는코드임
			// area_time.length가 전체 인덱스 갯수이고
			// 전체인덱스개수 - 화면인덱스개수가 화면을 제외한 나머지 인덱스 개수
			// area_loc에 들어가는 값이 화면 상단 시작점이니까 화면길이만큼 늘려줄라고 하는것같음
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
				errlog("위치가 없습니다 - merge_flow");
			}

			int index;
			if (area_flow.length < flow_size)
				flow_size = area_flow.length;
			//System.out.println("* = " + convert*(flow_size));
			// System.out.println("범위 = " + area_time.length);
			// System.out.println("확대률 = " + increase_rate);
			int num;
			for (int i = 0; i < flow_size; i++) {
				
				//area_loc에 들어있는 인자가 전체 인덱스에서 몇번쨰 인덱스인지 구할라고 하는거다
				index = (int) ((double) i * convert);
				
				
				// System.out.println("index = " + index);
				if (index >= area_loc.size()) {
					System.out.println("범위넘은 - " + index + "--merge_flow");
					index = area_loc.size() - 1;

				}
				
				/*
				 * 에리어 록에 들어있는 값을 전체 사이즈로 쭉 늘리는거다. 전체비율로 맞추기
				 * 스마트폰 화면 사이즈가 빠지는만큼 늘려주는 것 같다
				 */
				num = (int) ((double) area_loc.get(index) * increase_rate);
				//이 번호는 구간 번호이므로, *10을 해야 맞다.
				area_flow[i] = num*area_height;
				//System.out.println("i - > "+ i +" index = " + index + " flow = " + area_flow[i]);
			}

			//빈칸 채우기
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

	// 기능 : 완독률, 손잡이를 계산한다.
	// 인자 : 없음
	// 리턴 : 없음	
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

	
	// 기능 : 이 밑의 get 함수들은 전부 해당 변수를 리턴하는 역할을 한다.
	// 인자 : 없음
	// 리턴 : 해당 변수의 값
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

	// 기능 : 이 jedis에서 과거 데이터를 불러와 합쳐 리턴한다.
	// 인자 : 없음
	// 리턴 : 합쳐진 resultdata 객체
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
		
		
		//다이렉트 접근
		String key = result.get_url() + "," + result.get_device() + "," + result.get_day() + "," + result.get_country()+type;
		String key_area_time, key_area_flow, key_area_spread, key_area_reverse, key_area_finish,key_area_fast, key_area_slow;
		String key_tag_time, key_tag_copy, key_tag_reverse;
		String key_fault_tab, key_heat_map,key_read_pattern;
		String key_pure_user, key_all_user;
		
		//키 생성
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
		//개별 데이터 불러오기
		jedis.select(2);
		tmp = jedis.hget(key,"time_sum");
		
	
		if(tmp == null){//과거 기록이 없다면
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
		
			
		//배열 데이터 불러오기
		Long len;
		jedis.select(3);
		
		//구간 정보 불러오기
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
		
		//리딩 패턴 불러오기
		len = 31L;
		meta.read_patterns = toInt(jedis.lrange(key_read_pattern, 0, len), (int)(long)len);
		
		//구문 정보 불러오기
		len = jedis.llen(key_tag_time) ;
		meta.tag_rate = toDouble(jedis.lrange(key_tag_time, 0, len), (int)(long)len);
		len = jedis.llen(key_tag_copy) ;
		meta.tag_copy = toInt(jedis.lrange(key_tag_copy, 0, len), (int)(long)len);
		len = jedis.llen(key_tag_reverse) ;
		meta.tag_reverse = toInt(jedis.lrange(key_tag_reverse, 0, len), (int)(long)len);

		//잘못된 탭 불러오기
		len = jedis.llen(key_fault_tab) ;
		meta.fault_tab = toPoint(jedis.lrange(key_fault_tab, 0, len), (int)(long)len);

		//잘못된 탭 불러오기
		len = jedis.llen(key_heat_map) ;
		meta.heat_map = toPoint(jedis.lrange(key_heat_map, 0, len), (int)(long)len);

		//순수 유저 불러오기
		Set<String> pureuser_tmp = jedis.smembers(key_pure_user);	
		for(String t : pureuser_tmp){
			meta.pureuser.add(t);
		}

		//전체 유저 불러오기
		len = jedis.llen(key_all_user) ;		
		List<String> alluser_tmp = jedis.lrange(key_all_user, 0, len);		
		for(String t : alluser_tmp){
			meta.alluser.add(t);
		}
		
		//meta.alluser = jedis.lrange(key_all_user, 0, len);
		
		
		
		//둘을 합친 것을 리턴
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
				errlog("잘못된 falut_tab - toPoint");
				continue;
			}
			touch.x = Integer.parseInt(xy[0]);
			touch.y = Integer.parseInt(xy[1]);
			data.add(touch);
		}		
		return data;
	}

}
