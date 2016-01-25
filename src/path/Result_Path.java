package path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class Result_Path {

	String date;	
	HashMap<String, HashMap<String, Integer>> pathlog;
	ArrayList<String> close;
	ArrayList<String> bounce;
	ArrayList<Convergence_Data> destination;
	ArrayList<Convergence_Data> timehost;
	ArrayList<Convergence_Data> timepage;
	ArrayList<Convergence_Data> events;
	
	public Result_Path(String date){
		this.date = date;
		pathlog = new HashMap<String, HashMap<String, Integer>>();
		close = new ArrayList<>();
		bounce = new ArrayList<>();
		destination = new ArrayList<>();
		timehost = new ArrayList<>();
		timepage = new ArrayList<>();
		events = new ArrayList<>();
	}
	//key == 출발점(태그), field == 도착점(태그), value = 횟수
	public void push(String key, String field, Integer value){
		if(pathlog.get(key) == null){//key가 처음으로 들어온다면
			HashMap<String, Integer> log = new HashMap<>();
			log.put(field, value);
			pathlog.put(key, log);
		}
		else if(pathlog.get(key).get(field) == null){//field가 처음으로 들어온다면
			pathlog.get(key).put(field, value);
		}
		else{//전에 들어온 적이 있다면
			int num = pathlog.get(key).get(field);
			num += value;
			pathlog.get(key).put(field, num);			
		}
	}
	
	public void close_set(String touch_key){
		close.add(touch_key);
	}
	
	public void bounce_set(String touch_key){
		bounce.add(touch_key);
	}
	public void destination_set(String touch_key, long time, int totalcount){
		Convergence_Data newdata = new Convergence_Data(touch_key, time, totalcount);		
		destination.add(newdata);
	}
	
	public void timehost_set(String touch_key, long time, int totalcount){
		Convergence_Data newdata = new Convergence_Data(touch_key, time, totalcount);
		timehost.add(newdata);
	}
	public void timepages_set(String touch_key, long time, int totalcount){
		Convergence_Data newdata = new Convergence_Data(touch_key, time, totalcount);
		timepage.add(newdata);
	}
	
	public void events_set(String touch_key, long time, int totalcount){
		Convergence_Data newdata = new Convergence_Data(touch_key, time, totalcount);
		events.add(newdata);
	}

	public Result_Path combine(Result_Path data1, Result_Path data2){
		
		Result_Path sum = new Result_Path(data1.date);
		
		for(String bounce : data1.bounce){
			sum.bounce.add(bounce);
		}
		for(String bounce : data2.bounce){
			sum.bounce.add(bounce);
		}

		for(String close : data1.close){
			sum.close.add(close);
		}
		for(String close : data2.close){
			sum.close.add(close);
		}

		for(Convergence_Data destination : data1.destination){
			sum.destination.add(destination);
		}
		for(Convergence_Data destination : data2.destination){
			sum.destination.add(destination);
		}

		for(Convergence_Data timehost : data1.timehost){
			sum.timehost.add(timehost);
		}
		for(Convergence_Data timehost : data2.timehost){
			sum.timehost.add(timehost);
		}

		for(Convergence_Data timepages : data1.timepage){
			sum.timepage.add(timepages);
		}
		for(Convergence_Data timepages : data2.timepage){
			sum.timepage.add(timepages);
		}

		for(Convergence_Data events : data1.events){
			sum.events.add(events);
		}
		for(Convergence_Data events : data2.events){
			sum.events.add(events);
		}
		
        Set<String> keyset = data1.pathlog.keySet();
       // String []logkeys = (String[]) keyset.toArray();
        HashMap<String, Integer> field;
		Set<String> fieldset;
      //  String []logfields;
        
        
        for(String logkeys : keyset){
        	field = data1.pathlog.get(logkeys);
        	
        	fieldset = field.keySet();
        	
        	for(String logfields : fieldset){
        		sum.push(logkeys, logfields, field.get(logfields));
        	}
        	

        }		
        
        keyset = data2.pathlog.keySet();        
        
        for(String logkeys : keyset){
        	field = data2.pathlog.get(logkeys);
        	
        	fieldset = field.keySet();
        	
        	for(String logfields : fieldset){
        		sum.push(logkeys, logfields, field.get(logfields));
        	}       	

        }		

        //sum.pathlog = (HashMap<String, HashMap<String, Integer>>) this.pathlog.clone();

        return sum;

	}

	public Result_Path copy(){
		Result_Path clone = new Result_Path(this.date);

		for(String bounce : this.bounce){
			clone.bounce.add(bounce);
		}

		for(String close : this.close){
			clone.close.add(close);
		}
		
		for(Convergence_Data destination : this.destination){
			clone.destination.add(destination);
		}

		for(Convergence_Data timehost : this.timehost){
			clone.timehost.add(timehost);
		}

		for(Convergence_Data timepages : this.timepage){
			clone.timepage.add(timepages);
		}

		for(Convergence_Data events : this.events){
			clone.events.add(events);
		}

		Set<String> keyset = this.pathlog.keySet();
		//Object[]logkeys =  keyset.toArray();
		HashMap<String, Integer> field;
		//@SuppressWarnings("rawtypes")
		Set<String> fieldset;
		//String []logfields;


		for(String id :keyset){
			field = this.pathlog.get(id);

			fieldset = field.keySet();
			//logfields = (String[]) fieldset.toArray();

			for(String fields : fieldset){
				clone.push((String) id, fields, field.get(fields));
			}


		}		
		return clone;

	}

	
}
