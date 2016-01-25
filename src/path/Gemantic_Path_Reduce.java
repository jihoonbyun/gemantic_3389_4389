package path;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Gemantic_Path_Reduce extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {
	
	@Override
	public void reduce(Text key, Iterable<ObjectWritable> values,	Context context) throws IOException, InterruptedException {

		Path_Analysis path_an = new Path_Analysis();
		Result_Path result = null;
		Result_Path output = null;
		
		for (ObjectWritable value : values) {
			if(value.getDeclaredClass() == PathData.class){//제대로 들어왔을 때만 처리
				if(value.get() == null){
					System.out.println("pathdata is empty - reduce");
					continue;
				}
				if(((PathData)value.get()).linkdata == null){
					System.out.println("linkdata is empty - reduce");
					continue;
				}

				//System.out.println("url - " + ((PathData)value.get()).site_url);
				
				int flag = 1;
				for(Link_Data ld : ((PathData)value.get()).linkdata){
					if(ld == null){
						flag = -1;
						break;
						
					}
				}
				if(flag == -1){
					
					continue;
				}

				result = path_an.anlaysis(key.toString(), ((PathData)value.get()).date, ((PathData)value.get()).linkdata);
				//ystem.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&" + result);
				

				//System.out.println("length of keyset in result = " + result.pathlog.keySet().size());

				//for(String logkeys : result.pathlog.keySet()){
				//	System.out.println("----~~~~~!!!key = "+key+":"+result.date+":"+logkeys);
				//}			
				
				if(output == null){
					output = result.copy();
					//System.out.println("length of keyset in out_first = " + output.pathlog.keySet().size());

					//for(String logkeys : output.pathlog.keySet()){
				//		System.out.println("----~~~~~!!!key = "+key+":"+output.date+":"+logkeys);
					//}			
				}
				else{
					output = result.combine(result, output);
					//System.out.println("length of keyset in out = " + output.pathlog.keySet().size());

					//for(String logkeys : output.pathlog.keySet()){
					//	System.out.println("----~~~~~!!!key = "+key+":"+output.date+":"+logkeys);
					//}			
				}
				
			}
			
		}
//		
//		Set<String> keyset = output.pathlog.keySet();
//
//		//HashMap<String, String> input;
//
//		System.out.println("length of keyset in reduce = " + output.pathlog.keySet().size());
//		
//		for(String logkeys : keyset){
//			System.out.println("----~~~~~!!!key = "+key+":"+output.date+":"+logkeys);
//		}			
		
		
		if(output != null){
			
			
			System.out.println("output key = " + key);
			System.out.println("length of dest = " + output.destination.size());
			for(Convergence_Data cov : output.destination){
				System.out.println("cov = " + cov.touchdatakey.split(",")[1]);
			}
		
//
//			System.out.println("url - " + ((PathData)value.get()).site_url);
//
//			for(Link_Data ld : output.){
//				System.out.println("link = " + ld.link_name);
//				
//			}
			context.write(key, new ObjectWritable(output));
		}

	}

}