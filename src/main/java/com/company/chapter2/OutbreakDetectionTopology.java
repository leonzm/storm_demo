package com.company.chapter2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class OutbreakDetectionTopology {

	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		DiagnosisEventSpout spout = new DiagnosisEventSpout(); 
		Stream inputStream = topology.newStream("event", spout);
		inputStream
		.each(new Fields("event"), new DiseaseFilter()) // 过滤重复
		.each(new Fields("event"), new CityAssignment(), new Fields("city")) // 定位城市
		.each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour")) // 打上小时时间戳
		.groupBy(new Fields("cityDiseaseHour")) // 按 城市、疾病、小时 进行分类
		.persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count")) // 统计结果
		.newValuesStream()
		.each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alter")) // 告警
		.each(new Fields("alter"), new DispatchAlter(), new Fields());
		
		return topology.build();
	}
	
	public static void main(String[] args) {
        Config config = new Config();
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("Outbreak-Detection-Topology", config, buildTopology());
		
		Utils.sleep(100 * 1000);
		cluster.killTopology("Outbreak-Detection-Topology");
		cluster.shutdown();
	}
	
}
