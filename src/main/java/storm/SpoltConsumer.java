package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * storm消费生成者生产的假数据
 */
public class SpoltConsumer {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        //zookeeper服务器集群的地址
        ZkHosts zkHosts = new ZkHosts("hdp-1:2181,hdp-2:2181,hdp-3:2181");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts,"mylog_cmcc","/MyKafka","MyTrack");
        List<String> zkServers = new ArrayList<String>();
        System.out.println(zkHosts.brokerZkStr);
        for(String host:zkHosts.brokerZkStr.split(",")){
            //切分结果hdp-1,hdp-2,hdp-3放到集合中
            zkServers.add(host.split(":")[0]);
        }
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        //是否从头开始消费
        spoutConfig.ignoreZkOffsets = false;
        spoutConfig.socketTimeoutMs = 60 * 1000;
        //将流转化为字符串
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        builder.setSpout("spout",new KafkaSpout(spoutConfig),3);
        builder.setBolt("fbolt",new FilterBolt(),3).shuffleGrouping("spout");
        builder.setBolt("dbolt",new DaoBolt(),5).fieldsGrouping("fbolt",new Fields("cell_num"));

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(5);

        if(args.length>0){
            try {
                StormSubmitter.submitTopology(args[0],conf,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else {
            System.out.println("Local running");
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology",conf,builder.createTopology());
        }

    }
}
