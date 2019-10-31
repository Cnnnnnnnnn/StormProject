package storm;

import util.DateFmt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        try {
            if (tuple != null) {
                String[] split = word.split("\\t");
                // 切分前数据：2494  29448-000003  2016-01-05 10:25:17  1
                // 发出的数据格式： 时间, 小区编号, 掉话状态
                collector.emit(new Values(DateFmt.getCountDate(split[2], DateFmt.date_short), split[1], split[3]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data", "cell_num", "drop_num"));
    }
}
