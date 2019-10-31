package storm;

import util.DateFmt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class DaoBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    HBaseDAO dao = null;

    long beginTime = System.currentTimeMillis();

    long endTime = 0;

    String todayStr = null;
    // 通话总数
    Map<String, Long> countMap = new HashMap<String, Long>();
    // 掉话数
    Map<String, Long> dropCountMap = new HashMap<String, Long>();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        dao = new HBaseDAOImp();
        Calendar calendar = Calendar.getInstance();
    }

    public void execute(Tuple tuple) {
        if (tuple != null) {
            String time = tuple.getString(0);
            String phone = tuple.getString(1);
            String status = tuple.getString(2);
            //判断是否是当天的，不是就删除记录
            todayStr = DateFmt.getCountDate(null, DateFmt.date_short);
            //如果记录不是当天的就清空
            if (todayStr != time && todayStr.compareTo(time) < 0) {
                countMap.clear();
                dropCountMap.clear();
            }
            //统计电话号出现的次数
            Long phonecount = countMap.get(phone);
            if (phonecount == null) {
                phonecount = 0L;
            }
            countMap.put(phone, ++phonecount);
            //
            Long dropphonecount = dropCountMap.get(phone);
            int statuscount = Integer.getInteger(status);
            if (statuscount > 0) {
                if (dropphonecount == null) {
                    dropphonecount = 0L;
                }
                dropCountMap.put(phone, ++dropphonecount);
            }

            endTime = System.currentTimeMillis();

            if (endTime - beginTime >= 5000) {
                if (countMap.size() > 0 && dropCountMap.size() > 0) {
                    // x轴，相对于小时的偏移量，格式为 时：分，数值 数值是时间的偏移
                    String arr[] = this.getAxsi();
                    // 当前日期
                    String today = DateFmt.getCountDate(null, DateFmt.date_short);
                    // 当前分钟
                    String today_minute = DateFmt.getCountDate(null, DateFmt.date_minute);
                    // cellCountMap为通话数据的map
                    Set<String> keys = countMap.keySet();
                    for (Iterator iterator = keys.iterator(); iterator.hasNext(); ) {

                        String key_cellnum = (String) iterator.next();

                        System.out.println("key_cellnum: " + key_cellnum + "***"
                                + arr[0] + "---"
                                + arr[1] + "---"
                                + countMap.get(key_cellnum) + "----"
                                + dropCountMap.get(key_cellnum));

                        //写入HBase数据，样例： {time_title:"10:45",xAxis:10.759722222222223,call_num:140,call_drop_num:91}

                        dao.insert("cell_monitor_table", key_cellnum + "_" + today, "cf", new String[]{today_minute},
                                new String[]{"{" + "time_title:\"" + arr[0] + "\",xAxis:" + arr[1] + ",call_num:" + countMap.get(key_cellnum) + ",call_drop_num:" + dropCountMap.get(key_cellnum) + "}"}
                        );
                    }
                }
                beginTime = System.currentTimeMillis();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    // 获取X坐标，就是当前时间的坐标，小时是单位
    public String[] getAxsi() {
        // 取当前时间
        Calendar c = Calendar.getInstance();
        int hour = c.get(Calendar.HOUR_OF_DAY);
        int minute = c.get(Calendar.MINUTE);
        int sec = c.get(Calendar.SECOND);
        // 总秒数
        int curSecNum = hour * 3600 + minute * 60 + sec;

        // (12*3600+30*60+0)/3600=12.5
        Double xValue = (double) curSecNum / 3600;
        // 时：分，数值 数值是时间的偏移
        String[] end = {hour + ":" + minute, xValue.toString()};
        return end;
    }
}
