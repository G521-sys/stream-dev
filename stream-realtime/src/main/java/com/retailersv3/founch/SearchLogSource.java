package com.retailersv3.founch;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;
import java.util.Arrays;
import java.util.List;

public class SearchLogSource implements SourceFunction<String> {
    // 声明volatile变量控制数据源是否运行（多线程可见性）
    private volatile boolean isRunning = true;
    // 创建随机数生成器实例
    private Random random = new Random();

    // 定义示例查询词列表
    private List<String> sampleQueries = Arrays.asList(
            "unidays", "溪木源樱花奶盖身体乳", "除尘布袋工业", "双层空气层针织布料",
            "4812锂电", "鈴木雨燕方向機總成", "福特翼搏1.5l变速箱电脑模块",
            "a4红格纸", "岳普湖驴乃", "婴儿口罩0到6月医用专用婴幼儿"
    );

    // 定义示例商品标题列表
    private List<String> sampleTitles = Arrays.asList(
            "铂盛弹盖文艺保温杯学生男女情侣车载时尚英文锁扣不锈钢真空水杯",
            "可爱虎子华为荣耀X30i手机壳荣耀x30防摔全包镜头honorx30max液态硅胶虎年情侣女卡通手机套插画呆萌个性创意",
            "190色素色亚麻棉平纹布料衬衫裙服装定制手工绣花面料汇典亚麻",
            "松尼合金木工开孔器实木门开锁孔木板圆形打空神器定位打孔钻头",
            "微钩绿蝴蝶材料包非成品赠送视频组装教程需自备钩针染料",
            "春秋薄绒黑色打底袜女外穿高腰显瘦大码胖mm纯棉踩脚一体连袜裤",
            "New Balance/NB时尚长款过膝连帽保暖羽绒服女外套NCNPA/NPA46032",
            "2021博洋高级1拉舍尔云毯结婚庆毛毯子冬季加厚保暖被子珊瑚",
            "玉手牌平安无事牌天然翡翠a货男女款调节编织玉手链冰种玉石手串",
            "欧货加绒拼接开叉纽扣烟管裤女潮2021秋季高腰显瘦九分直筒牛仔裤"
    );

    // 实现SourceFunction的run方法，用于生成数据
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 初始化记录ID
        long id = 1;
        // 循环生成数据，直到isRunning为false
        while (isRunning) {
            // 随机选择一个查询词
            String query = sampleQueries.get(random.nextInt(sampleQueries.size()));
            // 随机选择一个商品标题
            String title = sampleTitles.get(random.nextInt(sampleTitles.size()));
            // 随机生成queryId：50%概率生成1-100000，50%概率生成200001-201000
            int queryId = random.nextBoolean() ? random.nextInt(100000) + 1 : random.nextInt(1000) + 200001;
            // 随机生成1-1000000的docId
            int docId = random.nextInt(1000000) + 1;
            // 获取当前时间戳
            long timestamp = System.currentTimeMillis();
            // 根据queryId判断数据类型（train/test）
            String type = queryId < 200000 ? "train" : "test";
            // 拼接CSV格式的日志字符串
            String log = String.format("%d,%s,%d,%s,%d,%s", 
                queryId, query, docId, title, timestamp, type);
            // 将生成的日志发送到下游
            ctx.collect(log);
            // 休眠100毫秒，控制数据生成速度
            Thread.sleep(100);

            // 自增ID，每生成1000条记录打印一次日志
            id++;
            if (id % 1000 == 0) {
                System.out.println("Generated " + id + " records");
            }
        }
    }
    // 实现SourceFunction的cancel方法，用于停止数据源
    @Override
    public void cancel() {
        isRunning = false;
    }
}