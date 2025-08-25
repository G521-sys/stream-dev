create table ods_professional(
    `afterbase` string,
    `table` string,
    `type` string,
    `ts` bigint,
    `after` map<string,string>,
    `old` map<string,string>,
    proc_time as proctime()
) with (
    'connector'='kafka',
    'topic'='ods_professional',
    'properties.bootstrap.servers'='cdh01:9092',
    'properties.group.id'='test',
    'scan.startup.mode'='latest-offset',
    'format'='json'
);


select
    `after`['id'] id,
    `after`['user_id'] user_id,
    `after`['sku_id'] sku_id,
    `after`['appraise'] appraise,
    `after`['comment_txt'] comment_txt,
    ts,
    proc_time
from ods_professional where `table`='comment_info' and `type`='insert'




create table base_dic(
    `dic_code` string,
    info row<dic_name string>,
    primary key (dic_code) not enforced
)with (
    'connector'='hbase-2.2',
    'table_name'='dim_base_dic',
    'zookeeper.quorum'='cdh01:2181',
    'lookup.async'='true',
    'lookup.cache'='PARTIAL',
    'lookup.partial.cache.max-rows'='500',
    'lookup.partial.cache.expire-after_write'='1 hour',
    'lookup.partial-cache.expire-after-access'='1 hour'
);




select
    id,
    user_id,
    sku_id,
    appraise,
    dic.dic_name as appraise_name,
    comment_txt,
    ts
from comment_info as c
json base_dic for system_time as of c.proc_time as dic
on c.appraise = dic.dic_code



create table a(
    id string,
    user_id string,
    sku_id string,
    appraise string,
    appraise_name string,
    comment_txt string,
    ts bigint,
    primary key (id) not enforced
)with (
    'connector'='upsert-kafka',
    'topic'='comment_info',
    'properties.bootstrap.servers'='cdh01:9092',
    'key.format'='json',
    'value.format'='json'
);



select
    `after`['id'],
    `after`['user_id'],
    `after`['sku_id'],
    `after`['sku_num'],
    ts
from ods_professional
where `table`='cart_info'
and (
    `type`='insert' or
    `type`='update' and old['sku_num'] is not null and (case (`after`['sku_num'] as int) > case (old['sku_num'] as int))
    )


create table ods_professional(
    `op` string,
    `before` map<string,string>,
    `after` map<string,string>,
    `source` map<string,string>,
    `ts_ms` bigint,
    proc_time as proctime()
)


create table aaa(
    id string,
    user_id string,
    sku_id string,
    sku_num string,
    ts bigint
)

create table ods_professional(
    `op` string,
    `before` map<string,string>,
    `after` map<string,string>,
    `source` map<string,string>,
    `ts_ms` bigint,
    proc_time as proctime()
)


select
    `after`['id'] id,
    `after`['order_id'],
    `after`['sku_id'],
    `after`['sku_name'],
    `after`['create_tiem'],
    `after`['source_id'],
    `after`['source_type'],
    `after`['sku_num'],
    cast(cast(`after`['sku_num'] as decimal(16,2))*
         cast(`after`['sku_price'] as decimal(16,2)) as string)split_original_amount,
    `after`['split_total_amount'],
    `after`['split_activity_amount'],
    `after`['split_coupon_amount'],
    ts
from ods_professional
where `after`['db']='realtime_v1'
and `after`['table']='order_detail'
and `op`='r'




Table orderInfo = tEnv.sqlQuery(
    "select " +
        "after['id'] id," +
        "after['user_id'] user_id," +
        "after['province_id'] province_id " +
        "from topic_db " +
        "where `after`['table']='order_info' " +
        "and `op`='r' ");
tEnv.createTemporaryView("order_info", orderInfo);

Table orderDetailActivity = tEnv.sqlQuery(
            "select " +
                "after['order_detail_id'] order_detail_id, " +
                "after['activity_id'] activity_id, " +
                "after['activity_rule_id'] activity_rule_id " +
                "from topic_db " +
                "where `after`['table'] = 'order_detail_activity' " +
                "and `op`='r' ");
tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);


Table orderDetailCoupon = tenv.sqlQuery(
            "select " +
                "after['order_detail_id'] order_detail_id, " +
                "after['coupon_id'] coupon_id " +
                "from topic_db " +
                "where `after`['table'] = 'order_detail_coupon' " +
                "and `op`='r' ");
tenv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        //orderDetailCoupon.execute().print();

create table page_log(
    common map<string,string>,
    page map<string,string>,
    `ts` bigint,
    et as to_timestamp(ts,3),
    watermark for et as et
)

select
    page['item'] fullword,
    et
from page_log
where page['last_page_id']='search' and page['item_type']='kerword' and page['item'] is not null

select
    keyword,
    et
from search_table
lateral table (ik_analyze(fullword)) t(keyword)



select
    window_start,
    window_end,
    keyword,
    count(*)
from TABLE (
  TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' SECONDS))
group by  window_start, window_end,keyword























