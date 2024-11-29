package com.seckill.search.service.impl;

import com.alibaba.fastjson.JSON;
import com.seckill.goods.feign.SkuFeign;
import com.seckill.goods.pojo.Sku;
import com.seckill.search.pojo.SkuInfo;
import com.seckill.search.service.SkuInfoService;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SkuInfoServiceImpl implements SkuInfoService {

    @Autowired
    private SkuFeign skuFeign;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private String index_name = "goodsindex";

    public void addAll(){
        Integer total = skuFeign.count();

        int page = 1;
        int size = 3200;

        int totalPages = total % size == 0 ? total / size : (total / size) + 1;

        for(int i = 0; i < totalPages; i++){
            List<Sku> skuList = skuFeign.list(page, size);

            page ++;

            List<SkuInfo> skuInfoList = JSON.parseArray(JSON.toJSONString(skuList), SkuInfo.class);

            BulkRequest bulkRequest = new BulkRequest();

            for(SkuInfo info : skuInfoList){
                skuInfoConverter(info);

                String data = JSON.toJSONString(info);
                IndexRequest indexRequest = new IndexRequest(index_name);
                indexRequest.id(info.getId() + "").source(data, XContentType.JSON);

                bulkRequest.add(indexRequest);
            }
            try {
                restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void addAll2(){
        Integer total = skuFeign.count();

        AtomicInteger page = new AtomicInteger(1);
        int size = 3200;

        int totalPages = total % size == 0 ? total / size : (total / size) + 1;

        CountDownLatch countDownLatch = new CountDownLatch(totalPages);

        ExecutorService pool = Executors.newFixedThreadPool(2);

        for(int i = 0; i < totalPages; i++){
            pool.execute(() -> {
                List<Sku> skuList = skuFeign.list(page.getAndIncrement(), size);

                List<SkuInfo> skuInfoList = JSON.parseArray(JSON.toJSONString(skuList), SkuInfo.class);

                BulkRequest bulkRequest = new BulkRequest();

                for(SkuInfo info : skuInfoList){
                    skuInfoConverter(info);

                    String data = JSON.toJSONString(info);
                    IndexRequest indexRequest = new IndexRequest(index_name);
                    indexRequest.id(info.getId() + "").source(data, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }

                try {
                    restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
    }

    // 将开始时间转换成字符串类型
    private void skuInfoConverter(SkuInfo skuInfo) {
        if (skuInfo.getSeckillBegin() != null) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
            skuInfo.setBgtime(simpleDateFormat.format(skuInfo.getSeckillBegin()));
        }
    }

    /**
     * 增量操作
     * ->添加索引   type=1
     * ->修改索引   type=2
     * ->删除索引   type=3
     */
    @Override
    public void modify(Integer type, SkuInfo skuInfo) {
        try {
            if (type == 1 || type == 2) {
                // 将开始时间转换成字符串类型
                skuInfoConverter(skuInfo);

                //将对象转为json
                String data = JSON.toJSONString(skuInfo);
                IndexRequest request = new IndexRequest(index_name).id(skuInfo.getId()).source(data, XContentType.JSON);

                //增加-修改
                restHighLevelClient.index(request, RequestOptions.DEFAULT);

            } else {
                //删除
                DeleteRequest deleteRequest = new DeleteRequest(index_name, skuInfo.getId());
                restHighLevelClient.delete(new DeleteRequest(index_name, skuInfo.getId()), RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
