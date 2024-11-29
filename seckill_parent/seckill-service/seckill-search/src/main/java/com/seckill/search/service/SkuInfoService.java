package com.seckill.search.service;

import com.seckill.search.pojo.SkuInfo;

public interface SkuInfoService {

    public void addAll();
    public void addAll2();

    /**
     * 单条索引操作
     * @param type: 1:增加，2：修改，3：删除
     * @param skuInfo
     */
    void modify(Integer type, SkuInfo skuInfo);
}
