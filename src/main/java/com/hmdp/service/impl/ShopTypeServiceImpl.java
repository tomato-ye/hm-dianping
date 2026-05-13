package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPELIST_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询所有店铺类型
     * @return
     */
    @Override
    public Result queryAllShopType() {

        String key = CACHE_SHOPTYPELIST_KEY;

        // 1. 从Redis查询商铺类型缓存
        String shopTypeListJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断Redis中是否存在
        if (StrUtil.isNotBlank(shopTypeListJson)) {
            // 2.1 存在--> 直接返回商铺list
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeListJson, ShopType.class);
            return Result.ok(shopTypeList);
        }

        // 2.2 不存在--> 直接查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        // 3. 判断数据库中是否存在
        if (shopTypeList == null || shopTypeList.isEmpty()) {
            // 3.1 不存在--> 返回错误信息
            return Result.fail("店铺类型不存在！");
        }

        // 3.2 存在--> 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));

        // 4. 返回
        return Result.ok(shopTypeList);

    }

}
