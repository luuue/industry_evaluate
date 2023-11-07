package com.chilunyc.process.service.industry.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.chilunyc.process.dao.industry.CleanIndicatorDao;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.service.industry.CicsCleanIndicatorService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class CicsCleanIndicatorImpl extends BaseServiceImpl<CleanIndicatorDao, CleanIndicatorDTO> implements CicsCleanIndicatorService {
    @Override
    public List<CleanIndicatorDTO> findListByField(Integer filed, String year) {
        QueryWrapper<CleanIndicatorDTO> query=new QueryWrapper<>();
        query.eq("field_id",filed);
        if(Objects.nonNull(year)) {
            query.eq("year", year);
        }
        return list(query);
    }

    @Override
    public  Page<CleanIndicatorDTO>  findListByField(Integer filed, Integer pageNo, Integer pageSize) {
        QueryWrapper<CleanIndicatorDTO> query=new QueryWrapper<>();
        query.eq("field_id",filed);
        Page<CleanIndicatorDTO> page =new Page<>();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        Page<CleanIndicatorDTO> page1 = page(page, query);
        return page1;
    }
}
