package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.AscriptionDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.AscriptionDTO;
import com.chilunyc.process.entity.ENUM.PublicEnum;
import com.chilunyc.process.service.enterprise.AscriptionService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("ascriptionImpl")
public class AscriptionImpl extends BaseServiceImpl<AscriptionDao, AscriptionDTO> implements AscriptionService {

    @Autowired
    private AscriptionDao ascriptionDao;
    private final String[] PUBLIC_QUARTERS = {"Q1", "Q2", "Q3"};
    private final String PUBLIC_QUARTER = "Q4";

    /**
     * 更新当前行业归属算法
     */
    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
//        读取年份行业所属和原始数据版本差异
        List<BaseEntityDTO> list = ascriptionDao.findAllYear();
        String minYear = list.stream().min(Comparator.comparing(BaseEntityDTO::getYear)).get().getYear();
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> findByEntityIds(baseEntityDTO, minYear)
            );
            futureList.add(future);

        }
        FutureGetUtil.futureGet(futureList);
    }

    @Override
    public List<AscriptionDTO> findByYearEnterpriseList(String year, String quarter, String month) {
        return ascriptionDao.findByYearEnterpriseList(year, quarter, month);
    }

    @Override
    public List<AscriptionDTO> findByYearAndQuarter(String year, String quarter) {
        QueryWrapper queryWrapper=new QueryWrapper();
        queryWrapper.eq("year",year);
        queryWrapper.eq("quarter",quarter);
        return list(queryWrapper);
    }

    private void findByEntityIds(BaseEntityDTO baseEntityDTO, String minYear) {
//        根据年份和版本获取CICS1行业下变化的企业信息
        List<BaseEntityDTO> yearList = ascriptionDao.findByEntityIds(baseEntityDTO.getYear());
        List<BaseEntityDTO> baseList=Lists.newArrayList();
        Map<Integer,List<BaseEntityDTO>> map=yearList.stream().collect(Collectors.groupingBy(a->a.getEnterpriseId()));
        for(List<BaseEntityDTO> list:map.values()){
            baseList.add( list.stream().max(Comparator.comparingDouble(a->a.getIndicatorValue())).get());
        }
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO yearAsc : baseList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                maxCICSToYear(yearAsc, minYear));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }


    private void deleteByYearEntity(List<BaseEntityDTO> yearList, String year,String  minYear) {
        if (yearList.size() > 0) {
            List<Integer> list = yearList.stream().map(a -> a.getEnterpriseId()).collect(Collectors.toList());
            QueryWrapper<AscriptionDTO> queryWrapper = new QueryWrapper<>();
            if (ObjectUtil.equal(year, minYear)) {
                queryWrapper.eq("year", year).in("enterprise_id", list);
                this.deleteEntities(queryWrapper);
            }else{
                queryWrapper.eq("quarter", PublicEnum.PUBLIC_QUARTER);
                this.deleteEntities(queryWrapper);
                year = String.valueOf(Integer.parseInt(year) + 1);
                for(String quarter:PublicEnum.PUBLIC_QUARTERS) {
                    queryWrapper = new QueryWrapper<>();
                    queryWrapper.eq("year", year).in("enterprise_id", list).eq("quarter", quarter);
                    this.deleteEntities(queryWrapper);
                }
            }


        }
    }

    /**
     * 读取企业所在年和季度最大营业规模下CICS1行业并写入数据库
     */
    private void maxCICSToYear(BaseEntityDTO yearAsc, String minYear) {
        //   AscriptionDTO ascriptionDTO = ascriptionDao.findByYearEntity(year, entityId);
        AscriptionDTO ascriptionDTO = new AscriptionDTO();
        BeanUtil.copyProperties(yearAsc, ascriptionDTO);
        if (!BeanUtil.isEmpty(ascriptionDTO)) {
//            插入Q4季度数据
            ascriptionDTO.setQuarter(PUBLIC_QUARTER);
            updateOrInsert(ascriptionDTO);
//                插入其他三季度数据
            quarterToUpdate(ascriptionDTO, yearAsc.getYear(), minYear);
        }
    }

    /**
     * 处理一年内其他季度数据，年维度【当年Q4，下年Q1，下年Q2，下年Q3】,如果是最小一年按照当年【Q1,Q2,Q3,Q4】
     *
     * @param ascriptionDTO 当年企业行业数据
     * @param year          年份
     * @param minYear       最小年份
     */
    private void quarterToUpdate(AscriptionDTO ascriptionDTO, String year, String minYear) {
        if (ObjectUtil.equal(year, minYear)) {
            ascriptionDTO.setYear(minYear);
            forQuarter(ascriptionDTO);
        }
        year = String.valueOf(Integer.parseInt(year) + 1);
        ascriptionDTO.setYear(year);
        forQuarter(ascriptionDTO);
    }

    private void forQuarter(AscriptionDTO ascriptionDTO) {
        for (String q : PUBLIC_QUARTERS) {
            ascriptionDTO.setQuarter(q);
            updateOrInsert(ascriptionDTO);
        }
    }

    /**
     * 插入
     *
     * @param ascriptionDTO
     */
    private void updateOrInsert(AscriptionDTO ascriptionDTO) {
        QueryWrapper<AscriptionDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.eq("year",ascriptionDTO.getYear()).eq("enterprise_id",ascriptionDTO.getEnterpriseId()).eq("quarter",ascriptionDTO.getQuarter());
      AscriptionDTO fDTO=  this.getSingleEntity(queryWrapper);
      if(!BeanUtil.isEmpty(fDTO)) {
          ascriptionDTO.setId(fDTO.getId());
      }else{
          ascriptionDTO.setId(null);
          ascriptionDTO.setCreateTime(new Date());
      }

        ascriptionDTO.setUpdateTime(new Date());
        this.createOrUpdateEntity(ascriptionDTO);

    }



}
