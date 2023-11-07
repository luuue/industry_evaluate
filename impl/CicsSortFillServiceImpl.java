package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsSortDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSortCountDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSortDTO;
import com.chilunyc.process.service.industry.CicsSortFillService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 指标值季度按cics1排序
 */
@Service("cicsSortFillServiceImpl")
public class CicsSortFillServiceImpl extends BaseServiceImpl<CicsSortDao, CicsSortDTO> implements CicsSortFillService {
    @Autowired
    private CicsSortDao cicsSortDao;


    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Integer> fieldIds = fieldList.stream().map(a -> a.getLeftFieldId()).collect(Collectors.toList());
        //   List<Future> futureList = Lists.newArrayList();
        for (Integer field : fieldIds) {
            //   Future future = ExecutorBuilderUtil.pool.submit(() ->
            startFillData(field);
            //   futureList.add(future);
        }
        //  FutureGetUtil.futureGet(futureList);
    }

    private void startFillData(Integer field) {
//        按照行业，年，季度维度变化原始数据
        CicsSortDTO cicsSortVm = new CicsSortCountDTO();
        cicsSortVm.setFieldId(field);
        startFillData(cicsSortVm);
    }


    //填充数据(排名数据及)
    private void startFillData(CicsSortDTO cicsSortVm) {
        List<Integer> enterpriseIds = getEnterpriseByNewest(cicsSortVm);
        List<Future> futureList = Lists.newArrayList();
        enterpriseIds.forEach(enterpriseId -> {
            ExecutorBuilderUtil.incrementAndGet(30,true);
            Future future = ExecutorBuilderUtil.pool.submit(() -> getEnterpriseAllYear(enterpriseId, cicsSortVm));
            futureList.add(future);
        });
        FutureGetUtil.futureGet(futureList);
    }

    private void getEnterpriseAllYear(Integer enterpriseId, CicsSortDTO cicsSortVm) {
        List<CicsSortCountDTO> CicsSortCount = getEnterpriseAllYear(enterpriseId, cicsSortVm.getFieldId());
        List<CicsSortDTO> cicsSortDTOS = fillData(CicsSortCount, enterpriseId, cicsSortVm.getFieldId());
        for(CicsSortDTO cicsSortDTO:cicsSortDTOS){//
            updateById(cicsSortDTO);
        }
        ExecutorBuilderUtil.decrementAndGet();
    }

    private List<CicsSortDTO> fillData(List<CicsSortCountDTO> enterpriseAllYearData, Integer enterpriseId, Integer fieldid) {
        //先进行分组
        List<List<CicsSortCountDTO>> list = new ArrayList();
        Integer index = 0;
        for (int i = 0; i < enterpriseAllYearData.size(); i++) {//这个是Q4即年度排名
            if (i == 0) {
                List<CicsSortCountDTO> groupList = new ArrayList();
                groupList.add(enterpriseAllYearData.get(i));
                list.add(groupList);
            }
            if (i > 0) {
                CicsSortCountDTO cicsSortCountDTO = list.get(index).get(0);
                //如果且相等   行业不为空!Objects.isNull(cicsSortCountDTO.getCicsId())&&可以不考虑
                if (Objects.equals(cicsSortCountDTO.getCicsId(), enterpriseAllYearData.get(i).getCicsId())) {
                    list.get(index).add(enterpriseAllYearData.get(i));//则把当前数据放入
                } else {
                    List<CicsSortCountDTO> groupList = new ArrayList();
                    groupList.add(enterpriseAllYearData.get(i));
                    list.add(groupList);
                    index++;
                }
            }
        }
        // 对好组的数据进行排名
        //第一步填充年度排名
        List<CicsSortCountDTO> reultList = new ArrayList();
        for (List<CicsSortCountDTO> groupList : list) {
            for (int i = 0; i < groupList.size(); i++) {
                CicsSortCountDTO cicsSortCountDTO = groupList.get(i);
                if (Objects.isNull(cicsSortCountDTO.getIndicatorSort())) {
                    if (i == 0) {
                        for (int j = 0; j < groupList.size(); j++) {
                            CicsSortCountDTO hasValue = groupList.get(j);
                            if (Objects.nonNull(hasValue.getIndicatorSort())) {
                                Integer indicatorSort = hasValue.getIndicatorSort();
                                if (Objects.nonNull(indicatorSort)) {
                                    if (indicatorSort < 1) {
                                        indicatorSort = 1;
                                    }
                                    if (indicatorSort > cicsSortCountDTO.getCicsEnCount()) {
                                        indicatorSort = cicsSortCountDTO.getCicsEnCount();
                                    }
                                    cicsSortCountDTO.setIndicatorSort(indicatorSort);
                                    if (cicsSortCountDTO.getCicsEnCount() != 0) {
                                        cicsSortCountDTO.setDesirability(Double.valueOf(hasValue.getIndicatorSort()) / Double.valueOf(cicsSortCountDTO.getCicsEnCount()));
                                    }
                                }
                                break;
                            }
                        }
                    } else if (i == 1) {
                        CicsSortCountDTO first = groupList.get(0);
                        Integer indicatorSort = first.getIndicatorSort();
                        if (Objects.nonNull(indicatorSort)) {
                            if (indicatorSort < 1) {
                                indicatorSort = 1;
                            }
                            if (indicatorSort > cicsSortCountDTO.getCicsEnCount()) {
                                indicatorSort = cicsSortCountDTO.getCicsEnCount();
                            }
                            cicsSortCountDTO.setIndicatorSort(indicatorSort);
                        }
                    } else if (i > 1) {
                        //求出上一个排名
                        CicsSortCountDTO lastSortCountDTO = groupList.get(i - 1);
                        //求出上上一个排名
                        CicsSortCountDTO last2cicsSortCountDTO = groupList.get(i - 2);
                        if (Objects.isNull(last2cicsSortCountDTO.getIndicatorSort()) || Objects.isNull(lastSortCountDTO.getIndicatorSort())) {
                        } else {
                            Integer indicatorsort = lastSortCountDTO.getIndicatorSort() + (lastSortCountDTO.getIndicatorSort() - last2cicsSortCountDTO.getIndicatorSort());
                            if (Objects.nonNull(indicatorsort)&&Objects.nonNull( cicsSortCountDTO.getCicsEnCount())) {
                                if (indicatorsort < 1) {
                                    indicatorsort = 1;
                                }
                                if (indicatorsort > cicsSortCountDTO.getCicsEnCount() ) {
                                    indicatorsort = cicsSortCountDTO.getCicsEnCount();
                                }
                                cicsSortCountDTO.setIndicatorSort(indicatorsort);
                                if (cicsSortCountDTO.getCicsEnCount() != 0) {
                                    cicsSortCountDTO.setDesirability(Double.valueOf(indicatorsort) / Double.valueOf(cicsSortCountDTO.getCicsEnCount()));
                                }
                                if (Objects.isNull(cicsSortCountDTO.getIndicatorSort())) {
                                }
                            }
                        }
                    }
                    cicsSortCountDTO.setIsRepair(1);
                    cicsSortCountDTO.setIsNewest(0);
                    groupList.set(i, cicsSortCountDTO);
                }
            }
            reultList.addAll(groupList);
        }

        //对处理完的数据进行整合
        List<CicsSortCountDTO> byEnterprsie = findByEnterprsie(enterpriseId, fieldid);//此处有超时提醒
        List<CicsSortDTO> collect = byEnterprsie.stream().map(a -> {
            List<CicsSortCountDTO> findYear = reultList.stream().filter(b -> Integer.valueOf(a.getYear()).equals(Integer.valueOf(b.getYear())) && a.getCicsId().equals(b.getCicsId())).collect(Collectors.toList());
            if (findYear.size() > 0) {
                CicsSortCountDTO cicsSortCountDTO = findYear.get(0);
                Integer indicatorSort = cicsSortCountDTO.getIndicatorSort();
                if (Objects.nonNull(indicatorSort)) {
                    if (indicatorSort < 1) {
                        indicatorSort = 1;
                    }
                    if (indicatorSort > a.getCicsEnCount()) {
                        indicatorSort = a.getCicsEnCount();
                    }
                    if(indicatorSort!=0){
                        a.setIndicatorSort(indicatorSort);
                        if (a.getCicsEnCount() != 0) {
                            a.setDesirability(Double.valueOf(indicatorSort) / Double.valueOf(a.getCicsEnCount()));
                        }
                    }
                }
            } else {
                //只有一个年度
                List<CicsSortCountDTO> lastSortCountDTOLs = reultList.stream().filter(b -> Integer.valueOf(a.getYear()).equals(Integer.valueOf(b.getYear()) + 1) && a.getCicsId().equals(b.getCicsId())).collect(Collectors.toList());
                List<CicsSortCountDTO> last2cicsSortCountDTOLs = reultList.stream().filter(b -> Integer.valueOf(a.getYear()).equals(Integer.valueOf(b.getYear()) + 2) && a.getCicsId().equals(b.getCicsId())).collect(Collectors.toList());
                if (lastSortCountDTOLs.size() > 0 && last2cicsSortCountDTOLs.size() > 0) {
                    CicsSortCountDTO lastSortCountDTO = lastSortCountDTOLs.get(0);
                    CicsSortCountDTO last2cicsSortCountDTO = last2cicsSortCountDTOLs.get(0);
                    if (Objects.nonNull(lastSortCountDTO.getIndicatorSort()) && Objects.nonNull(last2cicsSortCountDTO.getIndicatorSort())) {
                        Integer indicatorsort = lastSortCountDTO.getIndicatorSort() + (lastSortCountDTO.getIndicatorSort() - last2cicsSortCountDTO.getIndicatorSort());
                        if (Objects.nonNull(indicatorsort)) {
                            if (indicatorsort < 1) {
                                indicatorsort = 1;
                            }
                            if (indicatorsort > a.getCicsEnCount()) {
                                indicatorsort = a.getCicsEnCount();
                            }
                            if(indicatorsort!=0) {
                                a.setIndicatorSort(indicatorsort);
                                if (a.getCicsEnCount() != 0) {
                                    a.setDesirability(Double.valueOf(indicatorsort) / Double.valueOf(a.getCicsEnCount()));
                                }
                            }
                            if (Objects.isNull(a.getIndicatorSort())||a.getIndicatorSort()==0) {
                            }
                        }
                    }
                }
            }
            a.setIsRepair(1);
            a.setIsNewest(0);
            a.setUpdateTime(new Date());
            if (Objects.isNull(a.getIndicatorSort())) {
            }
            return a;
        }).collect(Collectors.toList());
        return collect;
    }

    private List<CicsSortCountDTO> findByEnterprsie(Integer enterpriseId, Integer fieldId) {
        List<CicsSortCountDTO> cicsCountList=new ArrayList<>();
        List<CicsSortDTO> enterprsieList=new ArrayList<>();
        List<CicsSortDTO> yqs = cicsSortDao.findYQByFiled(fieldId);
        yqs.forEach(a->{
            List<CicsSortCountDTO> cicsCountByYQ = cicsSortDao.findCicsCountByYQ(fieldId, a.getYear(), a.getQuarter());
            cicsCountList.addAll(cicsCountByYQ);
        });

        yqs.forEach(a->{
            List<CicsSortDTO> cicsSorts = cicsSortDao.findByEnterprsieNeedSort(enterpriseId, fieldId,a.getYear(),a.getQuarter());
            enterprsieList.addAll(cicsSorts);
        });
        Map<String, List<CicsSortCountDTO>> cicsCountMap = cicsCountList.stream().collect(Collectors.groupingBy(o -> o.getCicsId() + "_" + o.getYear() + "_" + o.getQuarter()));
        return   enterprsieList.stream().map(
            a->{
                CicsSortCountDTO cicsSortCountDTO=new CicsSortCountDTO();
                BeanUtil.copyProperties(a,cicsSortCountDTO);
                Optional<CicsSortCountDTO> first = cicsCountMap.get(a.getCicsId() + "_" + a.getYear()+"_"+a.getQuarter()).stream().findFirst();
                if(first.isPresent()){
                    cicsSortCountDTO.setCicsEnCount(first.get().getCicsEnCount());
                }
                return cicsSortCountDTO;
            }
        ).collect(Collectors.toList());
    }

    //获取企业全年度数据及当年所在行业企业数量
    private List<CicsSortCountDTO> getEnterpriseAllYear(Integer enterpriseId, Integer fieldId) {//有超时情况，改为分开查询
        QueryWrapper<CicsSortDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.eq("enterprise_id",enterpriseId);
        queryWrapper.eq("field_id",fieldId);
        queryWrapper.eq("quarter","Q4");
        queryWrapper.eq("is_delete","0");
        queryWrapper.orderByAsc("year");
        List<CicsSortDTO> list = list(queryWrapper);
        List<CicsSortCountDTO> cicsCounts = cicsSortDao.findCicsCount(fieldId);
        Map<String, List<CicsSortCountDTO>> cicsCountMap = cicsCounts.stream().collect(Collectors.groupingBy(o -> o.getCicsId() + "_" + o.getYear()));
        return   list.stream().map(
            a->{
                CicsSortCountDTO cicsSortCountDTO=new CicsSortCountDTO();
                BeanUtil.copyProperties(a,cicsSortCountDTO);
                Optional<List<CicsSortCountDTO>> cicsSortCountDTOS = Optional.ofNullable(cicsCountMap.get(a.getCicsId() + "_" + a.getYear()));
               if(!cicsSortCountDTOS.isPresent()){
                   return null;
               }
                Optional<CicsSortCountDTO> first =cicsSortCountDTOS.get().stream().findFirst();
                if(first.isPresent()){
                    cicsSortCountDTO.setCicsEnCount(first.get().getCicsEnCount());
                }
                return cicsSortCountDTO;
            }
        ).filter(Objects::nonNull).collect(Collectors.toList());
    }


    //获取所有有变动的企业id
    private List<Integer> getEnterpriseByNewest(CicsSortDTO cicsSortVm) {
        List<CicsSortDTO> byFieldAndIsRepair = cicsSortDao.findByFieldAndIsRepair(cicsSortVm.getFieldId());
        return byFieldAndIsRepair.stream().map(a -> a.getEnterpriseId()).collect(Collectors.toList());
    }
}
