package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.chilunyc.process.dao.industry.CicsClassifyDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.industry.CicsClassifyDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSummaryDTO;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.entity.DTO.system.SysClassifyDTO;
import com.chilunyc.process.service.industry.CicsClassifyService;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.chilunyc.process.service.industry.CicsSummaryService;
import com.chilunyc.process.service.industry.CleanIndicatorService;
import com.chilunyc.process.service.system.SysClassifyService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("cicsClassifyImpl")
public class CicsClassifyImpl extends BaseServiceImpl<CicsClassifyDao,CicsClassifyDTO> implements CicsClassifyService {
    @Autowired
    private CicsClassifyDao cicsClassifyDao;
    @Autowired
    private CleanIndicatorService cleanIndicatorService;
    @Autowired
    private CicsSummaryService cicsSummaryService;
    @Autowired
    private SysClassifyService sysClassifyService;
    @Autowired
    private CicsIndustryLevelService cicsIndustryLevelService;

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {

       // List<Future> futureList = Lists.newArrayList();
        for (BaseFieldDTO baseFieldDTO : enumFieldList) {
//            Future future = ExecutorBuilderUtil.pool.submit(() ->
//                rankToCalculation(baseFieldDTO));
//            futureList.add(future);
             rankToCalculation(baseFieldDTO);
        }
       // FutureGetUtil.futureGet(futureList);
    }

    @Override
    public void calculationIR(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {

        // List<Future> futureList = Lists.newArrayList();
        for (BaseFieldDTO baseFieldDTO : enumFieldList) {
//            Future future = ExecutorBuilderUtil.pool.submit(() ->
//                rankToCalculation(baseFieldDTO));
//            futureList.add(future);
            rankToCalculationV1(baseFieldDTO);
        }
        // FutureGetUtil.futureGet(futureList);
    }

    @Override
    public void IRCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        for (BaseFieldDTO baseFieldDTO : enumFieldList) {
            List<BaseEntityDTO> list = cicsClassifyDao.findByYQ(baseFieldDTO);
            for (BaseEntityDTO baseEntityDTO : list) {
                IRSortRankCalculation(baseEntityDTO, baseFieldDTO);
            }
        }
    }


    @Override
    public List<CicsClassifyDTO> getByField(Integer filedId) {
        QueryWrapper<CicsClassifyDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.eq("field_id",filedId);
        return list(queryWrapper);
    }

    @Override
    public Page<CicsClassifyDTO> getByField(Integer filedId, Integer pageNo, Integer pageSize) {
        QueryWrapper<CicsClassifyDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.eq("field_id",filedId);
        Page<CicsClassifyDTO> page =new Page<>();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        Page<CicsClassifyDTO> page1 = page(page, queryWrapper);
        return page1;
    }

    @Override
    public List<CicsClassifyDTO> findByYQAvg(Integer fieldId, String yearMQ) {
        return cicsClassifyDao.findByYQAvg(fieldId,yearMQ);
    }

    @Override
    public List<CicsClassifyDTO> findByYQAvgThree(Integer fieldId, String yearMQ, String startYearMQ) {
        return cicsClassifyDao.findByYQAvgThree(fieldId,yearMQ,startYearMQ);
    }

    /**
     * 行业数据
     * @return
     */
    private List<CicsIndustryLevelDTO> getByCicsIndustryLevelList() {
        List<CicsIndustryLevelDTO> list = cicsIndustryLevelService.getEntityList(new QueryWrapper());
        return list;
    }


    /** IR分组算法
     * @param baseEntityDTO
     * @param baseFieldDTO
     */
    private void IRSortRankCalculation(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> list = cleanIndicatorService.getEntityList(queryWrapper);


        Map<Integer, Double> rightMap = list.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));
        BaseEntityDTO leftBaseEntityDTO = yearAndQuarter(baseEntityDTO);
        queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", leftBaseEntityDTO.getYear()).eq("quarter", leftBaseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> leftList = cleanIndicatorService.getEntityList(queryWrapper);
        Map<Integer, Double> leftMap = leftList.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));

        List<SysClassifyDTO> classifyDTOList = sysClassifyService.industryLevelIR(baseFieldDTO.getParamName());
        List<CicsClassifyDTO> cicsClassifyDTOList = Lists.newArrayList();
        IRSortGroup(list, classifyDTOList, baseEntityDTO, baseFieldDTO, cicsClassifyDTOList);
        percentage(cicsClassifyDTOList, rightMap, leftMap);
        QueryWrapper<CicsClassifyDTO> cicsClassifyDTOQueryWrapper = new QueryWrapper<>();
        cicsClassifyDTOQueryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getRightFieldId());
        cicsClassifyDao.delete(cicsClassifyDTOQueryWrapper);
        cicsClassifyDTOList.forEach(a -> {
            cicsClassifyDao.insert(a);
        });
    }


    /**
     * IR 分组内排行算法
     * @param list
     * @param classifyDTOList
     * @param baseEntityDTO
     * @param baseFieldDTO
     * @param cicsClassifyDTOList
     */
    private void IRSortGroup(List<CleanIndicatorDTO> list, List<SysClassifyDTO> classifyDTOList, BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO, List<CicsClassifyDTO> cicsClassifyDTOList) {
        list = list.stream().sorted(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());
        int sort = 1;
        Double value = null;
        int size = list.size();
        for (int i = 0; i < list.size(); i++) {
            CleanIndicatorDTO cleanIndicatorDTO = list.get(i);
            if (!Objects.equals(value, cleanIndicatorDTO.getIndicatorValue())) {
                value = cleanIndicatorDTO.getIndicatorValue();
                sort = i + 1;
            }
            for (SysClassifyDTO sysClassifyDTO : classifyDTOList) {
                Double doubleCount = Math.floor(sysClassifyDTO.getPercentageEnd() * size);
                int count = doubleCount.intValue();
                if (sort <= count) {
                    CicsClassifyDTO cicsClassifyDTO = new CicsClassifyDTO();
                    cicsClassifyDTO.setClassName(sysClassifyDTO.getClassName());
                    cicsClassifyDTO.setClassGear(sysClassifyDTO.getClassGear());
                    cicsClassifyDTO.setClassGearTotal(classifyDTOList.size() + "");
                    cicsClassifyDTO.setRank(sort);
                    cicsClassifyDTO.setCicsId(cleanIndicatorDTO.getCicsId());
                    cicsClassifyDTO.setRankTotal(size);//  这个排名未区分资产象限
                    cicsClassifyDTO.setFieldId(baseFieldDTO.getRightFieldId());
                    cicsClassifyDTO.setYear(baseEntityDTO.getYear());
                    cicsClassifyDTO.setQuarter(baseEntityDTO.getQuarter());
                    cicsClassifyDTO.setVersion(baseEntityDTO.getVersion());
                    cicsClassifyDTO.setCreateTime(new Date());
                    cicsClassifyDTOList.add(cicsClassifyDTO);
                    break;
                }
            }
        }
    }


    /**
     * 获取变换的年份和季度
     *
     * @param baseFieldDTO
     */
    private void rankToCalculation(BaseFieldDTO baseFieldDTO) {
        List<BaseEntityDTO> list = cicsClassifyDao.findByYQ(baseFieldDTO);
        //  List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            // Future future = ExecutorBuilderUtil.pool.submit(() -> rankSortTOCalculation(baseEntityDTO, baseFieldDTO));
            //  futureList.add(future);
            rankSortTOCalculation(baseEntityDTO, baseFieldDTO);
        }
        //FutureGetUtil.futureGet(futureList);
    }
    /**
     * 获取变换的年份和季度
     *
     * @param baseFieldDTO
     */
    private void rankToCalculationV1(BaseFieldDTO baseFieldDTO) {
        List<BaseEntityDTO> list = cicsClassifyDao.findByYQ(baseFieldDTO);
        //  List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            // Future future = ExecutorBuilderUtil.pool.submit(() -> rankSortTOCalculation(baseEntityDTO, baseFieldDTO));
            //  futureList.add(future);
            rankSortTOCalculationV1(baseEntityDTO, baseFieldDTO);
        }
        //FutureGetUtil.futureGet(futureList);
    }
    /**
     * @param baseEntityDTO
     * @param baseFieldDTO
     */
    private void rankSortTOCalculation(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> list = cleanIndicatorService.getEntityList(queryWrapper);
        Map<Integer, Double> rightMap = list.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));
        BaseEntityDTO leftBaseEntityDTO = yearAndQuarter(baseEntityDTO);
        queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", leftBaseEntityDTO.getYear()).eq("quarter", leftBaseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> leftList = cleanIndicatorService.getEntityList(queryWrapper);
        Map<Integer, Double> leftMap = leftList.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));
        BaseEntityDTO summaryBaseEntityDTO = yearAndQuarterSummary(baseEntityDTO);
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("year", summaryBaseEntityDTO.getYear()).eq("quarter", summaryBaseEntityDTO.getQuarter()).isNotNull("assets_type").isNotNull("growth_type");
        List<CicsSummaryDTO> summaryDTOList = cicsSummaryService.getEntityList(wrapper);
        Map<String, List<CicsSummaryDTO>> cicsMap = summaryDTOList.stream().collect(Collectors.groupingBy(a -> a.getGrowthType() + "" + a.getAssetsType()));

        QueryWrapper<SysClassifyDTO> classifyDTOQueryWrapper = new QueryWrapper<>();
        classifyDTOQueryWrapper.eq("code", baseFieldDTO.getParamName());
        List<SysClassifyDTO> classifyDTOList = sysClassifyService.getEntityList(classifyDTOQueryWrapper);
        List<CicsClassifyDTO> cicsClassifyDTOList = Lists.newArrayList();
        for(List<CicsSummaryDTO> summaryDTOS:cicsMap.values()) {
            groupSort(summaryDTOS, list, classifyDTOList, baseEntityDTO, baseFieldDTO, cicsClassifyDTOList);
        }
        percentage(cicsClassifyDTOList, rightMap, leftMap);

        QueryWrapper<CicsClassifyDTO> cicsClassifyDTOQueryWrapper = new QueryWrapper<>();
        cicsClassifyDTOQueryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getRightFieldId());
        cicsClassifyDao.delete(cicsClassifyDTOQueryWrapper);
        cicsClassifyDTOList.forEach(a -> {
            cicsClassifyDao.insert(a);
        });


    }

    /**
     * @param baseEntityDTO
     * @param baseFieldDTO
     */
    private void rankSortTOCalculationV1(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> list = cleanIndicatorService.getEntityList(queryWrapper);
        Map<Integer, Double> rightMap = list.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));
        BaseEntityDTO leftBaseEntityDTO = yearAndQuarter(baseEntityDTO);
        queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", leftBaseEntityDTO.getYear()).eq("quarter", leftBaseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> leftList = cleanIndicatorService.getEntityList(queryWrapper);
        Map<Integer, Double> leftMap = leftList.stream().collect(Collectors.toMap(a -> a.getCicsId(), a -> a.getIndicatorValue()));
        BaseEntityDTO summaryBaseEntityDTO = yearAndQuarterSummary(baseEntityDTO);
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("year", summaryBaseEntityDTO.getYear()).eq("quarter", summaryBaseEntityDTO.getQuarter()).isNotNull("assets_type").isNotNull("growth_type");
        List<CicsSummaryDTO> summaryDTOList = cicsSummaryService.getEntityList(wrapper);
        Map<String, List<CicsSummaryDTO>> cicsMap = summaryDTOList.stream().collect(Collectors.groupingBy(a -> a.getGrowthType() + "" + a.getAssetsType()));

        QueryWrapper<SysClassifyDTO> classifyDTOQueryWrapper = new QueryWrapper<>();
        classifyDTOQueryWrapper.eq("code", baseFieldDTO.getParamName());
        List<SysClassifyDTO> classifyDTOList = sysClassifyService.getEntityList(classifyDTOQueryWrapper);
        List<CicsClassifyDTO> cicsClassifyDTOList = Lists.newArrayList();
        for(List<CicsSummaryDTO> summaryDTOS:cicsMap.values()) {
            groupSortV1(summaryDTOS, list, classifyDTOList, baseEntityDTO, baseFieldDTO, cicsClassifyDTOList);
        }
        percentage(cicsClassifyDTOList, rightMap, leftMap);

        QueryWrapper<CicsClassifyDTO> cicsClassifyDTOQueryWrapper = new QueryWrapper<>();
        cicsClassifyDTOQueryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getRightFieldId());
        cicsClassifyDao.delete(cicsClassifyDTOQueryWrapper);
        cicsClassifyDTOList.forEach(a -> {
            cicsClassifyDao.insert(a);
        });


    }


    /**
     * 分组内排行算法
     *
     * @param summaryDTOList
     * @param list
     * @param classifyDTOList
     * @param baseEntityDTO
     * @param baseFieldDTO
     * @param cicsClassifyDTOList
     */
    private void groupSort(List<CicsSummaryDTO> summaryDTOList, List<CleanIndicatorDTO> list, List<SysClassifyDTO> classifyDTOList, BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO, List<CicsClassifyDTO> cicsClassifyDTOList) {
        list = list.stream().filter(a -> summaryDTOList.stream().map(b -> b.getCicsId()).collect(Collectors.toList()).contains(a.getCicsId())).collect(Collectors.toList());
        list = list.stream().sorted(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());
        int sort = 1;
        Double value = null;
        int size = list.size();
        for (int i = 0; i < list.size(); i++) {
            CleanIndicatorDTO cleanIndicatorDTO = list.get(i);
            if (!Objects.equals(value, cleanIndicatorDTO.getIndicatorValue())) {
                value = cleanIndicatorDTO.getIndicatorValue();
                sort = i + 1;
            }
            for (SysClassifyDTO sysClassifyDTO : classifyDTOList) {
                Double doubleCount = Math.floor(sysClassifyDTO.getPercentageEnd() * size);
                int count = doubleCount.intValue();
                Double doubleCountStart=Math.floor(sysClassifyDTO.getPercentageStart()*size);
                int startCount=doubleCountStart.intValue();
                if (sort <= count) {
                    CicsClassifyDTO cicsClassifyDTO = new CicsClassifyDTO();
                    cicsClassifyDTO.setClassName(sysClassifyDTO.getClassName());
                    cicsClassifyDTO.setClassGear(sysClassifyDTO.getClassGear());
                    cicsClassifyDTO.setClassGearTotal(classifyDTOList.size() + "");
                    cicsClassifyDTO.setRank(sort);
                    cicsClassifyDTO.setCicsId(cleanIndicatorDTO.getCicsId());
                    cicsClassifyDTO.setRankTotal(summaryDTOList.size());//todo
                    cicsClassifyDTO.setFieldId(baseFieldDTO.getRightFieldId());
                    cicsClassifyDTO.setYear(baseEntityDTO.getYear());
                    cicsClassifyDTO.setQuarter(baseEntityDTO.getQuarter());
                    cicsClassifyDTO.setVersion(baseEntityDTO.getVersion());
                    cicsClassifyDTO.setCreateTime(new Date());
                    cicsClassifyDTOList.add(cicsClassifyDTO);
                    break;
                }
            }
        }
    }
    /**
     * 分组内排行算法
     *
     * @param summaryDTOList
     * @param list
     * @param classifyDTOList
     * @param baseEntityDTO
     * @param baseFieldDTO
     * @param cicsClassifyDTOList
     */
    private void groupSortV1(List<CicsSummaryDTO> summaryDTOList, List<CleanIndicatorDTO> list, List<SysClassifyDTO> classifyDTOList, BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO, List<CicsClassifyDTO> cicsClassifyDTOList) {
        list = list.stream().filter(a -> summaryDTOList.stream().map(b -> b.getCicsId()).collect(Collectors.toList()).contains(a.getCicsId())).collect(Collectors.toList());
        list = list.stream().sorted(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());
        int sort = 1;
        Double value = null;
        int size = list.size();
        for (int i = 0; i < list.size(); i++) {
            CleanIndicatorDTO cleanIndicatorDTO = list.get(i);
            if (!Objects.equals(value, cleanIndicatorDTO.getIndicatorValue())) {
                value = cleanIndicatorDTO.getIndicatorValue();
                sort = i + 1;
            }
            for (SysClassifyDTO sysClassifyDTO : classifyDTOList) {
                Double doubleCount = Math.floor(sysClassifyDTO.getPercentageEnd() * size);
                int count = doubleCount.intValue();
                Double doubleCountStart=Math.floor(sysClassifyDTO.getPercentageStart()*size);
                int startCount=doubleCountStart.intValue();
                if (sort <= count) {
                    CicsClassifyDTO cicsClassifyDTO = new CicsClassifyDTO();
                    cicsClassifyDTO.setClassName(sysClassifyDTO.getClassName());
                    cicsClassifyDTO.setClassGear(sysClassifyDTO.getClassGear());
                    cicsClassifyDTO.setClassGearTotal(classifyDTOList.size() + "");
                    cicsClassifyDTO.setRank(sort-startCount);
                    cicsClassifyDTO.setCicsId(cleanIndicatorDTO.getCicsId());
                    cicsClassifyDTO.setRankTotal(count);//todo
                    cicsClassifyDTO.setFieldId(baseFieldDTO.getRightFieldId());
                    cicsClassifyDTO.setYear(baseEntityDTO.getYear());
                    cicsClassifyDTO.setQuarter(baseEntityDTO.getQuarter());
                    cicsClassifyDTO.setVersion(baseEntityDTO.getVersion());
                    cicsClassifyDTO.setCreateTime(new Date());
                    cicsClassifyDTOList.add(cicsClassifyDTO);
                    break;
                }
            }
        }
    }

    /**
     * @param list
     * @param map
     */
    private void percentage(List<CicsClassifyDTO> list, Map<Integer, Double> map, Map<Integer, Double> leftMap) {
        list.forEach(a -> {
            if (map.containsKey(a.getCicsId())) {
                Double v1 = map.get(a.getCicsId());
                Double v2 = Double.valueOf(0);
                Double value = Double.valueOf(0);
                if (leftMap.containsKey(a.getCicsId())) {
                    v2 = leftMap.get(a.getCicsId());
                }
                if (v2 != 0) {
                    value = (v1 - v2) / v2;
                }
                a.setPercentage(NumberUtil.round(value,4).doubleValue());
            }
        });
    }

    /**
     * 年份季度-1
     *
     * @param baseEntityDTO
     * @return
     */
    private BaseEntityDTO yearAndQuarter(BaseEntityDTO baseEntityDTO) {
        String quarter = baseEntityDTO.getQuarter();
        BaseEntityDTO entityDTO = new BaseEntityDTO();
        BeanUtil.copyProperties(baseEntityDTO, entityDTO);
        if (Objects.equals("Q1", quarter)) {
            String year = baseEntityDTO.getYear();
            Integer y = Integer.valueOf(year) - 1;
            entityDTO.setYear(y + "");
            quarter = "Q4";
        } else {
            quarter = StrUtil.sub(quarter, 1, 2);
            Integer q = Integer.valueOf(quarter) - 1;
            quarter = "Q" + q;
        }
        entityDTO.setQuarter(quarter);
        return entityDTO;
    }

    /**
     * 获取上年或当年数据
     *
     * @param baseEntityDTO
     * @return
     */
    private BaseEntityDTO yearAndQuarterSummary(BaseEntityDTO baseEntityDTO) {
        BaseEntityDTO entityDTO = new BaseEntityDTO();
        BeanUtil.copyProperties(baseEntityDTO, entityDTO);
        if (!Objects.equals("Q4", baseEntityDTO.getQuarter())) {
            String year = baseEntityDTO.getYear();
            Integer y = Integer.valueOf(year) - 1;
            entityDTO.setYear(y + "");
        }
        entityDTO.setQuarter("Q4");
        return entityDTO;
    }
}
