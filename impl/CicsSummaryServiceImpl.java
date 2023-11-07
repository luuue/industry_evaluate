package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsIndicatorDao;
import com.chilunyc.process.dao.industry.CicsSummaryDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSummaryDTO;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.entity.DTO.system.SysFieldDTO;
import com.chilunyc.process.entity.ENUM.PublicEnum;
import com.chilunyc.process.service.enterprise.CleanBizIndicatorService;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.chilunyc.process.service.industry.CicsSummaryService;
import com.chilunyc.process.service.industry.CleanIndicatorService;
import com.chilunyc.process.service.system.SysFieldService;
import com.chilunyc.process.util.ConstData;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 行业基础信息分类/分组（固定资产、成长率、其他分组）
 */
@Service("cicsSummaryServiceImpl")
public class CicsSummaryServiceImpl extends BaseServiceImpl<CicsSummaryDao, CicsSummaryDTO> implements CicsSummaryService {
    @Autowired
    private CicsSummaryDao cicsSummaryDao;
    @Autowired
    private SysFieldService sysFildService;
    @Autowired
    private CicsIndicatorDao cicsIndicatorDao;
    @Autowired
    private CleanIndicatorService cleanIndicatorService;
    @Autowired
    private CicsIndustryLevelService cicsIndustryLevelService;
    @Autowired
    private CleanBizIndicatorService cleanBizIndicatorService;
    private final Logger log = LoggerFactory.getLogger(CicsSummaryServiceImpl.class);

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        Integer fixedAssetsId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.CICS_Q_FIXED_ASSETS.name());
        Integer totalAssetsId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.CICS_Q_TOTAL_ASSETS.name());
        List<Integer> fields = Lists.newArrayList();
        Integer cicsRevenuefieldId = fieldList.stream().map(a -> a.getLeftFieldId()).findFirst().get();//营收规模id
        fields.add(fixedAssetsId);
        fields.add(totalAssetsId);
        fields.add(cicsRevenuefieldId);//增长率

        // 查出所有变动行业固定资产和总资产数据及营业规模
        List<Map> chagedCicsSummary = getChagedCicsSummary(fields);

        //处理三年固定占比问题
        log.info("开始处理固占比数据..");
        calculationFixPercent(chagedCicsSummary, fixedAssetsId, totalAssetsId);
        //处理三年平营收规模均增长率问题
        log.info("开始营收增长率..");
        calculationRevenue(chagedCicsSummary, cicsRevenuefieldId);
    }

    /**
     * 处理三年固定占比问题
     */

    private void calculationFixPercent(List<Map> chagedCicsSummary, Integer fixedAssetsId, Integer totalAssetsId) {
        //计算固定资产占比特定算法----横向计算summarylist
        log.info("获取需要处理固占比的行业......");
        List<SysFieldDTO> sysFieldDTO = sysFildService.getEntityListByIds(Lists.newArrayList(fixedAssetsId, totalAssetsId));
        List<Map> changedCics = getChangedCics(chagedCicsSummary, sysFieldDTO);//查出的是需要修改固定占比的cics 年 季
        //改变固定占比及调整后固定占比
        log.info("调整固占比......");
        List<CicsSummaryDTO> cicsSummaryDTOS = setChangedFixedAssetsPercentage(changedCics, fixedAssetsId, totalAssetsId);
        //
        List<Integer> cicsIds = changedCics.stream().map(a -> Convert.toInt(a.get("cics_id"))).collect(Collectors.toList());
        log.info("计算三年平均固占比......");
        List<CicsSummaryDTO> summrarys = getsummaryByCicsIds(cicsIds);//查出的是cics 所有年度
        List<CicsSummaryDTO> cicsSummaryDTOS1 = calculationFixPercent3y(summrarys);
        //将三年固定资产占比放入数据库
        saveOrUpdateBatch(cicsSummaryDTOS1);
        log.info("计算固占比排名.....");
        updateFixedSort();
    }


    /**
     * @param list
     * @param name
     * @return
     */
    private Integer enumField(List<BaseFieldDTO> list, String name) {
        Integer value = null;
        for (BaseFieldDTO baseFieldDTO : list) {
            if (ObjectUtil.equal(baseFieldDTO.getParamName(), name)) {
                value = baseFieldDTO.getLeftFieldId();
                break;
            }
        }
        return value;
    }

    /**
     * 生成固定占比三年平均
     */
    public List<CicsSummaryDTO> calculationFixPercent3y(List<CicsSummaryDTO> summrarys) {
        Map<Integer, List<CicsSummaryDTO>> cicsGroup = summrarys.stream().collect(Collectors.groupingBy(CicsSummaryDTO::getCicsId));
        List<CicsSummaryDTO> resultSummarys = new ArrayList<>();
        cicsGroup.forEach((key, cicsGroups) -> {
                //计算三年数据
                for (int i = 0; i < cicsGroups.size(); i++) {
                    CicsSummaryDTO resultSummary = new CicsSummaryDTO();
                    CicsSummaryDTO summaryDTO = cicsGroups.get(i);
                    BeanUtil.copyProperties(summaryDTO, resultSummary);
                    if (i == 0 || i == 1) {
                        CicsSummaryDTO firstYearSummaryDTO = cicsGroups.get(0);
                        String year = firstYearSummaryDTO.getYear();
                        if (!Objects.equals(year, "2012")) {//行业不是起始于2012年
                            if (i == 0) {
                                resultSummary.setFixedAssetsRate3(firstYearSummaryDTO.getFixedAssetsRateAdjust());
                            }
                            if (i == 1) {
                                resultSummary.setFixedAssetsRate3(firstYearSummaryDTO.getFixedAssetsRateAdjust().add(summaryDTO.getFixedAssetsRateAdjust()).divide(BigDecimal.valueOf(2)));
                            }
                        }
                        resultSummarys.add(resultSummary);
                        continue;
                    }
                    CicsSummaryDTO lastSummaryDTO = cicsGroups.get(i - 1);
                    CicsSummaryDTO lastSecondSummaryDTO = cicsGroups.get(i - 2);
                    BigDecimal currentFixedRate = summaryDTO.getFixedAssetsRateAdjust();
                    BigDecimal lastFixedRate = lastSummaryDTO.getFixedAssetsRateAdjust();
                    BigDecimal lastSecondFixedRate = lastSecondSummaryDTO.getFixedAssetsRateAdjust();
                    BigDecimal sumFixedRate = currentFixedRate.add(lastFixedRate).add(lastSecondFixedRate).divide(BigDecimal.valueOf(3));
                    resultSummary.setFixedAssetsRate3(sumFixedRate);
                    resultSummarys.add(resultSummary);
                }
            }
        );
        return resultSummarys;
    }


    /**
     * 处理三年平营收规模均增长率问题
     *
     * @param
     * @return
     */
    private void calculationRevenue(List<Map> chagedCicsSummary, Integer cicsRevenuefieldId) {
        List<SysFieldDTO> sysFildDTO = sysFildService.getEntityListByIds(Lists.newArrayList(cicsRevenuefieldId));
        List<Map> changedCics = getChangedCics(chagedCicsSummary, sysFildDTO);//查出的是需要修改营收增长率的cics年 季
        //转华为需要保存的summary
        List<Integer> cicsIds = changedCics.stream().map(a -> Convert.toInt(a.get("cics_id"))).collect(Collectors.toList());
        List<Map<String, Object>> indicatoGrowthInCicsIndicator = getIndicatoGrowthInCicsIndicator(cicsIds, cicsRevenuefieldId);
        //将增长率存入sumarry表
        List<CicsSummaryDTO> cicsSummaryDTOS = updateRevenueGrowthRate(cicsIds, indicatoGrowthInCicsIndicator);
        // 对增长率进行调整
        //需要根据变化的cicsSummaryDTOS 的年度季度进行分组查询和处理(哪些年度和季度有增长率变化) 然后进行调整
        ajustRevenueGrowth(cicsSummaryDTOS);
        //根据调整后增长率计算三年平均增长率
        revenueGrowth3y(cicsIds);
        //处理年度排名
        updateRevenueGrowthSort();

    }


    //计算三年平均增长率
    private List<CicsSummaryDTO> revenueGrowth3y(List<Integer> cicsIds) {
        List<CicsSummaryDTO> sumarys = getsummaryByCicsIds(cicsIds);
        Map<Integer, List<CicsSummaryDTO>> cicsidGroup = sumarys.stream().collect(Collectors.groupingBy(CicsSummaryDTO::getCicsId));
        List<CicsSummaryDTO> resultSummarys = new ArrayList<>();
        cicsidGroup.forEach((key, gropuSumarys) -> {
            for (int i = 0; i < gropuSumarys.size(); i++) {
                CicsSummaryDTO resultSummary = new CicsSummaryDTO();
                CicsSummaryDTO summaryDTO = gropuSumarys.get(i);
                BeanUtil.copyProperties(summaryDTO, resultSummary);
                if (i == 0) {
                    resultSummarys.add(resultSummary);
                    continue;
                }
                CicsSummaryDTO lastYearGrowth = gropuSumarys.get(i - 1);
                if (i == 1 || i == 2) {
                    CicsSummaryDTO firstYearSummaryDTO = gropuSumarys.get(0);
                    String year = firstYearSummaryDTO.getYear();
                    if (!Objects.equals(year, "2012")) {//行业不是起始于2012年
                        if (i == 1) {
                            resultSummary.setRevenueGrowthRate3(summaryDTO.getRevenueGrowthRateAdjust().divide(BigDecimal.valueOf(1)));
                        }
                        if (i == 2) {
                            resultSummary.setRevenueGrowthRate3(summaryDTO.getRevenueGrowthRateAdjust().add(lastYearGrowth.getRevenueGrowthRateAdjust()).divide(BigDecimal.valueOf(2)));
                        }
                    }
                    resultSummarys.add(resultSummary);
                    continue;
                }
                CicsSummaryDTO lastSencondYearGrowth = gropuSumarys.get(i - 2);
                resultSummary.setRevenueGrowthRate3(summaryDTO.getRevenueGrowthRateAdjust().add(lastYearGrowth.getRevenueGrowthRateAdjust()).add(lastSencondYearGrowth.getRevenueGrowthRateAdjust()).divide(BigDecimal.valueOf(3)));
                resultSummarys.add(resultSummary);
            }
        });
        saveBatch(resultSummarys);
        return resultSummarys;
    }

    //调整营收增长率
    private void ajustRevenueGrowth(List<CicsSummaryDTO> cicsSummaryDTOS) {
        //查看哪些年度有增长率变化,仅处理Q4季度
        List<String> years = cicsSummaryDTOS.stream()
            .filter(cicsSummaryDTO -> ConstData.PUBLIC_QUARTER.equals(cicsSummaryDTO.getQuarter()))
            .map(CicsSummaryDTO::getYear).distinct().collect(Collectors.toList());
        for (String year : years) {
            QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
            wrapper.eq("year", year);
            wrapper.eq("quater", ConstData.PUBLIC_QUARTER);
            wrapper.eq("is_delete", 0);
            wrapper.isNotNull("revenue_growth_rate");//仅对有增长率进行处理，无增长率不参与排名
            //按原始增长率进行排名
            wrapper.orderByAsc("revenue_growth_rate");
            List<CicsSummaryDTO> yearSummarys = list(wrapper);
            //获取当年度前1.25%位置的数量
            Integer ajustCount = yearSummarys.size() * 125 / 100 / 100;
            //获取index为ajustCount+1位值，和size()-ajustCount
            CicsSummaryDTO topSummary = yearSummarys.get(ajustCount + 1);
            CicsSummaryDTO bottomSummary = yearSummarys.get(yearSummarys.size() - ajustCount);
            //改变前ajustCount名和后ajustCount名数据并保存到数据库
            for (int i = 0; i < yearSummarys.size(); i++) {
                if (i < ajustCount) {
                    yearSummarys.get(i).setRevenueGrowthRateAdjust(topSummary.getRevenueGrowthRate());
                    continue;
                }
                if (i > yearSummarys.size() - ajustCount) {
                    yearSummarys.get(i).setRevenueGrowthRateAdjust(bottomSummary.getRevenueGrowthRate());
                    continue;
                }
                yearSummarys.get(i).setRevenueGrowthRateAdjust(yearSummarys.get(i).getRevenueGrowthRate());
            }
            saveBatch(yearSummarys);
        }

    }


    /**
     * 将Indicator计算出的增长率放入summary,将Indicator和summary 表进行对照，如果表中有则替换 没有则新插入
     */

    public List<CicsSummaryDTO> updateRevenueGrowthRate(List<Integer> cicsIds, List<Map<String, Object>> indicatoGrowthInCicsIndicator) {
        List<CicsSummaryDTO> sumarys = getsummaryByCicsIds(cicsIds);
        List<CicsSummaryDTO> collect = indicatoGrowthInCicsIndicator.stream().map(cicsIndicatorGrowth -> {
            CicsSummaryDTO summaryDTO = convertMapToCicsSummaryDTO(cicsIndicatorGrowth);
            summaryDTO.setRevenueGrowthRate(Convert.toBigDecimal(cicsIndicatorGrowth.get("growth")));
            Optional<CicsSummaryDTO> first = sumarys.stream().
                filter(a -> Objects.equals(a.getYear(), Convert.toStr(cicsIndicatorGrowth.get("year")))
                    && Objects.equals(a.getQuarter(), Convert.toStr(cicsIndicatorGrowth.get("quarter")))
                    && Objects.equals(a.getCicsId(), Convert.toInt(cicsIndicatorGrowth.get("cics_id"))))
                .distinct().findFirst();
            if (first.isPresent()) {
                summaryDTO.setId(first.get().getId());
            }
            return summaryDTO;
        }).distinct().collect(Collectors.toList());
        saveOrUpdateBatch(collect);
        return collect;
    }

    //TODO   企业所属变动导致的原行业 的列表  //不需要从原始数据开始比较  计算行业固定资产和营收时已处理
//    private List<BaseEntityDTO> getChangedCICSByEnterprise(List<Integer> fieldList) {
//        List < List<BaseEntityDTO>> containerList = Lists.newArrayList();
//        return containerList.stream().flatMap(Collection::stream).distinct().collect(Collectors.toList());
//    }
    //查出所有变动行业固定资产和总资产数据及营业规模（二次加权已处理）  使用数据库能力（也可以为其他）left join 已包含 cics_indicator有但是summary 尚未计算的数据
    private List<Map> getChagedCicsSummary(List<Integer> fieldList) {
        List<SysFieldDTO> sysFildDTO = sysFildService.getEntityListByIds(fieldList);
        return cicsSummaryDao.getChagedCicsSummary(sysFildDTO);//
    }

    //计算固定资产占比特定算法----横向计算summarylist 同时使用
    private List<CicsSummaryDTO> setChangedFixedAssetsPercentage(List<Map> summarylist, Integer fixedAssetsId, Integer totalAssetsId) {

        SysFieldDTO fixedAssetsField = sysFildService.getEntity(fixedAssetsId);
        SysFieldDTO totalAssetsField = sysFildService.getEntity(totalAssetsId);


        List<CicsSummaryDTO> changedlist = summarylist.stream().map(map -> {
                CicsSummaryDTO summaryDTO = convertMapToCicsSummaryDTO(map);
                if (Objects.nonNull(map.get(fixedAssetsField.getCode())) && Objects.nonNull(map.get(totalAssetsField.getCode()))) {
                    BigDecimal fixedAssets = Convert.toBigDecimal(map.get(fixedAssetsField.getCode()));
                    BigDecimal totalAssets = Convert.toBigDecimal(map.get(totalAssetsField.getCode()));
                    BigDecimal fixedAssetsPercentage = BigDecimal.ZERO;
                    BigDecimal fixedAssetsPercentageAdjust = BigDecimal.ZERO;
                    if (!totalAssets.equals(BigDecimal.ZERO)) {
                        fixedAssetsPercentage = fixedAssets.divide(totalAssets);
                    }
                    summaryDTO.setFixedAssetsRate(fixedAssetsPercentage);//固定占比
                    fixedAssetsPercentageAdjust = fixedAssetsPercentage;
                    if (fixedAssetsPercentage.compareTo(BigDecimal.ONE) == 1) {
                        fixedAssetsPercentageAdjust = BigDecimal.ONE;
                    }
                    if (fixedAssetsPercentage.compareTo(BigDecimal.ZERO) == -1) {
                        fixedAssetsPercentageAdjust = BigDecimal.ZERO;
                    }
                    summaryDTO.setFixedAssetsRateAdjust(fixedAssetsPercentageAdjust);//调整后
                }
                return summaryDTO;
            }
        ).collect(Collectors.toList());
        //仅更固占比 和调整后固定占比
        saveOrUpdateBatch(changedlist);
        return changedlist;
    }

    /**
     * map 固定字段转换
     *
     * @param map
     * @return
     */
    private CicsSummaryDTO convertMapToCicsSummaryDTO(Map map) {
        CicsSummaryDTO summaryDTO = new CicsSummaryDTO();
        summaryDTO.setId(Convert.toInt(map.get("id")));//如果是null 则保存null
        summaryDTO.setCicsId(Convert.toInt(map.get("cics_id")));
        summaryDTO.setYear(Convert.toStr(map.get("year")));
        summaryDTO.setQuarter(Convert.toStr(map.get("quarter")));
        summaryDTO.setVersion(Convert.toInt(map.get("max_version")));//max_version 所有返回固定字段
        return summaryDTO;
    }


    /**
     * summary_version、max_version  固定字段 其他version
     *
     * @param summarylist
     * @return 根据字段返回需要处理的cicsId
     */
    private List<Map> getChangedCics(List<Map> summarylist, List<SysFieldDTO> filds) {
        //查询出需要处理cicsId
        List<Map> cicsIds = summarylist.stream().filter(map -> {
                Boolean flag = Boolean.FALSE;
                for (SysFieldDTO fild : filds) {
                    if (Objects.nonNull(map) && Objects.nonNull(map.get(fild.getCode() + "_version")) && Objects.nonNull(map.get("summary_version")) && Convert.toInt(map.get(fild.getCode() + "_version")) > Convert.toInt(map.get("summary_version"))) {
                        flag = Boolean.TRUE;
                    }
                }
                return flag;
            }
        ).distinct().collect(Collectors.toList());
        return cicsIds;
    }
    //计算应收增长率

    /**
     * 根据data_clean_cics_indicator表中计算增长率(任何field)
     * Map 中字段为cics_id year quarter  field_id 查询的fieldcode 即value值
     */
    //计算营收规模增长率--纵向(TODO 必须和固占比使用同一个list)
    private List<Map<String, Object>> getIndicatoGrowthInCicsIndicator(List<Integer> cicsIds, Integer fieldId) {//fieldId
        //查询出需要处理cicsId 的所有年度数据按年度和季度
        SysFieldDTO field = sysFildService.getEntity(fieldId);
        List returnList = Lists.newArrayList();
        for (Integer cicsId : cicsIds) {
            List<Map<String, Object>> cicsIndicatorValue = cicsIndicatorDao.getFieldList(field, cicsId, null, null);
            for (int i = 0; i < cicsIndicatorValue.size(); i++) {
                Map<String, Object> currentMap = cicsIndicatorValue.get(i);
                if (i == 0) {
                    returnList.add(currentMap);//首字段没有growth  或者说growth为null
                    continue;
                }
                Map<String, Object> breforeMap = cicsIndicatorValue.get(i - 1);
                if (Objects.nonNull(currentMap.get(field.getCode())) && Objects.nonNull(breforeMap.get(field.getCode()))) {
                    BigDecimal currentValue = Convert.toBigDecimal(currentMap.get(field.getCode()));
                    BigDecimal breforeValue = Convert.toBigDecimal(breforeMap.get(field.getCode()));
                    currentMap.put("growth", currentValue.divide(breforeValue).subtract(BigDecimal.ONE)); //TODO growth后期设置为全局变量
                    returnList.add(currentMap);
                }
            }
        }
        return returnList;
    }
    //获取summary 需要营收改变的cicsid

    /**
     * 查询需要重新计算的行业信息
     *
     * @param cicsIds
     * @return
     */
    private List<CicsSummaryDTO> getsummaryByCicsIds(List<Integer> cicsIds) {//fieldId
        if (CollectionUtil.isEmpty(cicsIds)) {
            return Lists.newArrayList();
        }
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.in("cics_id", cicsIds);
        wrapper.eq("quarter", ConstData.PUBLIC_QUARTER);//只查询q4季度
        return list(wrapper);
    }

    //因为营收和固定资产占比变化会导致全年度全行业重新排名
    //处理三年平均固定资产排名
    private void updateFixedSort() {
        //查询summary表中存在的所有年度
        List<String> allyears = getAllyears();
        log.info("开始追个年度处理每个年度的排名......");
        for (String year : allyears) {
            log.info("查询{}年度数据......", year);
            List<CicsSummaryDTO> summaryByYear = getSummaryByYear(year);
            List<CicsSummaryDTO> hasDataSummary = summaryByYear.stream().filter(a -> Objects.nonNull(a.getFixedAssetsRate3())).sorted(Comparator.comparing(CicsSummaryDTO::getFixedAssetsRate3, Comparator.reverseOrder())).collect(Collectors.toList());
            log.info("{}年度行业总数:{},参与排名{}......", year, summaryByYear.size(), hasDataSummary.size());
            log.info("写入排名......", year, summaryByYear.size(), hasDataSummary.size());
            //中间值计算
            Integer midSort = hasDataSummary.size() / 2 + (hasDataSummary.size() % 2 != 0 ? 1 : 0);
            for (int i = 0; i < hasDataSummary.size(); i++) {
                //从前往后排
                CicsSummaryDTO currentSummary = hasDataSummary.get(i);
                currentSummary.setFixedAssetsSortTotal(hasDataSummary.size());
                Integer sort = i + 1;
                if (i == 0) {
                    currentSummary.setFixedAssetsRate3sort(sort);
                    continue;
                }
                CicsSummaryDTO lastSummary = hasDataSummary.get(i - 1);
                if (Objects.equals(currentSummary.getFixedAssetsRate3(), lastSummary.getFixedAssetsRate3())) {
                    sort = lastSummary.getFixedAssetsRate3sort();
                }
                currentSummary.setFixedAssetsRate3sort(sort);
                log.info("开始进行轻重资产判别....");
                if (sort <= midSort) {
                    currentSummary.setAssetsType(0);//重资产
                } else {
                    currentSummary.setAssetsType(1);//轻资产
                }
                log.info("开始进行轻重标签判别....");
                if (sort <= hasDataSummary.size() * 10 / 100) {
                    currentSummary.setAssetsGroup(0);
                } else if (sort <= hasDataSummary.size() * 20 / 100) {
                    currentSummary.setAssetsGroup(1);
                } else if (sort <= hasDataSummary.size() * 30 / 100) {
                    currentSummary.setAssetsGroup(2);
                } else if (sort <= hasDataSummary.size() * 40 / 100) {
                    currentSummary.setAssetsGroup(3);
                } else if (sort <= hasDataSummary.size() * 50 / 100) {
                    currentSummary.setAssetsGroup(4);
                } else if (sort <= hasDataSummary.size() * 60 / 100) {
                    currentSummary.setAssetsGroup(5);
                } else if (sort <= hasDataSummary.size() * 70 / 100) {
                    currentSummary.setAssetsGroup(6);
                } else if (sort <= hasDataSummary.size() * 80 / 100) {
                    currentSummary.setAssetsGroup(7);
                } else if (sort <= hasDataSummary.size() * 90 / 100) {
                    currentSummary.setAssetsGroup(18);
                } else {
                    currentSummary.setAssetsGroup(9);
                }

            }
            log.info("保存{}年度排名数据......", year);
            saveBatch(hasDataSummary);
        }
    }


    private void updateRevenueGrowthSort() {
        List<String> allyears = getAllyears();
        log.info("开始逐个年度处理年度的排名......");
        for (String year : allyears) {
            log.info("查询{}年度数据......", year);
            List<CicsSummaryDTO> summaryByYear = getSummaryByYear(year);
            List<CicsSummaryDTO> hasDataSummary = summaryByYear.stream().filter(a -> Objects.nonNull(a.getRevenueGrowthRate3())).sorted(Comparator.comparing(CicsSummaryDTO::getRevenueGrowthRate3, Comparator.reverseOrder())).collect(Collectors.toList());
            log.info("{}年度行业总数:{},参与排名{}......", year, summaryByYear.size(), hasDataSummary.size());
            log.info("写入排名......", year, summaryByYear.size(), hasDataSummary.size());
            //中间值计算
            Integer midSort = hasDataSummary.size() / 2 + (hasDataSummary.size() % 2 != 0 ? 1 : 0);
            for (int i = 0; i < hasDataSummary.size(); i++) {
                //从前往后排
                CicsSummaryDTO currentSummary = hasDataSummary.get(i);
                currentSummary.setRevenueGrowthSortTotal(hasDataSummary.size());
                Integer sort = i + 1;
                if (i == 0) {
                    currentSummary.setRevenueGrowthRate3Sort(sort);
                    continue;
                }
                CicsSummaryDTO lastSummary = hasDataSummary.get(i - 1);
                if (Objects.equals(currentSummary.getRevenueGrowthRate3(), lastSummary.getRevenueGrowthRate3())) {
                    sort = lastSummary.getRevenueGrowthRate3Sort();
                }
                currentSummary.setRevenueGrowthRate3Sort(sort);
                log.info("开始进行成长属性判别....");
                if (sort <= midSort) {
                    currentSummary.setAssetsType(0);//高成长
                } else {
                    currentSummary.setAssetsType(1);//低成长
                }
                log.info("开始进行成长属性标签判别....");
                if (sort <= hasDataSummary.size() * 10 / 100) {
                    currentSummary.setAssetsGroup(0);
                } else if (sort <= hasDataSummary.size() * 20 / 100) {
                    currentSummary.setAssetsGroup(1);
                } else if (sort <= hasDataSummary.size() * 30 / 100) {
                    currentSummary.setAssetsGroup(2);
                } else if (sort <= hasDataSummary.size() * 40 / 100) {
                    currentSummary.setAssetsGroup(3);
                } else if (sort <= hasDataSummary.size() * 50 / 100) {
                    currentSummary.setAssetsGroup(4);
                } else if (sort <= hasDataSummary.size() * 60 / 100) {
                    currentSummary.setAssetsGroup(5);
                } else if (sort <= hasDataSummary.size() * 70 / 100) {
                    currentSummary.setAssetsGroup(6);
                } else if (sort <= hasDataSummary.size() * 80 / 100) {
                    currentSummary.setAssetsGroup(7);
                } else if (sort <= hasDataSummary.size() * 90 / 100) {
                    currentSummary.setAssetsGroup(18);
                } else {
                    currentSummary.setAssetsGroup(9);
                }
            }
            log.info("保存{}年度排名数据......", year);
            saveBatch(hasDataSummary);
        }
    }

    //查询summary表中存在的所有年度
    private List<String> getAllyears() {
        log.info("查询数据表中所有涉及到排名的年度......");
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("quarter", ConstData.PUBLIC_QUARTER);
        wrapper.groupBy("year");

        return list(wrapper).stream().map(CicsSummaryDTO::getYear).distinct().collect(Collectors.toList());
    }

    private List<CicsSummaryDTO> getSummaryByYear(String year) {//fieldId
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("year", year);
        wrapper.eq("quarter", ConstData.PUBLIC_QUARTER);//只查询q4季度
        return list(wrapper);
    }

    @Override
    public List<BaseEntityDTO> findByGroupVersion(String quarter) {
        return cicsSummaryDao.findByGroupVersion(quarter);
    }

    @Override
    public void twoCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Integer> fieldIds = enumFieldList.stream().map(a -> a.getLeftFieldId()).collect(Collectors.toList());
        List<BaseEntityDTO> list = cicsSummaryDao.findByFieldYQ(fieldIds);
        Integer fixedAssetsFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.CICS_Q_FIXED_ASSETS.name());
        Integer totalAssetsFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.CICS_Q_TOTAL_ASSETS.name());
        Integer revenueScaleFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.CICS_Q_REVENUE_SCALE.name());
        List<Integer> enterpriseFieldList = fieldList.stream().map(a -> a.getLeftFieldId()).collect(Collectors.toList());
        for (BaseEntityDTO baseEntityDTO : list) {
            executorSummary(baseEntityDTO, fixedAssetsFieldId, totalAssetsFieldId, revenueScaleFieldId, fieldIds,enterpriseFieldList);
        }
    }

    private void executorSummary(BaseEntityDTO baseEntityDTO, Integer fixedAssetsFieldId, Integer totalAssetsFieldId, Integer revenueScaleFieldId, List<Integer> fieldList, List<Integer> enterpriseFieldList) {
//    行业下所属企业大于3的行业列表
        List<BaseEntityDTO> enterpriseCicsList = cleanBizIndicatorService.findByCicsCountEnterprise(baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), enterpriseFieldList);
        List<Integer> enterpriseList = enterpriseCicsList.stream().map(a -> a.getCicsId()).collect(Collectors.toList());
        //        根据多字段读取行业集数据
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).in("field_id", fieldList);
        List<CleanIndicatorDTO> list = cleanIndicatorService.getEntityList(queryWrapper);
//        行业id集合转换
        List<Integer> integerList = list.stream().map(a -> a.getCicsId()).collect(Collectors.toList());
//        读取上一年营收规模行业及数据
        queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", previousYears(baseEntityDTO.getYear(), 1)).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", revenueScaleFieldId);
        List<CleanIndicatorDTO> lastList = cleanIndicatorService.getEntityList(queryWrapper);
//        读取分组下行业集合下前两年同年同季度数据
        QueryWrapper<CicsSummaryDTO> wrapper = new QueryWrapper<>();
        wrapper.in("year", previousYears(baseEntityDTO.getYear())).eq("quarter", baseEntityDTO.getQuarter()).in("cics_id", integerList);
        List<CicsSummaryDTO> cicsSummaryList = cicsSummaryDao.selectList(wrapper);
//        行业map
        Map<Integer, List<CleanIndicatorDTO>> integerListMap = list.stream().collect(Collectors.groupingBy(a -> a.getCicsId()));
//        去年行业 指标值
        Map<Integer, Double> integerDoubleMap = lastList.stream().collect(Collectors.toMap(CleanIndicatorDTO::getCicsId, CleanIndicatorDTO::getIndicatorValue, (k1, k2) -> k2));
//       分组行业指标值
        Map<Integer, List<CicsSummaryDTO>> stringListMap = cicsSummaryList.stream().collect(Collectors.groupingBy(a -> a.getCicsId()));
//        数据list
        List<CicsSummaryDTO> summaryDTOList = Lists.newArrayList();
        QueryWrapper<CicsIndustryLevelDTO> levelDTOQueryWrapper = new QueryWrapper();
        levelDTOQueryWrapper.eq("type", 1).select("id");
        List<CicsIndustryLevelDTO> levelDTOList = cicsIndustryLevelService.getEntityList(levelDTOQueryWrapper);
        List<Integer> levelList = levelDTOList.stream().map(a -> a.getId()).collect(Collectors.toList());
        QueryWrapper<CicsIndustryLevelDTO> levelQueryWrapper = new QueryWrapper();
        long count = cicsIndustryLevelService.getEntityListCount(levelQueryWrapper);

        for (Integer key : integerListMap.keySet()) {
            Double value = null;
            if (integerDoubleMap.containsKey(key)) {
                value = integerDoubleMap.get(key);
            }
            if (!levelList.contains(key) && enterpriseList.contains(key)) {
                executorProportionFixedAssets(key, integerListMap.get(key), stringListMap.get(key), summaryDTOList, fixedAssetsFieldId, totalAssetsFieldId, revenueScaleFieldId, baseEntityDTO, value);
            }
        }
        //TODO 问题修复
        executorProportionRevenueScale(stringListMap, summaryDTOList, revenueScaleFieldId, baseEntityDTO, count);
        List<Integer> fList = list.stream().filter(a -> Objects.equals(a.getFieldId(), revenueScaleFieldId) && Objects.nonNull(a.getIndicatorValue())).map(a -> a.getCicsId()).collect(Collectors.toList());
        threeYearRanking(summaryDTOList, baseEntityDTO, count, fList);
        List<Future> futureList = Lists.newArrayList();
        for (CicsSummaryDTO cicsSummaryDTO : summaryDTOList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> insertToUpdate(cicsSummaryDTO));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }


    /**
     * 固定资产算法
     */
    private void executorProportionFixedAssets(Integer cicsId, List<CleanIndicatorDTO> list, List<CicsSummaryDTO> summaryList, List<CicsSummaryDTO> summaryDTOList, Integer fixedAssetsFieldId, Integer totalAssetsFieldId, Integer revenueScaleFieldId, BaseEntityDTO baseEntityDTO, Double revenueScaleLastValue) {
        Map<Integer, Double> map = list.stream().collect(Collectors.toMap(CleanIndicatorDTO::getFieldId, CleanIndicatorDTO::getIndicatorValue, (k1, k2) -> k2));
        Map<String, CicsSummaryDTO> stringDoubleMap = Maps.newConcurrentMap();
        if (Objects.nonNull(summaryList)) {
            stringDoubleMap = summaryList.stream().collect(Collectors.toMap(a -> a.getYear(), Function.identity(), (k1, k2) -> k2));
        }
        CicsSummaryDTO cicsSummaryDTO = new CicsSummaryDTO();
        BeanUtil.copyProperties(baseEntityDTO, cicsSummaryDTO);
        cicsSummaryDTO.setCicsId(cicsId);
        if (map.containsKey(fixedAssetsFieldId) && map.containsKey(totalAssetsFieldId)) {
            Double fixedAssetsValue = map.get(fixedAssetsFieldId);
            Double totalAssetsValue = map.get(totalAssetsFieldId);
            if (Objects.nonNull(fixedAssetsValue) && Objects.nonNull(totalAssetsValue) && !NumberUtil.equals(totalAssetsValue, 0)) {
                Double value = NumberUtil.div(fixedAssetsValue, totalAssetsValue);//TODO  BigInteger divide by zero????
                cicsSummaryDTO.setFixedAssetsRate(BigDecimal.valueOf(value));
                if (NumberUtil.compare(value, Double.valueOf(0)) == -1) {
                    value = Double.valueOf(0);
                    cicsSummaryDTO.setFixedAssetsRateAdjust(BigDecimal.ZERO);
                } else if (NumberUtil.compare(value, Double.valueOf(1)) == 1) {
                    value = Double.valueOf(1);
                    cicsSummaryDTO.setFixedAssetsRateAdjust(BigDecimal.ONE);
                } else {
                    cicsSummaryDTO.setFixedAssetsRateAdjust(BigDecimal.valueOf(value));
                }
                Double oneValue = Double.valueOf(0);
                Double twoValue = Double.valueOf(0);
                int size = 1;
                if (stringDoubleMap.containsKey(previousYears(baseEntityDTO.getYear(), 1)) && Objects.nonNull(stringDoubleMap.get(previousYears(baseEntityDTO.getYear(), 1)).getFixedAssetsRateAdjust())) {
                    oneValue = stringDoubleMap.get(previousYears(baseEntityDTO.getYear(), 1)).getFixedAssetsRateAdjust().doubleValue();
                    size += 1;
                }
                if (stringDoubleMap.containsKey(previousYears(baseEntityDTO.getYear(), 2)) && Objects.nonNull(stringDoubleMap.get(previousYears(baseEntityDTO.getYear(), 2)).getFixedAssetsRateAdjust())) {
                    twoValue = stringDoubleMap.get(previousYears(baseEntityDTO.getYear(), 2)).getFixedAssetsRateAdjust().doubleValue();
                    size += 1;
                }

                value = NumberUtil.div((oneValue + twoValue + value), size);
                cicsSummaryDTO.setFixedAssetsRate3(BigDecimal.valueOf(value));
            }
        }
        if (map.containsKey(revenueScaleFieldId)) {
            Double revenueScaleValue = map.get(revenueScaleFieldId);
            if (Objects.nonNull(revenueScaleValue) && Objects.nonNull(revenueScaleLastValue) && !Objects.equals(revenueScaleLastValue,Double.valueOf(0))) {
                Double divValue = NumberUtil.div(revenueScaleValue, revenueScaleLastValue);
                Double value = NumberUtil.sub(divValue, Double.valueOf(1));
                cicsSummaryDTO.setRevenueGrowthRate(BigDecimal.valueOf(value));
            }
        }
        summaryDTOList.add(cicsSummaryDTO);
    }

    /**
     * 营收规模占比
     *
     * @param stringListMap
     * @param summaryList
     * @param revenueScaleFieldId
     * @param baseEntityDTO
     */
    private void executorProportionRevenueScale(Map<Integer, List<CicsSummaryDTO>> stringListMap, List<CicsSummaryDTO> summaryList, Integer revenueScaleFieldId, BaseEntityDTO baseEntityDTO, long count) {
        List<CicsSummaryDTO> summaryDTOList = summaryList.stream().filter(a -> Objects.nonNull(a.getRevenueGrowthRate())).collect(Collectors.toList());
        summaryDTOList = summaryDTOList.stream().sorted(Comparator.comparing(CicsSummaryDTO::getRevenueGrowthRate).reversed()).collect(Collectors.toList());

        int size = (int) Math.floor(NumberUtil.mul(Double.valueOf(summaryDTOList.size()), PublicEnum.SUMMARY_PROPORTION));
        int maxSize = summaryDTOList.size() - size;
        if (summaryDTOList.size() > size * 2) {
            AtomicInteger i = new AtomicInteger(0);
            Double minValue = summaryDTOList.get(maxSize - 2).getRevenueGrowthRate().doubleValue();
            Double maxValue = summaryDTOList.get(size).getRevenueGrowthRate().doubleValue();
            summaryDTOList.forEach(a -> {
                Double value = Double.valueOf(0);
                if (i.get() < size) {
                    a.setRevenueGrowthRateAdjust(BigDecimal.valueOf(maxValue));
                    value = maxValue;
                } else if (i.get() > maxSize - 2) {
                    a.setRevenueGrowthRateAdjust(BigDecimal.valueOf(minValue));
                    value = minValue;
                } else {
                    a.setRevenueGrowthRateAdjust(a.getRevenueGrowthRate());
                    value = a.getRevenueGrowthRate().doubleValue();
                }
                Double oneValue = Double.valueOf(0);
                Double twoValue = Double.valueOf(0);
                int groupSize = 1;
                if (stringListMap.containsKey(a.getCicsId())) {
                    List<CicsSummaryDTO> stringSummaryList = stringListMap.get(a.getCicsId()).stream().filter(b -> Objects.nonNull(b.getRevenueGrowthRateAdjust())).collect(Collectors.toList());
                    if (stringSummaryList.size() > 0) {
                        Map<String, BigDecimal> map = stringSummaryList.stream().collect(Collectors.toMap(CicsSummaryDTO::getYear, CicsSummaryDTO::getRevenueGrowthRateAdjust, (k1, k2) -> k2));
                        if (map.containsKey(previousYears(baseEntityDTO.getYear(), 1))) {
                            oneValue = map.get(previousYears(baseEntityDTO.getYear(), 1)).doubleValue();
                            groupSize+=1;
                        }
                        if (map.containsKey(previousYears(baseEntityDTO.getYear(), 2))) {
                            twoValue = map.get(previousYears(baseEntityDTO.getYear(), 2)).doubleValue();
                            groupSize+=1;
                        }
                    }
                }
                value = NumberUtil.div((oneValue + twoValue + value), groupSize);
                a.setRevenueGrowthRate3(BigDecimal.valueOf(value));
                i.addAndGet(1);
            });
        }
        Map<Integer, CicsSummaryDTO> summaryDTOMap = summaryDTOList.stream().collect(Collectors.toMap(a -> a.getCicsId(), Function.identity(), (k1, k2) -> k2));

        summaryList.forEach(a -> {
            if (summaryDTOMap.containsKey(a.getCicsId())) {
                BeanUtil.copyProperties(summaryDTOMap.get(a.getCicsId()), a);
            }
        });


    }

    /**
     * 处理数据
     *
     * @param summaryDTOList
     * @param baseEntityDTO
     * @param count
     */
    private void threeYearRanking(List<CicsSummaryDTO> summaryDTOList, BaseEntityDTO baseEntityDTO, long count, List<Integer> fList) {
        int sort_size = 0;
        int middle_size = 0;
        AtomicInteger summary_size = new AtomicInteger(1);
        List<CicsSummaryDTO> summaryList = summaryDTOList.stream().filter(a -> Objects.nonNull(a.getFixedAssetsRate3())).collect(Collectors.toList());
        if (Objects.nonNull(summaryList) && summaryList.size() > 0) {
            sort_size = (int) Math.floor(NumberUtil.mul(Double.valueOf(summaryDTOList.size()), PublicEnum.SUMMARY_SORT));
            middle_size = (int) Math.ceil(NumberUtil.mul(Double.valueOf(summaryDTOList.size()), PublicEnum.SUMMARY_MIDDLE));
            summaryList = summaryList.stream().sorted(Comparator.comparing(CicsSummaryDTO::getFixedAssetsRate3).reversed()).collect(Collectors.toList());
            AtomicInteger i = new AtomicInteger(0);
            List<CicsSummaryDTO> finalSummaryDTOList = summaryList;
            int finalMiddle_size = middle_size;
            AtomicInteger finalSort_size = new AtomicInteger(sort_size);
            long finalCount = summaryList.size();
            summaryList.forEach(a -> {
                int middleNSize = i.get() + 1;
                if (!Objects.equals(i.get(), 0) && finalSummaryDTOList.get(i.get() - 1).getFixedAssetsRate3().equals(a.getFixedAssetsRate3())) {
                    middleNSize = finalSummaryDTOList.get(i.get() - 1).getFixedAssetsRate3sort();
                    a.setFixedAssetsRate3sort(finalSummaryDTOList.get(i.get() - 1).getFixedAssetsRate3sort());
                } else {
                    a.setFixedAssetsRate3sort(i.get() + 1);
                }
                if (finalMiddle_size >= middleNSize) {
                    a.setAssetsType(PublicEnum.CICS_GROUP_A_G.ASSETS_LEFT.getId());
                    a.setAssetsRemark(PublicEnum.CICS_GROUP_A_G.ASSETS_LEFT.getName());
                } else {
                    a.setAssetsType(PublicEnum.CICS_GROUP_A_G.ASSETS_RIGHT.getId());
                    a.setAssetsRemark(PublicEnum.CICS_GROUP_A_G.ASSETS_RIGHT.getName());
                }
                if (middleNSize > finalSort_size.get() && summary_size.get() < 10) {
                    summary_size.addAndGet(1);
                    finalSort_size.set((int) Math.floor(NumberUtil.mul(finalCount / 10, summary_size.get())));
                }
                a.setAssetsGroup(summary_size.get());


                i.addAndGet(1);
            });
        }
        AtomicInteger gi = new AtomicInteger(0);
        summary_size.set(1);
        List<CicsSummaryDTO> summaryRList = summaryDTOList.stream().filter(a -> Objects.nonNull(a.getRevenueGrowthRate3())).collect(Collectors.toList());
        if (Objects.nonNull(summaryRList)) {
            sort_size = (int) Math.floor(NumberUtil.mul(Double.valueOf(summaryRList.size()), PublicEnum.SUMMARY_SORT));
            middle_size = (int) Math.ceil(NumberUtil.mul(Double.valueOf(summaryRList.size()), PublicEnum.SUMMARY_MIDDLE));
            summaryRList = summaryRList.stream().sorted(Comparator.comparing(CicsSummaryDTO::getRevenueGrowthRate3).reversed()).collect(Collectors.toList());
            List<CicsSummaryDTO> finalSummaryRDTOList = summaryRList;
            int finalMiddle_size1 = middle_size;
            AtomicInteger finalSort_size = new AtomicInteger(sort_size);
            long finalCount1 = summaryRList.size();
            summaryRList.forEach(a -> {
                int middleNSize = gi.get() + 1;
                if (!Objects.equals(gi.get(), 0) && finalSummaryRDTOList.get(gi.get() - 1).getRevenueGrowthRate3().equals(a.getRevenueGrowthRate3())) {
                    middleNSize = finalSummaryRDTOList.get(gi.get() - 1).getRevenueGrowthRate3Sort();
                    a.setRevenueGrowthRate3Sort(finalSummaryRDTOList.get(gi.get() - 1).getRevenueGrowthRate3Sort());
                } else {
                    a.setRevenueGrowthRate3Sort(gi.get() + 1);
                }
                if (finalMiddle_size1 >= middleNSize) {
                    a.setGrowthType(PublicEnum.CICS_GROUP_A_G.GROWTH_LEFT.getId());
                    a.setGrowthRemark(PublicEnum.CICS_GROUP_A_G.GROWTH_LEFT.getName());
                } else {
                    a.setGrowthType(PublicEnum.CICS_GROUP_A_G.GROWTH_RIGHT.getId());
                    a.setGrowthRemark(PublicEnum.CICS_GROUP_A_G.GROWTH_RIGHT.getName());
                }
                if (middleNSize > finalSort_size.get() && summary_size.get() < 10) {
                    summary_size.addAndGet(1);
                    finalSort_size.set((int) Math.floor(NumberUtil.mul(finalCount1 / 10, summary_size.get())));
                }
                a.setGrowthGroup(summary_size.get());

                gi.addAndGet(1);
            });
        }


        Map<Integer, CicsSummaryDTO> summaryDTOMap = summaryList.stream().collect(Collectors.toMap(a -> a.getCicsId(), Function.identity(), (k1, k2) -> k2));
        Map<Integer, CicsSummaryDTO> summaryRDTOMap = summaryRList.stream().collect(Collectors.toMap(a -> a.getCicsId(), Function.identity(), (k1, k2) -> k2));
        summaryDTOList.forEach(a -> {
            if (summaryDTOMap.containsKey(a.getCicsId())) {
                BeanUtil.copyProperties(summaryDTOMap.get(a.getCicsId()), a);
                a.setFixedAssetsSortTotal(summaryDTOMap.size());
            }
            if (summaryRDTOMap.containsKey(a.getCicsId())) {
                CicsSummaryDTO cicsSummaryDTO = summaryRDTOMap.get(a.getCicsId());
                a.setRevenueGrowthRate3Sort(cicsSummaryDTO.getRevenueGrowthRate3Sort());
                a.setGrowthGroup(cicsSummaryDTO.getGrowthGroup());
                a.setGrowthType(cicsSummaryDTO.getGrowthType());
                a.setGrowthRemark(cicsSummaryDTO.getGrowthRemark());
                a.setRevenueGrowthSortTotal(summaryRDTOMap.size());
            } else {
                if (fList.contains(a.getCicsId())) {
                    CicsSummaryDTO cicsSummaryDTO = historySummary(a.getCicsId());
                    if (!BeanUtil.isEmpty(cicsSummaryDTO)) {
                        a.setGrowthType(cicsSummaryDTO.getGrowthType());
                        a.setGrowthRemark(cicsSummaryDTO.getGrowthRemark());
                    }
                }
            }
        });
    }

    private CicsSummaryDTO historySummary(Integer cicsId) {
        QueryWrapper<CicsSummaryDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("cics_id", cicsId).isNotNull("growth_type").orderByDesc("year").orderByDesc("quarter");
        CicsSummaryDTO cicsSummaryDTO = getSingleEntity(queryWrapper);
        return cicsSummaryDTO;
    }

    /**
     * 往年年份，往前推两年
     *
     * @param year
     * @return
     */
    private List<String> previousYears(String year) {
        Integer value = Integer.valueOf(year);
        List<String> list = Lists.newArrayList(String.valueOf(value - 1), String.valueOf(value - 2));
        return list;
    }

    /**
     * 获取之前年份
     *
     * @param year
     * @param i
     * @return
     */
    private String previousYears(String year, int i) {
        Integer value = Integer.valueOf(year);
        return String.valueOf(value - i);
    }

    private void insertToUpdate(CicsSummaryDTO cicsSummaryDTO) {
        QueryWrapper<CicsSummaryDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", cicsSummaryDTO.getYear()).eq("quarter", cicsSummaryDTO.getQuarter()).eq("cics_id", cicsSummaryDTO.getCicsId());
        CicsSummaryDTO summaryDTO = this.getSingleEntity(queryWrapper);
        if (BeanUtil.isEmpty(summaryDTO)) {
            cicsSummaryDTO.setId(null);
            cicsSummaryDTO.setCreateTime(new Date());
        } else {
            cicsSummaryDTO.setId(summaryDTO.getId());
            cicsSummaryDTO.setUpdateTime(new Date());
        }
        this.createOrUpdateEntity(cicsSummaryDTO);
    }
}
