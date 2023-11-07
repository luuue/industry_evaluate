package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.NumberUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EnterpriseCompareDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizSummaryDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseCompareDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSimilarDTO;
import com.chilunyc.process.service.enterprise.BizSummaryService;
import com.chilunyc.process.service.enterprise.EnterpriseCompareService;
import com.chilunyc.process.service.enterprise.EnterpriseService;
import com.chilunyc.process.service.industry.CicsSimilarService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service("enterpriseCompareServiceImpl")
public class EnterpriseCompareServiceImpl extends BaseServiceImpl<EnterpriseCompareDao, EnterpriseCompareDTO> implements EnterpriseCompareService {
    @Autowired
    EnterpriseCompareDao enterpriseCompareDao;
    @Autowired
    BizSummaryService bizSummaryService;
    @Autowired
    CicsSimilarService cicsSimilarService;
    @Autowired
    private EnterpriseService enterpriseService;

    /**
     * 计算企业相似度
     *
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void similarityCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //获取所有业务数据有变化的企业及年度和最大版本号
        List<Map<String, Object>> compareEnterpriseData = enterpriseCompareDao.getCompareEnterpriseData();

        //1.目标企业业务变更（改变的企业及年度）
        for (Map<String, Object> map : compareEnterpriseData) {
            Integer enterpriseId = Convert.toInt(map.get("enterprise_id"));
            Integer year = Convert.toInt(map.get("year"));
            Integer version = Convert.toInt(map.get("version"));

            //所有现可对比的企业都要重新计算
            calculation(enterpriseId, year, version);
        }
    }

    @Override
    public void similarityTwoCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<BaseEntityDTO> yearList = bizSummaryService.findByYearList("2018");
        List<CicsSimilarDTO> similarList = cicsSimilarService.getEntityList(new QueryWrapper());
        Map<Integer, List<CicsSimilarDTO>> similarMap = similarList.stream().collect(Collectors.groupingBy(a -> a.getCicsId()));
        QueryWrapper<EnterpriseDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("enterprise_status", 0);
        List<EnterpriseDTO> list = enterpriseService.getEntityList(queryWrapper);
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : yearList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> similarityTwoCalculationInsert(baseEntityDTO, similarMap, list));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }

    private void similarityTwoCalculationInsert(BaseEntityDTO baseEntityDTO, Map<Integer, List<CicsSimilarDTO>> similarMap, List<EnterpriseDTO> list) {
        Integer limit=30;
        Map<Integer, List<BizSummaryDTO>> map = Maps.newConcurrentMap();
        Map<String, EnterpriseCompareDTO> compareDTOMap = Maps.newConcurrentMap();
        Map<String, EnterpriseCompareDTO> similarCompareDTOMap = Maps.newConcurrentMap();
        for (EnterpriseDTO enterpriseDTO : list) {
            if (!map.containsKey(enterpriseDTO.getId())) {
                //根据20230307 与张听会议 计算相似度时 将占比为负值过滤掉
                List<BizSummaryDTO> summaryDTOList = bizSummaryService.findByEnterpriseYearCics4(baseEntityDTO.getYear(), enterpriseDTO.getId());
                map.put(enterpriseDTO.getId(), summaryDTOList);
            }
            List<BizSummaryDTO> leftSummaryList = map.get(enterpriseDTO.getId());
            if (leftSummaryList.size() > 0) {
                List<BizSummaryDTO> similarSummaryList = leftSummaryList.stream().filter(a -> similarMap.containsKey(a.getCicsId())).collect(Collectors.toList());
                int maxVersion = leftSummaryList.stream().max(Comparator.comparing(BizSummaryDTO::getVersion)).map(a -> a.getVersion()).get();
                for (EnterpriseDTO rightDTO : list) {
                    if (!Objects.equals(enterpriseDTO.getId(), rightDTO.getId()) && !compareDTOMap.containsKey(enterpriseDTO.getId() + "-" + rightDTO.getId())) {
                        if (!map.containsKey(rightDTO.getId())) {
                            List<BizSummaryDTO> summaryDTOList = bizSummaryService.findByEnterpriseYearCics4(baseEntityDTO.getYear(), rightDTO.getId());
                            map.put(rightDTO.getId(), summaryDTOList);
                        }

                        List<BizSummaryDTO> rightSummaryList = map.get(rightDTO.getId());
                        Map<Integer, BizSummaryDTO> rightMap = rightSummaryList.stream().collect(Collectors.toMap(a -> a.getCicsId(), Function.identity(), (k1, k2) -> k2));
                        List<BizSummaryDTO> summaryList = leftSummaryList.stream().filter(a -> rightMap.containsKey(a.getCicsId())).collect(Collectors.toList());
                        List<Integer> summaryIntLst = summaryList.stream().map(a -> a.getCicsId()).collect(Collectors.toList());
                        Double leftValue = Double.valueOf(0);
                        Double rightValue = Double.valueOf(0);
                        for (BizSummaryDTO summaryDTO : summaryList) {
                            if (rightMap.containsKey(summaryDTO.getCicsId())) {
                                BizSummaryDTO rightSummaryDTO = rightMap.get(summaryDTO.getCicsId());
                                if (Objects.nonNull(summaryDTO.getRevenueProportion()) && Objects.nonNull(rightSummaryDTO.getRevenueProportion())) {
                                    Double v1 = Double.valueOf(0);
                                    if (NumberUtil.compare(summaryDTO.getRevenueProportion(), rightSummaryDTO.getRevenueProportion()) == -1) {
                                        if (NumberUtil.compare(rightSummaryDTO.getRevenueProportion(), Double.valueOf(0)) != 0) {
                                            v1 = NumberUtil.div(summaryDTO.getRevenueProportion(), rightSummaryDTO.getRevenueProportion());
                                        } else {
                                            v1 = Double.valueOf(0);
                                        }
                                    } else {
                                        if (NumberUtil.compare(summaryDTO.getRevenueProportion(), Double.valueOf(0)) != 0) {
                                            v1 = NumberUtil.div(rightSummaryDTO.getRevenueProportion(), summaryDTO.getRevenueProportion());
                                        } else {
                                            v1 = Double.valueOf(0);
                                        }
                                    }
                                    leftValue += v1 * rightSummaryDTO.getRevenueProportion() * 100;
                                    rightValue += v1 * summaryDTO.getRevenueProportion() * 100;
                                }
                            }
                        }
                        EnterpriseCompareDTO enterpriseCompareDTO = new EnterpriseCompareDTO();
                        enterpriseCompareDTO.setVersion(maxVersion);
                        enterpriseCompareDTO.setIndicatorValue(NumberUtil.round(leftValue, 2));
                        enterpriseCompareDTO.setEnterpriseId(enterpriseDTO.getId());
                        enterpriseCompareDTO.setComparableEnterpriseId(rightDTO.getId());
                        if (NumberUtil.compare(leftValue, limit) == 1) {
                            compareDTOMap.put(enterpriseDTO.getId() + "-" + rightDTO.getId(), enterpriseCompareDTO);
                        }
                        EnterpriseCompareDTO rightEnterpriseCompareDTO = new EnterpriseCompareDTO();
                        rightEnterpriseCompareDTO.setVersion(maxVersion);
                        rightEnterpriseCompareDTO.setIndicatorValue(NumberUtil.round(rightValue, 2));
                        rightEnterpriseCompareDTO.setComparableEnterpriseId(enterpriseDTO.getId());
                        rightEnterpriseCompareDTO.setEnterpriseId(rightDTO.getId());
                        if (NumberUtil.compare(rightValue, limit) == 1) {
                            compareDTOMap.put(rightDTO.getId() + "-" + enterpriseDTO.getId(), rightEnterpriseCompareDTO);
                        }
                        if (!similarCompareDTOMap.containsKey(enterpriseDTO.getId() + "-" + rightDTO.getId())) {
                            Double similarLeftValue = Double.valueOf(0);
                            Double similarRightValue = Double.valueOf(0);
                            for (BizSummaryDTO summaryDTO : similarSummaryList) {
                                if (similarMap.containsKey(summaryDTO.getCicsId())) {
                                    List<CicsSimilarDTO> similarDTOS = similarMap.get(summaryDTO.getCicsId());
                                    for (CicsSimilarDTO cicsSimilarDTO : similarDTOS) {
                                        if (!Objects.equals(cicsSimilarDTO.getCicsId(), cicsSimilarDTO.getCicsSimilarId()) && rightMap.containsKey(cicsSimilarDTO.getCicsSimilarId())) {
                                            BizSummaryDTO rightSummaryDTO = rightMap.get(cicsSimilarDTO.getCicsSimilarId());
                                            if (Objects.nonNull(summaryDTO.getRevenueProportion()) && Objects.nonNull(rightSummaryDTO.getRevenueProportion())) {
                                                Double v1 = Double.valueOf(0);
                                                if (NumberUtil.compare(summaryDTO.getRevenueProportion(), rightSummaryDTO.getRevenueProportion()) == -1) {
                                                    if (NumberUtil.compare(rightSummaryDTO.getRevenueProportion(), Double.valueOf(0)) != 0) {
                                                        v1 = NumberUtil.div(summaryDTO.getRevenueProportion(), rightSummaryDTO.getRevenueProportion());
                                                    } else {
                                                        v1 = Double.valueOf(0);
                                                    }
                                                } else {
                                                    if (NumberUtil.compare(summaryDTO.getRevenueProportion(), Double.valueOf(0)) != 0) {
                                                        v1 = NumberUtil.div(rightSummaryDTO.getRevenueProportion(), summaryDTO.getRevenueProportion());
                                                    } else {
                                                        v1 = Double.valueOf(0);
                                                    }
                                                }
                                                similarLeftValue += v1 * rightSummaryDTO.getRevenueProportion() * 100;
                                                similarRightValue += v1 * summaryDTO.getRevenueProportion() * 100;
                                            }

                                        }
                                    }
                                }
                            }
                            EnterpriseCompareDTO leftEnterpriseCompareDTO = new EnterpriseCompareDTO();
                            leftEnterpriseCompareDTO.setVersion(maxVersion);
                            leftEnterpriseCompareDTO.setIndicatorValue(NumberUtil.round(leftValue + similarLeftValue, 2));
                            leftEnterpriseCompareDTO.setEnterpriseId(enterpriseDTO.getId());
                            leftEnterpriseCompareDTO.setComparableEnterpriseId(rightDTO.getId());
                            //NumberUtil.compare(similarLeftValue, 0) == 1 &&
                            if ( NumberUtil.compare(similarLeftValue + leftValue, limit) == 1) {
                                similarCompareDTOMap.put(enterpriseDTO.getId() + "-" + rightDTO.getId(), leftEnterpriseCompareDTO);
                            }
                            EnterpriseCompareDTO rightBEnterpriseCompareDTO = new EnterpriseCompareDTO();
                            rightBEnterpriseCompareDTO.setVersion(maxVersion);
                            rightBEnterpriseCompareDTO.setIndicatorValue(NumberUtil.round(rightValue + similarRightValue, 2));
                            rightBEnterpriseCompareDTO.setComparableEnterpriseId(enterpriseDTO.getId());
                            rightBEnterpriseCompareDTO.setEnterpriseId(rightDTO.getId());
                            //NumberUtil.compare(similarRightValue, 0) == 1 &&
                            if ( NumberUtil.compare(similarRightValue + rightValue, limit) == 1) {
                                similarCompareDTOMap.put(rightDTO.getId() + "-" + enterpriseDTO.getId(), rightBEnterpriseCompareDTO);
                            }
                        }

                    }
                }
            }
//            System.out.println("compareDTOMap = " + compareDTOMap + ", similarCompareDTOMap = " + similarCompareDTOMap );
//            if(compareDTOMap.size()>similarCompareDTOMap .size()){
//                System.out.println("baseEntityDTO = " + baseEntityDTO + ", similarMap = " + similarMap + ", list = " + list);
//            }


        }
        QueryWrapper<EnterpriseCompareDTO> deleteWrapper = new QueryWrapper<>();
        deleteWrapper.eq("year", baseEntityDTO.getYear());
        this.deleteEntities(deleteWrapper);
        for (EnterpriseCompareDTO enterpriseCompareDTO : compareDTOMap.values()) {
//            if(!similarCompareDTOMap.containsKey(enterpriseCompareDTO.getEnterpriseId()+"-"+enterpriseCompareDTO.getComparableEnterpriseId()))
//            {
                enterpriseCompareDTO.setId(null);
                enterpriseCompareDTO.setYear(Integer.parseInt(baseEntityDTO.getYear()));
                enterpriseCompareDTO.setIsSimilar(0);
                enterpriseCompareDTO.setCreateTime(new Date());
                this.createEntity(enterpriseCompareDTO);
//            }
        }
        for (EnterpriseCompareDTO enterpriseCompareDTO : similarCompareDTOMap.values()) {
            enterpriseCompareDTO.setId(null);
            enterpriseCompareDTO.setYear(Integer.parseInt(baseEntityDTO.getYear()));
            enterpriseCompareDTO.setIsSimilar(1);
            enterpriseCompareDTO.setCreateTime(new Date());
            this.createEntity(enterpriseCompareDTO);
        }
    }

    private void calculation(Integer enterpriseId, Integer year, Integer version) {
        //所有原可对比的企业都要重新计算
        List<Integer> list = Lists.newArrayList();
        List<Integer> historyCompareEnterpriseId = getHistoryCompareEnterprise(enterpriseId, year, 0);

        list.addAll(historyCompareEnterpriseId);
        //不包含相似行业
        calculationNotcontainsSimilar(enterpriseId, year, version, list);
        calculationContainsSimilar(enterpriseId, year, version, list);

    }

    private void calculationContainsSimilar(Integer enterpriseId, Integer year, Integer version, List<Integer> list) {
        //所有原可对比的企业都要重新计算
        List<BizSummaryDTO> nowCompareEnterpriseNotContainSimilarCics = getNowCompareEnterprise(enterpriseId, year, 1);
        List<Integer> listNewContain = nowCompareEnterpriseNotContainSimilarCics.stream().map(o -> o.getEnterpriseId()).collect(Collectors.toList());
        list.addAll(listNewContain);
        List<Integer> finalList = list.stream().distinct().collect(Collectors.toList());

        //目标企业 业务信息
        List<BizSummaryDTO> goalBizSummary = bizSummaryService.getCompareBizSummary(enterpriseId, year);
        for (Integer goalEnterpriseId : finalList) {
            // 获取相关企业并计算
            List<EnterpriseCompareDTO> result = Lists.newArrayList();
            List<BizSummaryDTO> nowCompareEnterprise = getNowCompareEnterprise(goalEnterpriseId, year, 1);
            List<Integer> compareIds = nowCompareEnterprise.stream().map(o -> o.getEnterpriseId()).distinct().collect(Collectors.toList());
            for (Integer compareId : compareIds) {
                List<BizSummaryDTO> compareBiz = bizSummaryService.getCompareBizSummary(compareId, year);
                EnterpriseCompareDTO enterpriseCompareDTO = new EnterpriseCompareDTO();
                enterpriseCompareDTO.setEnterpriseId(goalEnterpriseId);
                enterpriseCompareDTO.setYear(year);
                enterpriseCompareDTO.setComparableEnterpriseId(compareId);
                enterpriseCompareDTO.setVersion(version);
                enterpriseCompareDTO.setIsDelete(0);
                enterpriseCompareDTO.setIsSimilar(1);
                enterpriseCompareDTO.setIndicatorValue(BigDecimal.ZERO);
                enterpriseCompareDTO.setId(null);
                goalBizSummary.stream().forEach(o -> {
                    //查找相似行业
                    List<Integer> similarCicsIdByCicsId = cicsSimilarService.getSimilarCicsIdByCicsId(o.getCicsId());


                    compareBiz.stream().forEach(compare -> {
                        if (Objects.equals(o.getCicsId(), compare.getCicsId()) || similarCicsIdByCicsId.contains(compare.getCicsId())) {
                            if (Objects.nonNull(o.getRevenueProportion()) && Objects.nonNull(compare.getRevenueProportion())) {
                                Double revenueProportion = o.getRevenueProportion() / compare.getRevenueProportion();
                                if (revenueProportion > 1) {
                                    revenueProportion = 1 / revenueProportion;
                                }
                                if (Objects.nonNull(enterpriseCompareDTO.getIndicatorValue()) && Objects.nonNull(revenueProportion) && Objects.nonNull(compare.getRevenueProportion())) {
                                    BigDecimal sum = enterpriseCompareDTO.getIndicatorValue().add(BigDecimal.valueOf(revenueProportion * compare.getRevenueProportion()));
                                    enterpriseCompareDTO.setIndicatorValue(sum);
                                    result.add(enterpriseCompareDTO);
                                }
                            }
                        }
                    });
                });
            }
            dealResult(goalEnterpriseId, year, 1, result);
        }
    }

    private void calculationNotcontainsSimilar(Integer enterpriseId, Integer year, Integer version, List<Integer> list) {
        //所有原可对比的企业都要重新计算
        List<BizSummaryDTO> nowCompareEnterpriseNotContainSimilarCics = getNowCompareEnterprise(enterpriseId, year, 0);
        List<Integer> listNewNotcontain = nowCompareEnterpriseNotContainSimilarCics.stream().map(o -> o.getEnterpriseId()).collect(Collectors.toList());
        list.addAll(listNewNotcontain);
        List<Integer> finalList = list.stream().distinct().collect(Collectors.toList());

        //目标企业 业务信息
        List<BizSummaryDTO> goalBizSummary = bizSummaryService.getCompareBizSummary(enterpriseId, year);
        List<EnterpriseCompareDTO> result = Lists.newArrayList();
        for (Integer goalEnterpriseId : finalList) {
            // 获取相关企业并计算
            List<BizSummaryDTO> nowCompareEnterprise = getNowCompareEnterprise(goalEnterpriseId, year, 0);
            List<Integer> compareIds = nowCompareEnterprise.stream().map(o -> o.getEnterpriseId()).distinct().collect(Collectors.toList());
            for (Integer compareId : compareIds) {
                List<BizSummaryDTO> compareBiz = bizSummaryService.getCompareBizSummary(compareId, year);
                EnterpriseCompareDTO enterpriseCompareDTO = new EnterpriseCompareDTO();
                enterpriseCompareDTO.setEnterpriseId(goalEnterpriseId);
                enterpriseCompareDTO.setYear(year);
                enterpriseCompareDTO.setComparableEnterpriseId(compareId);
                enterpriseCompareDTO.setVersion(version);
                enterpriseCompareDTO.setIsDelete(0);
                enterpriseCompareDTO.setIsSimilar(0);
                enterpriseCompareDTO.setIndicatorValue(BigDecimal.ZERO);
                enterpriseCompareDTO.setId(null);
                goalBizSummary.stream().forEach(o -> {
                    compareBiz.stream().forEach(compare -> {
                        if (Objects.equals(o.getCicsId(), compare.getCicsId())) {
                            if (Objects.nonNull(o.getRevenueProportion()) && Objects.nonNull(compare.getRevenueProportion())) {
                                Double revenueProportion = o.getRevenueProportion() / compare.getRevenueProportion();
                                if (revenueProportion > 1) {
                                    revenueProportion = 1 / revenueProportion;
                                }
                                if (Objects.nonNull(enterpriseCompareDTO.getIndicatorValue()) && Objects.nonNull(revenueProportion) && Objects.nonNull(compare.getRevenueProportion())) {
                                    BigDecimal sum = enterpriseCompareDTO.getIndicatorValue().add(BigDecimal.valueOf(revenueProportion * compare.getRevenueProportion()));
                                    enterpriseCompareDTO.setIndicatorValue(sum);
                                    result.add(enterpriseCompareDTO);
                                }
                            }
                        }
                    });
                });

            }
            dealResult(goalEnterpriseId, year, 0, result);
        }
    }

    /**
     *
     */
    public void dealResult(Integer enterpriseId, Integer year, Integer isSimilar, List<EnterpriseCompareDTO> result) {
        deleteCompareDTO(enterpriseId, year, isSimilar);
        //存入数据库
        for (EnterpriseCompareDTO compareDTO : result) {
            compareDTO.setId(null);
            this.createEntity(compareDTO);
        }
    }

    /**
     * 获取某个企业的历史对标企业(历史即改变前对标企业)
     */
    public List<Integer> getHistoryCompareEnterprise(Integer enterpriseId, Integer year, Integer isSimilar) {
        QueryWrapper<EnterpriseCompareDTO> queryWrapper = new QueryWrapper();
        queryWrapper.eq("enterprise_id", enterpriseId);
        queryWrapper.eq("year", year);
        queryWrapper.eq("is_similar", isSimilar);
        return list(queryWrapper).stream().map(o -> o.getComparableEnterpriseId()).collect(Collectors.toList());
    }

    /**
     * 获取某个企业的对标企业(现有对标企业对标企业)
     * isSimilar 是否包含相似行业 isSimilar=1 包含  isSimilar=0不包含
     */
    public List<BizSummaryDTO> getNowCompareEnterprise(Integer enterpriseId, Integer year, Integer isSimilar) {
        List<BizSummaryDTO> compareBizSummary = bizSummaryService.getCompareBizSummary(enterpriseId, year);
        List<Integer> cicsIds = compareBizSummary.stream().map(o -> o.getCicsId()).collect(Collectors.toList());
        if (isSimilar == 1) {
            //查找相似行业
            List<Integer> similarCicsIds = cicsSimilarService.getSimilarCicsIdsByCicsIds(cicsIds);
            cicsIds.addAll(similarCicsIds);
        }
        cicsIds = cicsIds.stream().distinct().collect(Collectors.toList());
        return bizSummaryService.getCompareBizSummaryByCicsId(cicsIds, year);
    }


    public Boolean deleteCompareDTO(Integer enterpriseId, Integer year, Integer isSimilar) {
        QueryWrapper<EnterpriseCompareDTO> queryWrapper = new QueryWrapper();
        queryWrapper.eq("enterprise_id", enterpriseId);
        queryWrapper.eq("year", year);
        queryWrapper.eq("is_similar", isSimilar);
        return deleteEntities(queryWrapper);
    }
}
