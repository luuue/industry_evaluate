package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsSortDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.AscriptionDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizDefectDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSortDTO;
import com.chilunyc.process.service.enterprise.AscriptionService;
import com.chilunyc.process.service.enterprise.BizIndicatorService;
import com.chilunyc.process.service.enterprise.CleanBizDefectService;
import com.chilunyc.process.service.industry.CicsSortService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("cicsSortImpl")
public class CicsSortImpl extends BaseServiceImpl<CicsSortDao, CicsSortDTO> implements CicsSortService {
    @Autowired
    private CicsSortDao cicsSortDao;
    @Autowired
    private BizIndicatorService bizIndicatorService;
    @Autowired
    private CleanBizDefectService cleanBizDefectService;
    @Autowired
    private AscriptionService ascriptionService;


    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Integer> fieldIds = fieldList.stream().map(a -> a.getLeftFieldId()).collect(Collectors.toList());
        List<Future> futureList = Lists.newArrayList();
        for (Integer field : fieldIds) {
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                findByCicsYQ(field)
            );
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    private void findByCicsYQ(Integer field) {
        //            按照行业，年，季度维度变化原始数据
        List<BaseEntityDTO> baseList = baseAllList(field);
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : baseList) {
//            ExecutorBuilderUtil.incrementAndGet(30,true);
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                enByCicsSort(baseEntityDTO, field));
            futureList.add(future);
            Future f1 = ExecutorBuilderUtil.pool.submit(() -> defectInsert(baseEntityDTO, field));
            futureList.add(f1);
        }
        FutureGetUtil.futureGet(futureList);
    }


    private List<BaseEntityDTO> baseAllList(Integer fieldId) {
        List<Future> futureList = Lists.newArrayList();
        Map<String, String> map = bizIndicatorService.findByMaxMinYearMQ(fieldId);
        List<BaseEntityDTO> list = Lists.newArrayList();
        if (Objects.nonNull(map)) {
            List<String> yearList = inspectYear(map.get("minYear"), map.get("maxYear"));
            for (String yearMQ : yearList) {
                Future future = ExecutorBuilderUtil.pool.submit(() ->
                    baseAllListByYQ(fieldId, yearMQ));
                futureList.add(future);
            }
            futureList.forEach(
                future -> {
                    try {
                        Object o = future.get();
                        List<BaseEntityDTO> baseEntityDTOS = Convert.toList(BaseEntityDTO.class, o);
                        if (CollectionUtil.isNotEmpty(baseEntityDTOS)) {
                            list.addAll(baseEntityDTOS);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            );
        }
        return list;
    }

    private List<BaseEntityDTO> baseAllListByYQ(Integer fieldId, String yearMQ) {
        List<BaseEntityDTO> list = Lists.newArrayList();
        String year = StrUtil.sub(yearMQ, 0, 4);
        String quarter = StrUtil.sub(yearMQ, 4, 6);
        List<AscriptionDTO> ascriptions = ascriptionService.findByYearAndQuarter(year, quarter);// 企业 行业1 v 多
        List<BizIndicatorDTO> bizIndicators = bizIndicatorService.findEnterpriseMaxVersionByYearAndQuarterAndFiled(fieldId, year, quarter); //企业版本1v1

        Map<Integer, List<BizIndicatorDTO>> bizIndicatorMap = bizIndicators.stream().collect(Collectors.groupingBy(BizIndicatorDTO::getEnterpriseId));

        List<CicsSortDTO> cicsSorts = cicsSortDao.findCicsMaxVersionByYearAndQuarterAndField(fieldId, year, quarter);
        Map<Integer, List<CicsSortDTO>> cicsSortMap = cicsSorts.stream().collect(Collectors.groupingBy(CicsSortDTO::getCicsId));
//                List<BaseEntityDTO> baseList = cicsSortDao.findByCicsYQ(fieldId, year, quarter);// 超时错误
//                list.addAll(baseList);
        Map<Integer, Integer> cicsIdVersionMap = new HashMap();
        Map<Integer, AscriptionDTO> ascriptionVersionMap = new HashMap<>();
        if (CollectionUtil.isNotEmpty(ascriptions)) {
            ascriptions.stream().forEach(ascription -> {
                List<BizIndicatorDTO> bizIndicatorDTOS = Optional.ofNullable(bizIndicatorMap.get(ascription.getEnterpriseId())).orElse(new ArrayList<>());
                Optional<BizIndicatorDTO> bizVersion = bizIndicatorDTOS.stream().max(Comparator.comparing(BizIndicatorDTO::getVersion));

                Integer version = bizVersion.isPresent() ? bizVersion.get().getVersion() : 0;
                version = ascription.getVersion() >= version ? ascription.getVersion() : version;
                ascription.setVersion(version);
                Integer cicsVersion = cicsIdVersionMap.get(ascription.getCicsId());
                if (Objects.isNull(cicsVersion) || cicsVersion < version) {
                    cicsIdVersionMap.put(ascription.getCicsId(), version);
                    ascriptionVersionMap.put(ascription.getCicsId(), ascription);
                }
            });

            ascriptionVersionMap.forEach((key, ascription) -> {
                List<CicsSortDTO> cicsSortDTOS = Optional.ofNullable(cicsSortMap.get(ascription.getCicsId())).orElse(new ArrayList<>());
                Optional<CicsSortDTO> cicsSortVersion = cicsSortDTOS.stream().max(Comparator.comparing(CicsSortDTO::getVersion));
                Integer sortVersion = cicsSortVersion.isPresent() ? cicsSortVersion.get().getVersion() : 0;
                //年度 季度 fieldId cicsId 唯一
                if (ascription.getVersion() > sortVersion) {
                    BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
                    baseEntityDTO.setYear(ascription.getYear());
                    baseEntityDTO.setQuarter(ascription.getQuarter());
                    baseEntityDTO.setCicsId(ascription.getCicsId());
                    baseEntityDTO.setVersion(ascription.getVersion());
                    list.add(baseEntityDTO);
                }
            });
        }
        return list;
    }

    private void defectInsert(BaseEntityDTO baseEntityDTO, Integer fieldId) {
        long count = bizIndicatorService.findByEnterpriseBizListCount(fieldId, baseEntityDTO.getYear(), baseEntityDTO.getQuarter());
        long fCount = bizIndicatorService.findByEnterpriseListCount(fieldId, baseEntityDTO.getYear(), baseEntityDTO.getQuarter());
        Double v1 = Double.valueOf(0);
        if (NumberUtil.compare(fCount, 0) == 1) {
            v1 = NumberUtil.div(fCount - count, fCount);
        }
        QueryWrapper<CleanBizDefectDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId);
        CleanBizDefectDTO cleanBizDefectDTO = cleanBizDefectService.getSingleEntity(wrapper);
        CleanBizDefectDTO defectDTO = new CleanBizDefectDTO();
        defectDTO.setFieldId(fieldId);
        defectDTO.setYear(baseEntityDTO.getYear());
        defectDTO.setQuarter(baseEntityDTO.getQuarter());
        defectDTO.setMissingRate(v1);
        defectDTO.setVersion(10000);
        defectDTO.setUpdateTime(new Date());
        if (BeanUtil.isEmpty(cleanBizDefectDTO)) {
            defectDTO.setCreateTime(new Date());
            cleanBizDefectService.createEntity(defectDTO);
        } else {
            defectDTO.setId(cleanBizDefectDTO.getId());
            cleanBizDefectService.updateEntity(defectDTO);
        }
    }

    @Override
    public List<BaseEntityDTO> findByCicsCleanYQ(BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO) {
        return cicsSortDao.findByCicsCleanYQ(baseFieldDTO, baseEntityDTO);
    }

    @Override
    public List<CicsSortDTO> findByCicsCleanLastYear(Integer fieldId, String year, String quarter, String lastYear, String lastQuarter, Integer cicsId) {
        return cicsSortDao.findByCicsCleanLastYear(fieldId, year, quarter, lastYear, lastQuarter, cicsId);
    }

    @Override
    public Map<String, String> findByCicsCLeanMinYear(Integer fieldId) {
        return cicsSortDao.findByCicsCLeanMinYear(fieldId);
    }

    /**
     * 按照行业，年，季度读取企业信息，并排序，先删除原有数据在写入数据库
     *
     * @param baseEntityDTO
     */
    private void enByCicsSort(BaseEntityDTO baseEntityDTO, Integer fieldId) {
        BizIndicatorDTO bizIndicatorDTO = new BizIndicatorDTO();
        BeanUtil.copyProperties(baseEntityDTO, bizIndicatorDTO);
        bizIndicatorDTO.setFieldId(fieldId);
        List<BizIndicatorDTO> list = bizIndicatorService.findByCicsAll(bizIndicatorDTO); // 超时错误
//            根据指标值进行排序
        List<BizIndicatorDTO> leftList = list.stream().filter(a -> ObjectUtil.isNotEmpty(a.getIndicatorValue())).collect(Collectors.toList());
        leftList = leftList.stream().sorted(Comparator.comparingDouble(BizIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());

        List<BizIndicatorDTO> rightList = list.stream().filter(a -> ObjectUtil.isEmpty(a.getIndicatorValue())).collect(Collectors.toList());
        //获取指标值不为空的数量
        long count = list.stream().filter(a -> ObjectUtil.isNotEmpty(a.getIndicatorValue())).count();
//      删除根据年季度行业字段删除原有数据
        QueryWrapper<CicsSortDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("cics_id", baseEntityDTO.getCicsId()).eq("field_id", fieldId);
        cicsSortDao.delete(queryWrapper);
//      循环插入
        List<CicsSortDTO> cicsSortDTOList = Lists.newArrayList();
        for (int i = 1; i <= leftList.size(); i++) {
            BizIndicatorDTO biz = leftList.get(i - 1);
            CicsSortDTO cicsSortDTO = new CicsSortDTO();
            BeanUtil.copyProperties(biz, cicsSortDTO);
            cicsSortDTO.setCicsId(bizIndicatorDTO.getCicsId());
            cicsSortDTO.setFieldId(fieldId);
//          如果指标值没有则无排名
            cicsSortDTO.setId(null);
            if (ObjectUtil.isNotEmpty(biz.getIndicatorValue())) {
                cicsSortDTO.setIndicatorSort(i);

                double desirability = (double) i / count;
                cicsSortDTO.setDesirability(desirability);
            }
            cicsSortDTO.setCreateTime(new Date());
            cicsSortDTO.setUpdateTime(new Date());
            cicsSortDTOList.add(cicsSortDTO);
        }
        for (BizIndicatorDTO indicatorDTO : rightList) {
            CicsSortDTO cicsSortDTO = new CicsSortDTO();
            BeanUtil.copyProperties(indicatorDTO, cicsSortDTO);
            cicsSortDTO.setCicsId(bizIndicatorDTO.getCicsId());
            cicsSortDTO.setFieldId(fieldId);
            cicsSortDTO.setCreateTime(new Date());
            cicsSortDTO.setUpdateTime(new Date());
            cicsSortDTOList.add(cicsSortDTO);
        }
        this.createEntities(cicsSortDTOList);
//        for (CicsSortDTO cicsSortDTO : cicsSortDTOList) {//唯一值
////            cicsSortDao.insertOrUpdateSingle(cicsSortDTO);
//            this.createEntity(cicsSortDTO);
//        }
    }

    /**
     * 获取周期内所有季度或月度
     *
     * @param startDate
     * @param endDate
     */
    private List<String> inspectYear(String startDate, String endDate) {
        int size = 12;
        List<String> yearMQList = Lists.newArrayList();
        if (Objects.nonNull(startDate) && Objects.nonNull(endDate)) {
            String leftYear = StrUtil.sub(startDate, 0, 4);
            String leftMQ = StrUtil.sub(startDate, 4, 6);
            String rightYear = StrUtil.sub(endDate, 0, 4);
            String rightMQ = StrUtil.sub(endDate, 4, 6);
            boolean zStatus = StrUtil.contains(leftMQ, 'Q');
            if (zStatus) {
                size = 4;
                leftMQ = StrUtil.sub(leftMQ, 1, 2);
                rightMQ = StrUtil.sub(rightMQ, 1, 2);
            }
            if (NumberUtil.isInteger(leftYear) && NumberUtil.isInteger(rightYear)) {
                for (int i = Integer.valueOf(leftYear); i <= Integer.valueOf(rightYear); i++) {

                    for (int j = 1; j <= size; j++) {
                        String mq = null;
                        if (zStatus) {
                            mq = "Q" + j;
                        } else {
                            mq = "" + j;
                        }
                        if (Objects.equals(leftYear, rightYear)) {
                            if (j >= Integer.valueOf(leftMQ) && j <= Integer.valueOf(rightMQ)) {
                                yearMQList.add(i + mq);
                            }
                        } else {
                            if (Objects.equals(i + "", leftYear)) {
                                if (j >= Integer.valueOf(leftMQ)) {
                                    yearMQList.add(i + mq);
                                }
                            } else if (Objects.equals(i + "", rightYear)) {
                                if (j <= Integer.valueOf(rightMQ)) {
                                    yearMQList.add(i + mq);
                                }
                            } else {
                                yearMQList.add(i + mq);

                            }
                        }
                    }
                }

            }
        }
        return yearMQList;
    }
}
