package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EnterpriseDao;
import com.chilunyc.process.dao.industry.CicsIndicatorDao;
import com.chilunyc.process.dao.industry.CleanIndicatorDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizSummaryDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.EntityBreakContractDTO;
import com.chilunyc.process.entity.DTO.enterprise.EntityIssuanceBondsDTO;
import com.chilunyc.process.entity.DTO.industry.CicsCreditSpreadDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSummaryDTO;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.entity.DTO.system.SysFieldDTO;
import com.chilunyc.process.entity.ENUM.PublicEnum;
import com.chilunyc.process.service.enterprise.BizSummaryService;
import com.chilunyc.process.service.enterprise.CleanBizIndicatorService;
import com.chilunyc.process.service.enterprise.EntityBreakContractService;
import com.chilunyc.process.service.enterprise.EntityIssuanceBondsService;
import com.chilunyc.process.service.industry.CicsCreditSpreadService;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.chilunyc.process.service.industry.CicsSummaryService;
import com.chilunyc.process.service.industry.CleanIndicatorService;
import com.chilunyc.process.service.system.SysFieldService;
import com.chilunyc.process.util.DateYearMqUtils;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service("cleanIndicatorImpl")
public class CleanIndicatorImpl extends BaseServiceImpl<CleanIndicatorDao, CleanIndicatorDTO> implements CleanIndicatorService {

    @Autowired
    private CleanBizIndicatorService cleanBizIndicatorService;
    @Autowired
    private CleanIndicatorDao cleanIndicatorDao;
    @Autowired
    private CicsIndicatorDao cicsIndicatorDao;
    @Autowired
    private EnterpriseDao enterpriseDao;
    @Autowired
    private BizSummaryService bizSummaryService;
    @Autowired
    private CicsSummaryService cicsSummaryService;
    @Autowired
    private CicsCreditSpreadService cicsCreditSpreadService;
    @Autowired
    private CicsIndustryLevelService cicsIndustryLevelService;
    @Autowired
    private EntityBreakContractService entityBreakContractService;
    @Autowired
    private EntityIssuanceBondsService entityIssuanceBondsService;

    @Autowired
    private SysFieldService sysFieldService;

    private final String[] PUBLIC_QUARTERS = {"Q1", "Q2", "Q3"};
    private final String PUBLIC_QUARTER = "Q4";
    private final String PUBLIC_YEAR = "2010";
    /**
     * 算法类型：0:压缩算法，1：topsis求值算法
     */
    private Integer typeCalculation;
    /**
     * 劣距字段id值
     */
    private Integer subFieldIdCalculation;
    /**
     * 优距字段id值
     */
    private Integer addFieldIdCalculation;
    /**
     * topsis scrUnit指标值字段id
     */
    private Integer scrUnitFieldIdCalculation;

    /**
     * topsis scr指标值字段id(前端展示字段)
     */
    private Integer scrFieldIdCalculation;

    /**
     * topsis src排名字段id
     */
    private Integer scrSortFieldCalculation;

    /**
     * topsis scr 加工后排名字段id
     */
    private Integer scrProcessSortFieldCalculation;

    /**
     * 劣距指标值：key规则：字段:行业
     */
    private Map<String, Double> subDoubleMap = Maps.newConcurrentMap();
    /**
     * 优距指标值：key规则：字段:行业
     */
    private Map<String, Double> addDoubleMap = Maps.newConcurrentMap();
    /**
     * 字段，年，季度，分组下；平均值，key规则：字段：年：季度：分组
     */
    private Map<String, Double> yearQuarterGroupMap = Maps.newConcurrentMap();

    /**
     * 字段，年，季度，分组下；最大版本，key规则：字段：年：季度：分组
     */
    private Map<String, Integer> yearQuarterVersionGroupMap = Maps.newConcurrentMap();

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Future> futureList = Lists.newArrayList();
        for (BaseFieldDTO fieldDTO : fieldList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                findByCleanYQ(fieldDTO)
            );
            futureList.add(future);

        }
        FutureGetUtil.futureGet(futureList);

    }

    private void findByCleanYQ(BaseFieldDTO fieldDTO) {
        List<BaseEntityDTO> list = cleanBizIndicatorService.findByCleanYQ(fieldDTO);
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            // Future future = ExecutorBuilderUtil.pool.submit(() ->
            findByCleanYQCics(baseEntityDTO, fieldDTO);
            //  );
            // futureList.add(future);
        }
        // FutureGetUtil.futureGet(futureList);
    }

    private void findByCleanYQCics(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO) {
        String lastYear = lastYear(baseEntityDTO);
        List<BaseEntityDTO> list = cleanBizIndicatorService.findByCleanYQCics(baseFieldDTO, baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), lastYear);

        for (BaseEntityDTO entityDTO : list) {

            normalize(entityDTO, baseFieldDTO);

        }

    }

    private String lastYear(BaseEntityDTO baseEntityDTO) {
        String year = null;
//        如果是Q4则是当年 否则则是上一年
        if (PUBLIC_QUARTER.equals(baseEntityDTO.getQuarter())) {
            year = baseEntityDTO.getYear();
        } else {
            if (PUBLIC_YEAR.equals(baseEntityDTO.getYear())) {
                year = baseEntityDTO.getYear();
            } else {
                year = String.valueOf(Integer.parseInt(baseEntityDTO.getYear()) - 1);
            }
        }
        return year;
    }

    @Override
    public void compressCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        typeCalculation = 0;
        findByGroupVersion(fieldList);

    }

    @Override
    public void standardCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {

        subFieldIdCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.SUB_FIELD_ID_CALCULATION.name());
        addFieldIdCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.ADD_FIELD_ID_CALCULATION.name());
        scrUnitFieldIdCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.SCR_UNIT_FIELD_ID_CALCULATION.name());
        scrSortFieldCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.SCR_UNIT_SORT_FIELD_ID_CALCULATION.name());
        scrFieldIdCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.SCR_FIELD_ID_CALCULATION.name());

        //typeCalculation = 1;
        //findByGroupVersion(fieldList);
        if (fieldList.size() > 0) {
            List<Integer> fieldIds = fieldList.stream().map(a -> a.getLeftFieldId()).collect(Collectors.toList());
            List<BaseEntityDTO> list = cleanIndicatorDao.finByFieldYearMqList(fieldIds);
            standardTwo(list, fieldList);
        }

    }

    @Override
    public void finalScrCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        finalScrGroupYQ(fieldList, rightFieldId);

    }

    @Override
    public void processFinalScrCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        typeCalculation = 2;
        scrProcessSortFieldCalculation = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.SCR_SORT_FIELD_ID_CALCULATION.name());
        findByGroupVersion(fieldList);
    }

    @Override
    public void creditSpreadCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {

        List<CicsCreditSpreadDTO> list = cicsCreditSpreadService.findByVersionYQ(rightFieldId);
        List<Future> futureList = Lists.newArrayList();
        List<CicsIndustryLevelDTO> levelDTOList = cicsIndustryLevelService.getEntityList(new QueryWrapper());
        Map<Integer, List<CicsIndustryLevelDTO>> map = levelDTOList.stream().collect(Collectors.groupingBy(a -> a.getParentId()));
        Map<String, CicsCreditSpreadDTO> fMap = list.stream().collect(Collectors.toMap(a -> a.getYear() + a.getQuarter() + "-" + a.getCicsId(), Function.identity(), (k1, k2) -> k2));
        for (CicsCreditSpreadDTO cicsCreditSpreadDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> ExecutorFindByVersionYQ(cicsCreditSpreadDTO, rightFieldId, map, fMap));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }

    /**
     * 信用利差 线程池
     *
     * @param cicsCreditSpreadDTO
     * @param rightFieldId
     */
    private void ExecutorFindByVersionYQ(CicsCreditSpreadDTO cicsCreditSpreadDTO, Integer rightFieldId, Map<Integer, List<CicsIndustryLevelDTO>> map, Map<String, CicsCreditSpreadDTO> fMap) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        BeanUtil.copyProperties(cicsCreditSpreadDTO, baseEntityDTO);
        Double v1 = ExecutorFindByValue(cicsCreditSpreadDTO.getYear() + cicsCreditSpreadDTO.getQuarter(), fMap, cicsCreditSpreadDTO.getIndicatorValue(), 1, cicsCreditSpreadDTO.getCicsId());
        insertToTopsis(baseEntityDTO, rightFieldId, v1, cicsCreditSpreadDTO.getCicsId());
        List<Integer> levelList = Lists.newArrayList();
        levelCicsRecursion(levelList, map, cicsCreditSpreadDTO.getCicsId());
        creditSpreadCicsRecursion(rightFieldId, v1, baseEntityDTO, levelList);
    }

    private Double ExecutorFindByValue(String yearMQ, Map<String, CicsCreditSpreadDTO> fMap, Double v1, int size, Integer cicsId) {

        Double v2 = v1;

        String lastYearMQ = DateYearMqUtils.laggingSubYear(yearMQ, size);
        String key = lastYearMQ + "-" + cicsId;
        if (!Objects.equals(lastYearMQ, "2014Q4")) {
            if (fMap.containsKey(key)) {
                v2 = fMap.get(key).getIndicatorValue();
            } else {
                ExecutorFindByValue(yearMQ, fMap, v1, size + 1, cicsId);
            }
        }
        Double v3 = Double.valueOf(0);
        if (NumberUtil.compare(v2, 0) != 0) {
            v3 = NumberUtil.div(v1, v2) - 1;
        }
        return v3;
    }

    @Override
    public void entityBreakContractCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
//        违约金额字段
        Integer defaultAmountFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.DEFAULT_AMOUNT_FIELD_ID_CALCULATION.name());
//        违约金额占比字段
        Integer proportionDefaultAmountFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.PROPORTION_DEFAULT_AMOUNT_FIELD_ID_CALCULATION.name());
//        企业数量字段
        Integer sizeFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.ENTITY_SIZE_FIELD_ID_CALCULATION.name());
//        企业数量占比字段
        Integer proportionSizeFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.PROPORTION_ENTITY_SIZE_FIELD_ID_CALCULATION.name());
//        字段集合
        List<Integer> fieldIds = Lists.newArrayList(defaultAmountFieldId, proportionDefaultAmountFieldId, sizeFieldId, proportionSizeFieldId);
//        获取变化年和季度
        List<BaseEntityDTO> list = entityBreakContractService.findByGroupYQ(fieldIds);
//        循环年和季度
        List<Future> futureList = Lists.newArrayList();
        List<CicsIndustryLevelDTO> levelDTOList = cicsIndustryLevelService.getEntityList(new QueryWrapper());
        Map<Integer, List<CicsIndustryLevelDTO>> map = levelDTOList.stream().collect(Collectors.groupingBy(a -> a.getParentId()));
        for (BaseEntityDTO baseEntityDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> ExecutorEntityBreakContractCalculation(baseEntityDTO, defaultAmountFieldId, proportionDefaultAmountFieldId, sizeFieldId, proportionSizeFieldId, map));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    /**
     * 违约展望 线程池
     *
     * @param entityDTO
     * @param defaultAmountFieldId
     * @param proportionDefaultAmountFieldId
     * @param sizeFieldId
     * @param proportionSizeFieldId
     */
    private void ExecutorEntityBreakContractCalculation(BaseEntityDTO entityDTO, Integer defaultAmountFieldId, Integer proportionDefaultAmountFieldId, Integer sizeFieldId, Integer proportionSizeFieldId, Map<Integer, List<CicsIndustryLevelDTO>> map) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;
        //            获取年和季度内信息
        List<EntityBreakContractDTO> breakContractDTOList = entityBreakContractService.findByYQ(baseEntityDTO.getYear(), baseEntityDTO.getQuarter());
//            写入数据
        entityBreakContractInsert(breakContractDTOList, baseEntityDTO, defaultAmountFieldId, proportionDefaultAmountFieldId, sizeFieldId, proportionSizeFieldId, map);
    }

    @Override
    public void entityIssuanceBondsCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
//        获取变化年，季度信息
        List<BaseEntityDTO> list = entityIssuanceBondsService.findByGroupYQ(rightFieldId);
        List<Future> futureList = Lists.newArrayList();
        List<CicsIndustryLevelDTO> levelDTOList = cicsIndustryLevelService.getEntityList(new QueryWrapper());
        Map<Integer, List<CicsIndustryLevelDTO>> levelMap = levelDTOList.stream().collect(Collectors.groupingBy(a -> a.getParentId()));
//        循环年季度
        for (BaseEntityDTO baseEntityDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> executorEntityIssuanceBondsCalculation(baseEntityDTO, rightFieldId, levelMap));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    /**
     * @param baseEntityDTO
     * @param rightFieldId
     */
    private void executorEntityIssuanceBondsCalculation(BaseEntityDTO baseEntityDTO, Integer rightFieldId, Map<Integer, List<CicsIndustryLevelDTO>> levelMap) {
        //            读取年和季度全部信息
        List<EntityIssuanceBondsDTO> issuanceBondsDTOList = entityIssuanceBondsService.findByYQ(baseEntityDTO.getYear(), baseEntityDTO.getQuarter());
        //        list转map根据cicsId转换
        Map<Integer, List<EntityIssuanceBondsDTO>> map = issuanceBondsDTOList.stream().collect(Collectors.groupingBy(EntityIssuanceBondsDTO::getCicsId));
//            循环
        List<Future> futureList = Lists.newArrayList();
        for (Integer key : map.keySet()) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> executorEntityIssuanceBondsFindByYQ(map, key, baseEntityDTO, rightFieldId, levelMap));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    /**
     * @param map
     * @param key
     * @param baseEntityDTO
     * @param rightFieldId
     */
    private void executorEntityIssuanceBondsFindByYQ(Map<Integer, List<EntityIssuanceBondsDTO>> map, Integer key, BaseEntityDTO baseEntityDTO, Integer rightFieldId, Map<Integer, List<CicsIndustryLevelDTO>> levelMap) {
        //                获取数量
        Double size = Double.valueOf(map.get(key).size());
//                写入数据
        insertToTopsis(baseEntityDTO, rightFieldId, size, key);
//                递归子行业数据
        List<Integer> levelList = Lists.newArrayList();
        levelCicsRecursion(levelList, levelMap, key);
        creditSpreadCicsRecursion(rightFieldId, size, baseEntityDTO, levelList);
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
                value = baseFieldDTO.getRightFieldId();
                break;
            }
        }
        return value;
    }

    /**
     * @param list
     * @param name
     * @return
     */
    private Integer enumFieldLeft(List<BaseFieldDTO> list, String name) {
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
     * 违约或展望算法
     *
     * @param list                           当年，当季度所有数据
     * @param baseEntityDTO                  基础信息
     * @param defaultAmountFieldId           违约金额字段
     * @param proportionDefaultAmountFieldId 违约金额占比字段
     * @param sizeFieldId                    企业数量字段
     * @param proportionSizeFieldId          企业数量占比字段
     */
    private void entityBreakContractInsert(List<EntityBreakContractDTO> list, BaseEntityDTO baseEntityDTO, Integer defaultAmountFieldId, Integer proportionDefaultAmountFieldId, int sizeFieldId, int proportionSizeFieldId, Map<Integer, List<CicsIndustryLevelDTO>> levelMap) {
//        当年当季度所有数据违约金额和
        Double sumValue = list.stream().collect(Collectors.summingDouble(EntityBreakContractDTO::getIndicatorValue));
//        当年当季度数量
        int size = list.size();
//        list转map根据cicsId转换
        Map<Integer, List<EntityBreakContractDTO>> map = list.stream().collect(Collectors.groupingBy(EntityBreakContractDTO::getCicsId));
//        循环
        for (Integer key : map.keySet()) {
//            获取同一个cics下 违约金额和
            Double value = map.get(key).stream().collect(Collectors.summingDouble(EntityBreakContractDTO::getIndicatorValue));
//            计算占比
            Double zbValue = Double.valueOf(0);
            if (NumberUtil.compare(sumValue, 0) != 0) {
                zbValue = NumberUtil.div(value, sumValue);
            }
//            同cics下数量
            int mapSize = map.get(key).size();
//            数量占比算法
            Double zbSize = NumberUtil.div(mapSize, size);

            List<Integer> levelList = Lists.newArrayList();
            levelCicsRecursion(levelList, levelMap, key);
//            写入违约金额
            insertToTopsis(baseEntityDTO, defaultAmountFieldId, value, key);
//            递归子行业违约金额
            creditSpreadCicsRecursion(defaultAmountFieldId, value, baseEntityDTO, levelList);
//            写入违约金额占比
            insertToTopsis(baseEntityDTO, proportionDefaultAmountFieldId, zbValue, key);
//            递归子行业违约金额占比
            creditSpreadCicsRecursion(proportionDefaultAmountFieldId, zbValue, baseEntityDTO, levelList);
//            写入企业违约数量
            insertToTopsis(baseEntityDTO, sizeFieldId, Double.valueOf(mapSize), key);
//            递归子行业企业违约数量
            creditSpreadCicsRecursion(sizeFieldId, Double.valueOf(mapSize), baseEntityDTO, levelList);
//            写入企业违约数量占比
            insertToTopsis(baseEntityDTO, proportionSizeFieldId, zbSize, key);
//            递归子行业企业违约数量占比
            creditSpreadCicsRecursion(proportionSizeFieldId, zbSize, baseEntityDTO, levelList);
        }

    }

    /**
     * 递归处理信用利差，违约展望，子行业数据
     *
     * @param fieldId       字段id
     * @param value         指标值
     * @param baseEntityDTO 基础表
     */
    private void creditSpreadCicsRecursion(Integer fieldId, Double value, BaseEntityDTO baseEntityDTO, List<Integer> levelList) {
        if (levelList.size() > 0) {
//            QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper();
//            queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId).in("cics_id", levelList);
//            this.deleteEntities(queryWrapper);
            for (Integer id : levelList) {
                CleanIndicatorDTO cleanIndicatorDTO = new CleanIndicatorDTO();
                BeanUtil.copyProperties(baseEntityDTO, cleanIndicatorDTO);
                cleanIndicatorDTO.setIndicatorValue(value);
                cleanIndicatorDTO.setFieldId(fieldId);
                cleanIndicatorDTO.setCicsId(id);
                cleanIndicatorDTO.setId(null);
                // 唯一值报错
//                cleanIndicatorDao.insert(cleanIndicatorDTO);
                cleanIndicatorDao.insertOrUpdateSingle(cleanIndicatorDTO);
            }
        }
    }

    /**
     * 递归获取下级行业
     *
     * @param list
     * @param map
     * @param parentId
     */
    private void levelCicsRecursion(List<Integer> list, Map<Integer, List<CicsIndustryLevelDTO>> map, Integer parentId) {
        if (map.containsKey(parentId)) {
            for (CicsIndustryLevelDTO cicsIndustryLevelDTO : map.get(parentId)) {
                list.add(cicsIndustryLevelDTO.getId());
                levelCicsRecursion(list, map, cicsIndustryLevelDTO.getId());
            }
        }
    }

    /**
     * 根据字段获取表中全部年份和季度
     *
     * @param list
     * @param fieldId
     */
    private void finalScrGroupYQ(List<BaseFieldDTO> list, Integer fieldId) {
        List<Integer> fieldList = list.stream().map(BaseFieldDTO::getLeftFieldId).collect(Collectors.toList());
//        根据字段集合读取有数据年份和季度
        List<BaseEntityDTO> groupList = cleanIndicatorDao.findByGroupYQ(fieldList);
//        循环年份季度
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : groupList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                finalScrFieldValue(baseEntityDTO, list, fieldId, fieldList));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    /**
     * 读取数据并计算写入数据库
     *
     * @param baseEntityDTO
     * @param fieldDTOList
     * @param fieldId
     * @param fieldList
     */
    private void finalScrFieldValue(BaseEntityDTO baseEntityDTO, List<BaseFieldDTO> fieldDTOList, Integer fieldId, List<Integer> fieldList) {
//        根据年，季度，字段集合获取相关数据
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).in("field_id", fieldList);
        List<CleanIndicatorDTO> list = cleanIndicatorDao.selectList(queryWrapper);
//        按照行业id list转map
        Map<Integer, List<CleanIndicatorDTO>> map = list.stream().collect(Collectors.groupingBy(BaseEntityDTO::getCicsId));
//        按照字段id list转map
        Map<Integer, Double> fieldMap = fieldDTOList.stream().collect(Collectors.toMap(BaseFieldDTO::getLeftFieldId, BaseFieldDTO::getProportion));
//        循环行业
        for (Integer key : map.keySet()) {
//            数量
            int size = 0;
//            指标值
            Double value = Double.valueOf(0);
            for (CleanIndicatorDTO cleanIndicatorDTO : map.get(key)) {
//                判断是否包含该字段
                if (fieldMap.containsKey(cleanIndicatorDTO.getFieldId())) {
                    size += 1;
//                    算法
                    value += NumberUtil.mul(cleanIndicatorDTO.getIndicatorValue(), fieldMap.get(cleanIndicatorDTO.getFieldId()));
                }
            }
//            判断字段数量和返回行业指标值字段是否一致，如果一致也计算
            if (fieldDTOList.size() != size) {
                //TODO 不处理提醒
            } else {
//                删除并写写入数据库
                insertToTopsis(baseEntityDTO, fieldId, value, key);
            }
        }
    }

    /**
     * 读取分组信息
     *
     * @param fieldList
     */
    private void findByGroupVersion(List<BaseFieldDTO> fieldList) {
//        获取分组Q4季度信息
        List<BaseEntityDTO> list = cicsSummaryService.findByGroupVersion(PUBLIC_QUARTER);
//        获取季节枚举
        PublicEnum.CICS_GROUP[] groupList = PublicEnum.CICS_GROUP.values();
        List<Future> futureList = Lists.newArrayList();
        for (int i = 0; i < list.size(); i++) {
            int finalI = i;
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
                compressIndustry(list.get(finalI), groupList, fieldList, finalI));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
        addDoubleMap = Maps.newConcurrentMap();
        subDoubleMap = Maps.newConcurrentMap();
    }


    /**
     * 根据年份季节分组压缩部分数据
     *
     * @param baseEntityDTO
     * @param groupList
     * @param fieldList
     * @param i
     */
    private void compressIndustry(BaseEntityDTO baseEntityDTO, PublicEnum.CICS_GROUP[] groupList, List<BaseFieldDTO> fieldList, int i) {
//        处理年份和季度
        List<BaseEntityDTO> quarterList = yearToQuarterList(baseEntityDTO, i);
//        行业分组枚举循环
        for (PublicEnum.CICS_GROUP cicsGroup : groupList) {
//            读取分组下行业信息
            QueryWrapper<CicsSummaryDTO> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("assets_type", cicsGroup.getAssetsType()).eq("growth_type", cicsGroup.getGrowthType());
            List<CicsSummaryDTO> summaryDTOList = cicsSummaryService.getEntityList(queryWrapper);
//            判断走那个算法规则
            if (typeCalculation == 2) {
                fieldProcessIndustryAvg(summaryDTOList, fieldList, quarterList, cicsGroup);
            } else {
                //            处理指标值
                fieldCompressIndustry(summaryDTOList, fieldList, quarterList);
            }

        }
        if (typeCalculation == 2) {
            fieldProcessIndustry(quarterList, groupList, fieldList, baseEntityDTO);
        }

    }

    /**
     * 计算分组内差值
     *
     * @param quarterList
     * @param groupList
     * @param fieldList
     */
    private void fieldProcessIndustry(List<BaseEntityDTO> quarterList, PublicEnum.CICS_GROUP[] groupList, List<BaseFieldDTO> fieldList, BaseEntityDTO entityDTO) {
//        循环字段
        for (BaseFieldDTO baseFieldDTO : fieldList) {
//            循环年季度
            for (BaseEntityDTO baseEntityDTO : quarterList) {
//                全分组下行业平局值之和，平均值见方法fieldProcessIndustryAv
                Double value = Double.valueOf(0);
                int size = 0;
//                循环分组
                for (PublicEnum.CICS_GROUP group : groupList) {
//                    全局分组平局值map
                    String key = baseFieldDTO.getLeftFieldId() + ":" + baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + group;
                    if (yearQuarterGroupMap.containsKey(key)) {
//                        读取分组平局值
                        Double groupValue = yearQuarterGroupMap.get(key);
                        size += 1;
                        value += groupValue;
                    }

                }
//                判断size数量是不是和分组相同
                if (size == groupList.length) {
//                    求四个分组之和的平局值
                    Double avgValue = NumberUtil.div(value, Double.valueOf(groupList.length));
                    for (PublicEnum.CICS_GROUP group : groupList) {
                        String key = baseFieldDTO.getLeftFieldId() + ":" + baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + group;
//                        平局值和分组平局值之差
                        Double subValue = NumberUtil.sub(yearQuarterGroupMap.get(key), avgValue);

                        Integer version = yearQuarterVersionGroupMap.get(key);
//                        读取分组下行业信息
                        QueryWrapper<CicsSummaryDTO> queryWrapper = new QueryWrapper<>();
                        queryWrapper.eq("year", entityDTO.getYear()).eq("quarter", entityDTO.getQuarter()).eq("assets_type", group.getAssetsType()).eq("growth_type", group.getGrowthType());
                        List<CicsSummaryDTO> summaryDTOList = cicsSummaryService.getEntityList(queryWrapper);
//                        计算行业差值，写入数据库
                        fieldProcessIndustryInsert(summaryDTOList, baseFieldDTO, baseEntityDTO, subValue, version);
                    }
                }
            }
        }
    }

    /**
     * 读取行业指标值，然后加上分组差值重新写入数据库
     *
     * @param summaryDTOList
     * @param baseFieldDTO
     * @param baseEntityDTO
     * @param subValue
     */
    private void fieldProcessIndustryInsert(List<CicsSummaryDTO> summaryDTOList, BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO, Double subValue, Integer version) {
//        list<bean>转list<integer> cicsId转换
        List<Integer> list = summaryDTOList.stream().map(CicsSummaryDTO::getCicsId).collect(Collectors.toList());
        baseEntityDTO.setVersion(version);
        if (list.size() > 0) {
//        读取年，季度，字段，行业列表
            QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
            queryWrapper.in("cics_id", list).eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
            List<CleanIndicatorDTO> indicatorDTOList = cleanIndicatorDao.selectList(queryWrapper);
            indicatorDTOList.forEach(a -> {
                //            行业指标值-分组差值=行业计算后指标值
                Double value = NumberUtil.sub(a.getIndicatorValue(), subValue);
                a.setIndicatorValue(value);
            });
            //        降序排序
            indicatorDTOList = indicatorDTOList.stream().sorted(Comparator.comparingDouble(CleanIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());
            for (int i = 0; i < indicatorDTOList.size(); i++) {
                CleanIndicatorDTO cleanIndicatorDTO = indicatorDTOList.get(i);
//            写入数据库(指标值)
                insertToTopsis(baseEntityDTO, baseFieldDTO.getRightFieldId(), cleanIndicatorDTO.getIndicatorValue(), cleanIndicatorDTO.getCicsId());
//            写入数据库(排行)
                insertToTopsis(baseEntityDTO, scrProcessSortFieldCalculation, Double.valueOf(i + 1), cleanIndicatorDTO.getCicsId());
            }
        }
    }

    /**
     * 根据年，字段。季度，分组下行业列表求每一分组平均值
     *
     * @param summaryDTOList
     * @param fieldList
     * @param quarterList
     * @param cicsGroup
     */
    private void fieldProcessIndustryAvg(List<CicsSummaryDTO> summaryDTOList, List<BaseFieldDTO> fieldList, List<BaseEntityDTO> quarterList, PublicEnum.CICS_GROUP cicsGroup) {
        List<Integer> list = summaryDTOList.stream().map(CicsSummaryDTO::getCicsId).collect(Collectors.toList());
//        循环分组支持的季度
        for (BaseEntityDTO baseEntityDTO : quarterList) {
//            循环字段
            List<Future> futureList = Lists.newArrayList();
            for (BaseFieldDTO baseFieldDTO : fieldList) {
                //   Future future = ExecutorBuilderUtil.pool.submit(() ->
                findByGroupCicsAvg(baseFieldDTO, baseEntityDTO, list, cicsGroup);
                //  );
                //  futureList.add(future);
            }
            //  FutureGetUtil.futureGet(futureList);
        }
    }

    private void findByGroupCicsAvg(BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO, List<Integer> list, PublicEnum.CICS_GROUP cicsGroup) {
        if (list.size() > 0) {
            //                获取年，季度，字段，行业集合下，根据年，季度，字段平局值
            List<CleanIndicatorDTO> indicatorDTOList = cleanIndicatorDao.findByGroupCicsAvg(baseFieldDTO.getLeftFieldId(), baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), list);
            for (CleanIndicatorDTO cleanIndicatorDTO : indicatorDTOList) {
//                    循环写入全局变量中
                String key = baseFieldDTO.getLeftFieldId() + ":" + cleanIndicatorDTO.getYear() + ":" + cleanIndicatorDTO.getQuarter() + ":" + cicsGroup;
                yearQuarterGroupMap.put(key, cleanIndicatorDTO.getIndicatorValue());
                yearQuarterVersionGroupMap.put(key, cleanIndicatorDTO.getVersion());
            }
        }

    }


    /**
     * 根据字段获取行业和年份指标值
     *
     * @param summaryDTOList
     * @param fieldList
     */
    private void fieldCompressIndustry(List<CicsSummaryDTO> summaryDTOList, List<BaseFieldDTO> fieldList, List<BaseEntityDTO> quarterList) {
        List<Integer> list = summaryDTOList.stream().map(CicsSummaryDTO::getCicsId).collect(Collectors.toList());
        if (list.size() > 0) {
//        循环分组支持的季度
            for (BaseEntityDTO baseEntityDTO : quarterList) {
//            循环字段
                for (BaseFieldDTO baseFieldDTO : fieldList) {
                    executorSelectList(list, baseEntityDTO, baseFieldDTO);
                }
//            计算行业topsis值
                if (typeCalculation != 0) {
                    topsisSrcSubCalculation(list, fieldList, baseEntityDTO);
                }
            }
        }
    }

    private void executorSelectList(List<Integer> list, BaseEntityDTO entityDTO, BaseFieldDTO baseFieldDTO) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;

        //                获取分组先所有行业，当前年，季度，字段数据
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("cics_id", list).eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        queryWrapper.orderBy(true, true, "indicator_value");
        List<CleanIndicatorDTO> indicatorDTOList = cleanIndicatorDao.selectList(queryWrapper);
//               判断计算规则类型
        if (typeCalculation == 0) {
            algorithmCompress(indicatorDTOList, baseEntityDTO, baseFieldDTO, list);
        } else {
            topsisAddAndSubCalculation(indicatorDTOList, baseFieldDTO, baseEntityDTO);
        }
    }

    /**
     * 压缩算法
     *
     * @param indicatorDTOList
     */
    private void algorithmCompress(List<CleanIndicatorDTO> indicatorDTOList, BaseEntityDTO entityDTO, BaseFieldDTO baseFieldDTO, List<Integer> list) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;
        //        最小压缩值
        Double minCompress = null;
//        最大压缩值
        Double maxCompress = null;
//        当前数量必须大于3负责无法计算最大最小值
        if (indicatorDTOList.size() >= 3) {
            int size = indicatorDTOList.size() + 1;
//            最小坐标
            Double minValue = NumberUtil.div(size, 4);
//            最大坐标
            Double maxValue = NumberUtil.mul(minValue, Double.valueOf(3));
//            最小整数坐标
            int intMinValue = minValue.intValue();
//            最大整数坐标
            int intMaxValue = maxValue.intValue();
//            最小坐标距左侧整数小数值
            Double minRightValue = minValue - intMinValue;
//            最小坐标距右侧证书小数值
            Double minLeftValue = 1 - minRightValue;
//            最大坐标距左侧证书小数值
            Double maxRightValue = maxValue - intMaxValue;
//            最大坐标距右侧证书小数值
            Double maxLeftValue = 1 - maxRightValue;
//            如果坐标为整数则走方案一
            if (minRightValue == 0) {
//                最大值标值
                Double maxIndicatorValue = indicatorDTOList.get(intMaxValue - 1).getIndicatorValue();
//                最小指标值
                Double minIndicatorValue = indicatorDTOList.get(intMinValue - 1).getIndicatorValue();
                Double IQR = maxIndicatorValue - minIndicatorValue;
//                IQR指标值1.5倍
                Double multiple = NumberUtil.mul(IQR, Double.valueOf(1.5));
//                求最小压缩值
                minCompress = NumberUtil.sub(minIndicatorValue, multiple);
//                求最大压缩值
                maxCompress = NumberUtil.add(maxIndicatorValue, multiple);
            } else {
//                左侧最大值标值
                Double maxLeftIndicatorValue = indicatorDTOList.get(intMaxValue - 1).getIndicatorValue();
//                右侧最大指标值
                Double maxRightIndicatorValue = indicatorDTOList.get(intMaxValue).getIndicatorValue();

                Double maxIndicatorValue = null;
//                根据规则，距离越近，权数越大，距离越远，权数越小
                maxIndicatorValue = NumberUtil.mul(maxLeftValue, maxLeftIndicatorValue) + NumberUtil.mul(maxRightValue, maxRightIndicatorValue);
//              左侧最小指标值
                Double minLeftIndicatorValue = indicatorDTOList.get(intMinValue - 1).getIndicatorValue();
//                右侧最小指标值
                Double minRightIndicatorValue = indicatorDTOList.get(intMinValue).getIndicatorValue();
                Double minIndicatorValue = null;
//                根据规则，距离越近，权数越大，距离越远，权数越小
                minIndicatorValue = NumberUtil.mul(minLeftValue, minLeftIndicatorValue) + NumberUtil.mul(minRightValue, minRightIndicatorValue);
                Double IQR = maxIndicatorValue - minIndicatorValue;
//                最大指标值1.5倍
                Double multiple = NumberUtil.mul(IQR, Double.valueOf(1.5));
//                求最小压缩值
                minCompress = NumberUtil.sub(minIndicatorValue, multiple);
//                求最大压缩值
                maxCompress = NumberUtil.add(maxIndicatorValue, multiple);
            }
        }
//        删除原有数据
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.in("cics_id", list).eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getRightFieldId());
        cleanIndicatorDao.delete(queryWrapper);
//        新增新数据
        for (int i = 0; i < indicatorDTOList.size(); i++) {
            CleanIndicatorDTO cleanIndicatorDTO = indicatorDTOList.get(i);
            if (minCompress != null && NumberUtil.compare(cleanIndicatorDTO.getIndicatorValue(), minCompress) == -1) {
                cleanIndicatorDTO.setIndicatorValue(minCompress);
            } else if (maxCompress != null && NumberUtil.compare(cleanIndicatorDTO.getIndicatorValue(), maxCompress) == 1) {
                cleanIndicatorDTO.setIndicatorValue(maxCompress);
            }
            cleanIndicatorDTO.setId(null);
            cleanIndicatorDTO.setFieldId(baseFieldDTO.getRightFieldId());
            if (Objects.isNull(cleanIndicatorDTO.getIndicatorValue())) {
            }
            cleanIndicatorDao.insert(cleanIndicatorDTO);
        }


    }

    /**
     * topsis算值 求行业字段劣距优距
     *
     * @param list
     */
    private void topsisAddAndSubCalculation(List<CleanIndicatorDTO> list, BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO) {
        if (list.size() > 0) {
            Double sumValue = Double.valueOf(0);
            //求所有数据二次方之和: 3-6
            for (CleanIndicatorDTO cleanIndicatorDTO : list) {
                sumValue += NumberUtil.mul(cleanIndicatorDTO.getIndicatorValue(), cleanIndicatorDTO.getIndicatorValue());
            }
//            平方根处理:3-6
            Double finalSumValue = Math.sqrt(sumValue);
//            设置标准化值：3-6
            list.forEach(a -> {
                if (NumberUtil.compare(finalSumValue, Double.valueOf(0)) != 0) {
                    a.setIndicatorValue(NumberUtil.div(a.getIndicatorValue(), finalSumValue));
                } else {
                    a.setIndicatorValue(Double.valueOf(0));
                }

            });
//        获取最小值
            Double minValue = list.stream().min(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//        获取最大值
            Double maxValue = list.stream().max(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//        求每一个行业字段劣距和优距值
            list.forEach(a -> {
                addDoubleMap.put(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + a.getCicsId(), NumberUtil.sub(a.getIndicatorValue(), maxValue));
                subDoubleMap.put(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + a.getCicsId(), NumberUtil.sub(a.getIndicatorValue(), minValue));
            });
//        标准化指标值
            List<Future> futureList = Lists.newArrayList();
            for (CleanIndicatorDTO cleanIndicatorDTO : list) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() -> insertToTopsis(baseEntityDTO, baseFieldDTO.getRightFieldId(), cleanIndicatorDTO.getIndicatorValue(), cleanIndicatorDTO.getCicsId()));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }

    }

    /**
     * 获取指标值和最优距和最劣距值
     *
     * @param cicsList
     * @param fieldList
     */
    private void topsisSrcSubCalculation(List<Integer> cicsList, List<BaseFieldDTO> fieldList, BaseEntityDTO entityDTO) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;
        List<CleanIndicatorDTO> list = Lists.newArrayList();

        for (Integer cicsId : cicsList) {
//            最优距之和
            Double addValue = Double.valueOf(0);
//            最劣距之和
            Double subValue = Double.valueOf(0);
//            循环字段
            for (BaseFieldDTO baseFieldDTO : fieldList) {
//                获取当前字段和行业值
                Double getAddValue = addDoubleMap.get(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + cicsId);
                Double getSubValue = subDoubleMap.get(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + cicsId);
//                获取优距劣距二次方值
                Double getAddValueSq = NumberUtil.mul(getAddValue, getAddValue);
                Double getSubValueSq = NumberUtil.mul(getSubValue, getSubValue);
//                写入优距劣距之和字段
                addValue += NumberUtil.div(getAddValueSq, Double.valueOf(fieldList.size()));
                subValue += NumberUtil.div(getSubValueSq, Double.valueOf(fieldList.size()));

            }
//            优距劣距平方根算最劣距，最优距值
            Double radicalAddValue = Math.sqrt(addValue);
            Double radicalSubValue = Math.sqrt(subValue);
            if (NumberUtil.compare(radicalAddValue, 0) == 0) {
            }
            Double radicalAddAndSubValue = Double.valueOf(0);
            if (radicalSubValue != 0) {
                radicalAddAndSubValue = NumberUtil.add(radicalAddValue, radicalSubValue);
            }
//            行业指标值
            Double srcValue = Double.valueOf(0);
            if (radicalAddAndSubValue != 0) {
                srcValue = NumberUtil.div(radicalSubValue, radicalAddAndSubValue);
            }

//            删除写入

            insertToTopsis(baseEntityDTO, addFieldIdCalculation, radicalAddValue, cicsId);
//            删除写入
            insertToTopsis(baseEntityDTO, subFieldIdCalculation, radicalSubValue, cicsId);
//            删除写入

            insertToTopsisList(baseEntityDTO, scrUnitFieldIdCalculation, srcValue, cicsId, list);

        }

//        行业所属分组内排行
        topsisCicsSort(list, baseEntityDTO);
    }

    /**
     * 指标值处理算法和排序
     *
     * @param list
     * @param entityDTO
     */
    private void topsisCicsSort(List<CleanIndicatorDTO> list, BaseEntityDTO entityDTO) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;
        //        获取最小值
        Double minValue = list.stream().min(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//        获取最大值
        Double maxValue = list.stream().max(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//        分母
        Double maxSubMinValue = NumberUtil.sub(maxValue, minValue);
        list.forEach(a -> {
//            分子
            Double valueSubMinValue = NumberUtil.sub(a.getIndicatorValue(), minValue);
//            写入分子除分母值
            Double v1 = Double.valueOf(0);
            if (maxSubMinValue != 0) {
                v1 = NumberUtil.div(valueSubMinValue, maxSubMinValue);
            }
            a.setIndicatorValue(v1);
        });
//        降序排序
        list = list.stream().sorted(Comparator.comparingDouble(CleanIndicatorDTO::getIndicatorValue).reversed()).collect(Collectors.toList());
        Double v1 = Double.valueOf(0);
        int size = 0;
        for (int i = 0; i < list.size(); i++) {
//            先删除后添加
            CleanIndicatorDTO cleanIndicatorDTO = list.get(i);
            //排行

            if (NumberUtil.compare(cleanIndicatorDTO.getIndicatorValue(), v1) != 0) {
                size = i;
            }
            v1 = cleanIndicatorDTO.getIndicatorValue();
            BaseEntityDTO finalBaseEntityDTO = baseEntityDTO;
            int finalSize = size;
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> insertToTopsis(finalBaseEntityDTO, scrSortFieldCalculation, Double.valueOf(finalSize), cleanIndicatorDTO.getCicsId()));
            //指标值
            Double vv = NumberUtil.mul(cleanIndicatorDTO.getIndicatorValue(), PublicEnum.INDUSTRY_BASE_SCR);
            insertToTopsis(finalBaseEntityDTO, scrFieldIdCalculation, vv, cleanIndicatorDTO.getCicsId());

        }


    }

    /**
     * 先删除后增加
     *
     * @param baseEntityDTO
     * @param fieldId
     * @param value
     * @param cicsId
     */
    private CleanIndicatorDTO insertToTopsis(BaseEntityDTO baseEntityDTO, Integer fieldId, Double value, Integer cicsId) {
        //        新增数据
        CleanIndicatorDTO cleanIndicatorDTO = new CleanIndicatorDTO();
        BeanUtil.copyProperties(baseEntityDTO, cleanIndicatorDTO);
//        删除原有数据
//        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
//        queryWrapper.eq("cics_id", cicsId).eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId);
//        cleanIndicatorDao.delete(queryWrapper);

        cleanIndicatorDTO.setIndicatorValue(value);
        cleanIndicatorDTO.setCreateTime(new Date());
        cleanIndicatorDTO.setUpdateTime(new Date());
        cleanIndicatorDTO.setFieldId(fieldId);
        cleanIndicatorDTO.setCicsId(cicsId);
        cleanIndicatorDTO.setId(null);
        cleanIndicatorDao.insertOrUpdateSingle(cleanIndicatorDTO); //唯一值报错
        return cleanIndicatorDTO;
    }

    /**
     * 先删除后增加
     *
     * @param baseEntityDTO
     * @param fieldId
     * @param value
     * @param cicsId
     */
    private void insertToTopsisList(BaseEntityDTO baseEntityDTO, Integer fieldId, Double value, Integer cicsId, List<CleanIndicatorDTO> list) {
        //        新增数据
        CleanIndicatorDTO cleanIndicatorDTO = new CleanIndicatorDTO();
        BeanUtil.copyProperties(baseEntityDTO, cleanIndicatorDTO);
//        删除原有数据
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("cics_id", cicsId).eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId);
        cleanIndicatorDao.delete(queryWrapper);

        cleanIndicatorDTO.setIndicatorValue(value);
        cleanIndicatorDTO.setFieldId(fieldId);
        cleanIndicatorDTO.setCicsId(cicsId);
        cleanIndicatorDTO.setCreateTime(new Date());
        cleanIndicatorDTO.setUpdateTime(new Date());
        cleanIndicatorDTO.setId(null);
        cleanIndicatorDao.insert(cleanIndicatorDTO);
        list.add(cleanIndicatorDTO);
    }


    /**
     * 生成分组下支持季度
     *
     * @param baseEntityDTO
     * @param i
     * @return
     */
    private List<BaseEntityDTO> yearToQuarterList(BaseEntityDTO baseEntityDTO, int i) {
        List<BaseEntityDTO> list = Lists.newArrayList();
        String[] publicQuarters = PublicEnum.PUBLIC_QUARTERS;
        if (i != 0) {
            list.add(yearToQuarter(baseEntityDTO.getYear(), PublicEnum.PUBLIC_QUARTER, baseEntityDTO.getVersion()));
        }
        String year = String.valueOf(Integer.valueOf(baseEntityDTO.getYear()) + 1);
        for (String quarter : publicQuarters) {
            list.add(yearToQuarter(year, quarter, baseEntityDTO.getVersion()));
        }
        return list;
    }

    /**
     * 生成Bean
     *
     * @param year
     * @param quarter
     * @return
     */
    private BaseEntityDTO yearToQuarter(String year, String quarter, Integer version) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO.setQuarter(quarter);
        baseEntityDTO.setYear(year);
        baseEntityDTO.setVersion(version);
        return baseEntityDTO;
    }

    /**
     * 二次加权归一法
     */
    private void normalize(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO) {
//        读取年季度，行业字段下所有数据
        baseEntityDTO.setFieldId(baseFieldDTO.getLeftFieldId());
        String lastYear = lastYear(baseEntityDTO);
        List<CleanBizIndicatorDTO> list = cleanBizIndicatorService.findByCleanBizIndicatorList(baseEntityDTO, lastYear);
        List<BizSummaryDTO> summaryList = summaryList(baseEntityDTO);
        Double normalizeValue = Double.valueOf(0);
        Double denominatorValue = Double.valueOf(0);
        for (CleanBizIndicatorDTO cleanBizIndicatorDTO : list) {
            for (BizSummaryDTO bizSummaryDTO : summaryList) {
                if (ObjectUtil.equal(cleanBizIndicatorDTO.getEnterpriseId(), bizSummaryDTO.getEnterpriseId())) {
                    Double value = NumberUtil.mul(bizSummaryDTO.getRevenueProportion(), bizSummaryDTO.getRevenueShare());
                    Double nValue = NumberUtil.mul(cleanBizIndicatorDTO.getIndicatorValue(), value);
                    normalizeValue = NumberUtil.add(nValue, normalizeValue);
                    denominatorValue = NumberUtil.add(value, denominatorValue);
                    break;
                }
            }
        }
        Double cicsValue = Double.valueOf(0);
        if (NumberUtil.compare(denominatorValue, Double.valueOf(0)) != 0) {
            cicsValue = NumberUtil.div(normalizeValue, denominatorValue);
        }
        CleanIndicatorDTO cleanIndicatorDTO = new CleanIndicatorDTO();
        BeanUtil.copyProperties(baseEntityDTO, cleanIndicatorDTO);
        cleanIndicatorDTO.setIndicatorValue(cicsValue);
        cleanIndicatorDTO.setFieldId(baseFieldDTO.getRightFieldId());
//        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper();
//        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("field_id", baseFieldDTO.getRightFieldId()).eq("quarter", baseEntityDTO.getQuarter()).eq("cics_id", baseEntityDTO.getCicsId());
//        cleanIndicatorDao.delete(queryWrapper);
        cleanIndicatorDTO.setId(null);
        cleanIndicatorDTO.setCreateTime(new Date());
        cleanIndicatorDTO.setUpdateTime(new Date());
        cleanIndicatorDao.insertOrUpdateSingle(cleanIndicatorDTO);   //  唯一值报错

    }

    /**
     * 根据年和cics读取信息
     *
     * @param baseEntityDTO
     * @return
     */
    private List<BizSummaryDTO> summaryList(BaseEntityDTO baseEntityDTO) {
        String year = null;
//        如果是Q4则是当年 否则则是上一年
        if (PUBLIC_QUARTER.equals(baseEntityDTO.getQuarter())) {
            year = baseEntityDTO.getYear();
        } else {
            if (PUBLIC_YEAR.equals(baseEntityDTO.getYear())) {
                year = baseEntityDTO.getYear();
            } else {
                year = String.valueOf(Integer.parseInt(baseEntityDTO.getYear()) - 1);
            }
        }

//        查询行业和年下企业所有业务指标
        QueryWrapper<BizSummaryDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("cics_id", baseEntityDTO.getCicsId()).eq("year", year).eq("is_cics", 1);
        List<BizSummaryDTO> listDTO = bizSummaryService.getEntityList(queryWrapper);

        return listDTO;

    }

    /**
     * 行业指标
     */
    @Override
    public void cicsIndustryIndexCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //获取企业指标需要更新的cics
        BaseFieldDTO baseFieldDTO = fieldList.get(0);
        SysFieldDTO leftField = sysFieldService.getEntity(baseFieldDTO.getLeftFieldId());
        SysFieldDTO rightField = sysFieldService.getEntity(baseFieldDTO.getRightFieldId());

        List<Map<String, Object>> cicsByVersion = cleanIndicatorDao.findCicsByVersion(leftField, rightField);
        for (Map map : cicsByVersion) {
            List<CleanIndicatorDTO> resultList = Lists.newArrayList();
            Integer cics_id = Convert.toInt(map.get("cics_id"));
            //获取某个行业的所有年度和季度数据
            List<CleanIndicatorDTO> cicsIndicator = getCicsIndicatorByFieldAndCics(cics_id, leftField.getId());
            for (int i = 0; i < cicsIndicator.size(); i++) {

                //保存行业指数
                CleanIndicatorDTO industryindex = new CleanIndicatorDTO();

                CleanIndicatorDTO currentQuarterIndicatorDTO = cicsIndicator.get(i);
                CleanIndicatorDTO firstQuaterCleanIndicatorDTO = cicsIndicator.get(0);
                Double currentQuarterClosePrice = currentQuarterIndicatorDTO.getIndicatorValue();
                Double firstQuarterClosePrice = firstQuaterCleanIndicatorDTO.getIndicatorValue();
                if (Objects.isNull(firstQuarterClosePrice)) {
                    break;
                    //TODO 如果数据问题 应暂停数据 抛出异常
                }

                BeanUtil.copyProperties(currentQuarterIndicatorDTO, industryindex);
                industryindex.setId(null);
                if (Objects.nonNull(firstQuarterClosePrice) && BigDecimal.ZERO.compareTo(BigDecimal.valueOf(firstQuarterClosePrice)) == 0) {
                    industryindex.setIndicatorValue(null);
                } else {
                    industryindex.setIndicatorValue(Objects.isNull(currentQuarterClosePrice) ? null : BigDecimal.valueOf(currentQuarterClosePrice).divide(BigDecimal.valueOf(firstQuarterClosePrice), 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100)).doubleValue());
                }

                industryindex.setFieldId(rightField.getId());
                resultList.add(industryindex);
            }
            if (resultList.size() > 0) {
                //删除原行业指数
                deleteCicsIndicatorByFieldAndCics(cics_id, rightField.getId());
                saveBatch(resultList); //todo Field 'indicator_value' doesn't have a default value??
            }
        }
    }

    /**
     * 行业指标收益率计算
     * leftField： 企业季度收盘价
     * rightFieldId：行业指数
     */
    @Override
    public void cicsIndustryIndexYieldCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //获取企业指标需要更新的cics
        BaseFieldDTO baseFieldDTO = fieldList.get(0);
        SysFieldDTO leftField = sysFieldService.getEntity(baseFieldDTO.getLeftFieldId());
        SysFieldDTO rightField = sysFieldService.getEntity(baseFieldDTO.getRightFieldId());
        List<Map<String, Object>> cicsByVersion = cleanIndicatorDao.findCicsByVersion(leftField, rightField);
        for (Map map : cicsByVersion) {
            List<CleanIndicatorDTO> resultList = Lists.newArrayList();
            Integer cics_id = Convert.toInt(map.get("cics_id"));
            //获取某个行业的所有年度和季度数据
            List<CleanIndicatorDTO> cicsIndicator = getCicsIndicatorByFieldAndCics(cics_id, leftField.getId());
            for (int i = 0; i < cicsIndicator.size(); i++) {
                //保存行业指数收益率
                CleanIndicatorDTO industryIndexYield = new CleanIndicatorDTO();
                CleanIndicatorDTO currentQuarterIndicatorDTO = cicsIndicator.get(i);
                BeanUtil.copyProperties(currentQuarterIndicatorDTO, industryIndexYield);
                industryIndexYield.setId(null);
                industryIndexYield.setFieldId(rightField.getId());
                if (i == 0) {
//                    industryIndexYield.setIndicatorValue(null);
                    continue;
                } else {
                    CleanIndicatorDTO lastQuarterIndicatorDTO = cicsIndicator.get(i - 1);
                    Double currentQuarterIndex = currentQuarterIndicatorDTO.getIndicatorValue();
                    Double lastQuarterIndex = lastQuarterIndicatorDTO.getIndicatorValue();
                    if (Objects.isNull(currentQuarterIndex)) {
                        currentQuarterIndex = lastQuarterIndex;
                    }
                    currentQuarterIndicatorDTO.setIndicatorValue(currentQuarterIndex);
                    if (Objects.nonNull(lastQuarterIndex) && lastQuarterIndex != 0) {
                        Double x = currentQuarterIndex / lastQuarterIndex;
                        if (x > 0) {
                            Double log = Math.log(x);
                            BigDecimal round = NumberUtil.round(log, 4);
                            industryIndexYield.setIndicatorValue(round.doubleValue());
                            resultList.add(industryIndexYield);
                        }
                    }
                }

            }
            if (resultList.size() > 0) {
                //删除原行业指数
                deleteCicsIndicatorByFieldAndCics(cics_id, rightField.getId());
                saveBatch(resultList);
            }
        }
    }

    private List<CleanIndicatorDTO> getCicsIndicatorByFieldAndCics(Integer cicsId, Integer fieldId) {
        List orderList = Lists.newArrayList("year", "quarter");
        QueryWrapper<CleanIndicatorDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("cics_id", cicsId)
            .eq("field_id", fieldId)
            .orderByAsc(orderList);
        return list(wrapper);
    }

    private void deleteCicsIndicatorByFieldAndCics(Integer cicsId, Integer fieldId) {
        List orderList = Lists.newArrayList("year", "quarter");
        QueryWrapper<CleanIndicatorDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("cics_id", cicsId)
            .eq("field_id", fieldId);
        deleteEntities(wrapper);
    }

    /**
     * 计数（企业指标->计算企业在季度内数量等（由企业级数据转化为行业级别数据））
     * 行业新上市数量/行业st数量  计数算法(或者count)
     */
    @Override
    public void cicsSumCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        BaseFieldDTO baseFieldDTO = fieldList.get(0);
        Integer leftFieldId = baseFieldDTO.getLeftFieldId();
        Integer rightField = baseFieldDTO.getRightFieldId();
        List<Map<String, Object>> changedList = cleanIndicatorDao.getChangedList(leftFieldId, rightField);

        if (changedList.size() > 0) {
            Integer max_version = changedList.stream().map(a -> Convert.toInt(a.get("max_version"))).max(Integer::compareTo).get();
            //重新生成计数
            List<Map<String, Object>> countByField = cleanIndicatorDao.getCountByField(leftFieldId);
            List<CleanIndicatorDTO> result = countByField.stream().map(a -> convertToCleanIndicatorDTO(max_version, a, rightField)).collect(Collectors.toList());
            QueryWrapper<CleanIndicatorDTO> query = new QueryWrapper();
            query.eq("field_id", rightField);
            deleteEntities(query);
            saveBatch(result);
        }
    }

    /**
     * 根据年度季度
     *
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void cicsNewListingDivideCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //查询大于2015年Q1 之后最大的年度季度
        BaseFieldDTO baseFieldDTO = fieldList.stream().findFirst().get();
        BaseEntityDTO maxYQ = cleanIndicatorDao.findByMaxYQ(baseFieldDTO.getLeftFieldId());
        if (Objects.nonNull(maxYQ) && Objects.nonNull(maxYQ.getYear()) && Objects.nonNull(maxYQ.getQuarter())) {
            Integer maxYear = NumberUtil.parseInt(maxYQ.getYear());
            Integer maxQuarter = NumberUtil.parseInt(maxYQ.getQuarter().replace("Q", ""));
            //查询所有行业
            List<CicsIndustryLevelDTO> allList = cicsIndustryLevelService.getAllList();
            List<Future> futureList = org.apache.commons.compress.utils.Lists.newArrayList();
            for (int year = 2015; year <= maxYear; year++) {
                for (int quarter = 1; quarter <= 4; quarter++) {
                    if (year == maxYear && quarter > maxQuarter) {
                        break;
                    }
                    String thisYear = StrUtil.toString(year);
                    String thisQuarter = "Q" + quarter;
                    ExecutorBuilderUtil.workQueueYield();
                    Future future1 = ExecutorBuilderUtil.pool.submit(() -> getNewTagAndRate(thisYear, thisQuarter, allList, baseFieldDTO));
                    futureList.add(future1);
                }
            }
            FutureGetUtil.futureGet(futureList);
        }

    }

    private void getNewTagAndRate(String thisYear, String thisQuarter, List<CicsIndustryLevelDTO> allList, BaseFieldDTO baseFieldDTO) {
        String maxVersion = cleanBizIndicatorService.findMaxVersion(baseFieldDTO.getLeftFieldId());
        if (Objects.isNull(maxVersion)) {
            maxVersion = "1000";
        }
        Map<Integer, Double> cicsCountMap = new HashMap(allList.size());
        allList.stream().forEach(a -> {
            cicsCountMap.put(a.getId(), 0D);
        });
        Integer listedCount = 0;
        List<CleanBizIndicatorDTO> byYearQuarter = cleanBizIndicatorService.findByYearQuarter(thisYear, thisQuarter, baseFieldDTO.getLeftFieldId());
        if (byYearQuarter.size() > 0) {

            List<CleanBizIndicatorDTO> listedList = byYearQuarter.stream().filter(a -> NumberUtil.equals(a.getIndicatorValue(), 1D)).collect(Collectors.toList());
            //当季度上市企业业总数量
            listedCount = listedList.size();
            for (CleanBizIndicatorDTO cleanBizIndicatorDTO : listedList) {
                //获取企业首次拥有主营数据的行业
                List<BizSummaryDTO> cicsFistByEnterprise = bizSummaryService.findCicsFistByEnterprise(cleanBizIndicatorDTO.getEnterpriseId(), cleanBizIndicatorDTO.getYear());
                for (BizSummaryDTO bizSummaryDTO : cicsFistByEnterprise) {
                    Integer cicsId = bizSummaryDTO.getCicsId();
                    if (Objects.nonNull(cicsId)) {
                        Double aDouble = cicsCountMap.get(cicsId);
                        if (Objects.nonNull(aDouble)) {
                            Double count = aDouble + 1D;
                            cicsCountMap.put(cicsId, count);
                        }
                    }
                }
            }
        }
        List<CleanIndicatorDTO> result = Lists.newArrayList();
        String finalMaxVersion = maxVersion;
        Integer finalListedCount = listedCount;
        cicsCountMap.forEach((key, value) -> {
            //行业新上市总数量--中间数据写死了fieldID
            CleanIndicatorDTO cleanIndicatorSum = new CleanIndicatorDTO();
            cleanIndicatorSum.setYear(thisYear);
            cleanIndicatorSum.setQuarter(thisQuarter);
            cleanIndicatorSum.setFieldId(3024);
            cleanIndicatorSum.setCicsId(key);
            cleanIndicatorSum.setVersion(NumberUtil.parseInt(finalMaxVersion));
            cleanIndicatorSum.setIndicatorValue(value);
            cleanIndicatorSum.setCreateTime(new Date());
            cleanIndicatorSum.setUpdateTime(new Date());
            result.add(cleanIndicatorSum);
            //行业新上市占比
            CleanIndicatorDTO cleanIndicatorRate = new CleanIndicatorDTO();
            cleanIndicatorRate.setYear(thisYear);
            cleanIndicatorRate.setQuarter(thisQuarter);
            cleanIndicatorRate.setFieldId(baseFieldDTO.getRightFieldId());
            cleanIndicatorRate.setVersion(NumberUtil.parseInt(finalMaxVersion));
            if (finalListedCount > 0) {
                cleanIndicatorRate.setIndicatorValue(NumberUtil.div(value, finalListedCount, 4).doubleValue());
            } else {
                cleanIndicatorRate.setIndicatorValue(0D);
            }
            cleanIndicatorRate.setCicsId(key);
            cleanIndicatorRate.setCreateTime(new Date());
            cleanIndicatorRate.setUpdateTime(new Date());

            result.add(cleanIndicatorRate);
        });
        //删除相应字段当季度数据
        QueryWrapper<CleanIndicatorDTO> cleanIndicatorDTOQueryWrapper = new QueryWrapper<>();
        cleanIndicatorDTOQueryWrapper.eq("year", thisYear);
        cleanIndicatorDTOQueryWrapper.eq("quarter", thisQuarter);
        cleanIndicatorDTOQueryWrapper.eq("field_id", 3024);//企业新上市数量
        deleteEntities(cleanIndicatorDTOQueryWrapper);
        QueryWrapper<CleanIndicatorDTO> cleanIndicatorDTOQueryRateWrapper = new QueryWrapper<>();
        cleanIndicatorDTOQueryRateWrapper.eq("year", thisYear);
        cleanIndicatorDTOQueryRateWrapper.eq("quarter", thisQuarter);
        cleanIndicatorDTOQueryRateWrapper.eq("field_id", baseFieldDTO.getRightFieldId());//企业新上市企业占比
        deleteEntities(cleanIndicatorDTOQueryRateWrapper);
        //保存
        List<CleanIndicatorDTO> collect = result.stream().filter(a -> a.getIndicatorValue() > 0).collect(Collectors.toList());
//        System.out.println(collect.size());
//        saveBatch(result);
        for (CleanIndicatorDTO cleanIndicatorDTO : result) {
            save(cleanIndicatorDTO);
        }
    }

    /**
     * map转化为CleanIndicatorDTO
     *
     * @param a
     * @return
     */
    private CleanIndicatorDTO convertToCleanIndicatorDTO(Map<String, Object> a, Integer fieldId, int i) {
        CleanIndicatorDTO result = new CleanIndicatorDTO();
        result.setIndicatorValue(Convert.toDouble(a.get("count")));
        result.setCicsId(Convert.toInt(a.get("cics_id")));
        result.setYear(Convert.toStr(a.get("year")));
        result.setQuarter("Q" + i);
        result.setVersion(Convert.toInt(a.get("max_version")));
        result.setFieldId(fieldId);
        return result;
    }

    /**
     * map转化为CleanIndicatorDTO
     *
     * @param a
     * @return
     */
    private CleanIndicatorDTO convertToCleanIndicatorDTO(Integer maxVersion, Map<String, Object> a, Integer fieldId) {
        CleanIndicatorDTO result = new CleanIndicatorDTO();
        result.setIndicatorValue(Convert.toDouble(a.get("count")));
        result.setCicsId(Convert.toInt(a.get("cics_id")));
        result.setYear(Convert.toStr(a.get("year")));
        result.setQuarter(Convert.toStr(a.get("quarter")));
        result.setVersion(Convert.toInt(maxVersion));
        result.setFieldId(fieldId);
        return result;
    }

    /**
     * 企业所属转换为当季度行业企业数量
     */
    @Override
    public void cicsEnterpriseCountCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Map<String, Object>> enterpriseChangeList = cleanIndicatorDao.getEnterpriseChangeList(rightFieldId);
        if (enterpriseChangeList.size() > 0) {
            List<Map<String, Object>> enterpriseCount = enterpriseDao.getEnterpriseCount();
            QueryWrapper<CleanIndicatorDTO> query = new QueryWrapper();
            query.eq("field_id", rightFieldId);
            deleteEntities(query);
            List<Future> futureList = Lists.newArrayList();
            for (int i = 0; i < 4; i++) {
                int finalI = i + 1;
                ExecutorBuilderUtil.workQueueYield();
                Future future1 = ExecutorBuilderUtil.pool.submit(() -> savecicsEnterpriseCount(enterpriseCount, rightFieldId, finalI));
                futureList.add(future1);
            }
            FutureGetUtil.futureGet(futureList);
        }
    }


    private void savecicsEnterpriseCount(List<Map<String, Object>> enterpriseCount, Integer rightFieldId, int finalI) {
        List<CleanIndicatorDTO> result = enterpriseCount.stream().map(a -> convertToCleanIndicatorDTO(a, rightFieldId, finalI)).collect(Collectors.toList());
        saveBatch(result);
    }

    /**
     * 计算行业季度数据表中相互之间得比例算法
     */
    @Override
    public void cicsIndicatorDivide(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        Integer resultSysFieldId = rightFieldId;
        Integer divisorSysFieldId = enumFieldLeft(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.DIVISOR_SYSFIELD.name());
        Integer dividendSysFieldId = enumFieldLeft(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.DIVIDEND_SYSFIELD.name());
        SysFieldDTO divisorSysFieldDTO = sysFieldService.getEntity(divisorSysFieldId);
        SysFieldDTO dividendSysFieldDTO = sysFieldService.getEntity(dividendSysFieldId);
        SysFieldDTO resultSysFieldDTO = sysFieldService.getEntity(resultSysFieldId);
        List<SysFieldDTO> leftList = new ArrayList<>();
        // leftList.add(divisorSysFieldDTO);
        leftList.add(dividendSysFieldDTO);


        List<Map<String, Object>> enterpriseChangeList = cleanIndicatorDao.getChangeListBymultiFiled(divisorSysFieldDTO, leftList, resultSysFieldDTO);
        if (enterpriseChangeList.size() > 0) {
            List<CleanIndicatorDTO> result = enterpriseChangeList.stream()
                .filter(a -> Objects.nonNull(a.get(divisorSysFieldDTO.getCode() + "_version")) && Objects.nonNull(a.get(dividendSysFieldDTO.getCode() + "_version")))
                .map(a -> convertMultiMapToCleanIndicatorDTOByDevide(a, divisorSysFieldDTO, dividendSysFieldDTO, resultSysFieldDTO)).collect(Collectors.toList());
            List<Future> futureList = Lists.newArrayList();
            for (CleanIndicatorDTO cleanBizIndicatorDTO : result) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() ->
//                    saveOrUpdate(cleanBizIndicatorDTO));
                    cicsIndicatorDao.insertOrUpdate(cleanBizIndicatorDTO));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }
    }

    @Override
    public void cicsSort(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //获取cics 有变动的年度/季度
        BaseFieldDTO baseFieldDTO = fieldList.stream().findFirst().get();
        SysFieldDTO leftSysFieldDTO = sysFieldService.getEntity(baseFieldDTO.getLeftFieldId());
        SysFieldDTO resultSysFieldDTO = sysFieldService.getEntity(baseFieldDTO.getRightFieldId());
        List<Map<String, Object>> enterpriseChangeList = cleanIndicatorDao.getChangeListBymultiFiled(leftSysFieldDTO, Lists.newArrayList(), resultSysFieldDTO);
        List<Map<String, Object>> collect = enterpriseChangeList.stream().map(a -> {
            Map<String, Object> yearMap = new HashMap();
            yearMap.put("year", a.get("year"));
            yearMap.put("quarter", a.get("quarter"));
            return yearMap;
        }).distinct().collect(Collectors.toList());
        //获取相应年度/季度的所有指标并排序

        for (Map<String, Object> yearMap : collect) {
            QueryWrapper<CleanIndicatorDTO> query = new QueryWrapper<>();
            query.eq("year", Convert.toStr(yearMap.get("year")));
            query.eq("quarter", Convert.toStr(yearMap.get("quarter")));
            query.eq("field_id", baseFieldDTO.getLeftFieldId());
            query.orderByAsc("indicator_value");
            List<CleanIndicatorDTO> list = list(query);
            int size = list.size();
            Integer remarkable = size / 3;
            List<CleanIndicatorDTO> result = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                CleanIndicatorDTO cleanIndicatorDTO = list.get(i);
                cleanIndicatorDTO.setId(null);
                cleanIndicatorDTO.setIndicatorValue(0.0);
                cleanIndicatorDTO.setFieldId(baseFieldDTO.getRightFieldId());
                if (i < remarkable) {
                    cleanIndicatorDTO.setIndicatorValue(1.0);
                }
                result.add(cleanIndicatorDTO);
            }
            if (result.size() > 0) {
                QueryWrapper<CleanIndicatorDTO> delQuery = new QueryWrapper<>();
                delQuery.eq("year", Convert.toStr(yearMap.get("year")));
                delQuery.eq("quarter", Convert.toStr(yearMap.get("quarter")));
                delQuery.eq("field_id", baseFieldDTO.getRightFieldId());
                deleteEntities(delQuery);
                saveBatch(result);
            }
        }
    }

    @Override
    public List<CleanIndicatorDTO> findByCicsYearMQ(Integer cicsId, Integer fieldId, String yearMq) {
        return cicsIndicatorDao.findByCicsYearMQ(cicsId, fieldId, yearMq);
    }

    @Override
    public List<CleanIndicatorDTO> findByCicsYearMQTWO(Integer cicsId, Integer fieldId, List<String> yearMq) {
        return cicsIndicatorDao.findByCicsYearMQTWO(cicsId, fieldId, yearMq);
    }

    @Override
    public List<BaseEntityDTO> findByCicsYear(List<Integer> cicsIds, Integer fieldId, String year) {
        return cicsIndicatorDao.findByCicsYear(cicsIds, fieldId, year);
    }

    @Override
    public int findByCicsYearMQCount(Integer cicsId, Integer fieldId, List<String> yearMq) {
        return cicsIndicatorDao.findByCicsYearMQCount(cicsId, fieldId, yearMq);
    }

    @Override
    public List<CleanIndicatorDTO> findByCicsYearAll(Integer cicsId, Integer fieldId, String year) {
        return cicsIndicatorDao.findByCicsYearAll(cicsId, fieldId, year);
    }

    @Override
    public BaseEntityDTO getMinYearQuarter(Integer fieldId) {
        return cleanIndicatorDao.getMinYearQuarter(fieldId);
    }

    @Override
    public List<BaseEntityDTO> getGroupYearQuarter(Integer fieldId) {
        return cleanIndicatorDao.getGroupYearQuarter(fieldId);
    }

    @Override
    public void complementCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<CicsIndustryLevelDTO> levelList = cicsIndustryLevelService.getEntityList(new QueryWrapper());
        QueryWrapper<CleanIndicatorDTO> wrapper = new QueryWrapper<>();
        wrapper.orderByDesc("year", "quarter");
        CleanIndicatorDTO indicatorDTO = getSingleEntity(wrapper);
        if (!BeanUtil.isEmpty(indicatorDTO)) {
            for (BaseFieldDTO baseFieldDTO : fieldList) {
                QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
                queryWrapper.eq("field_id", baseFieldDTO.getLeftFieldId()).select("year", "quarter", "cics_id", "version");
                List<CleanIndicatorDTO> list = getEntityList(queryWrapper);
                if (list.size() == 0) {
                    continue;
                }
                Integer maxVersion = list.stream().max(Comparator.comparingInt(a -> a.getVersion())).get().getVersion();
                List<String> stringList = list.stream().map(a -> a.getCicsId() + "-" + a.getYear() + a.getQuarter()).collect(Collectors.toList());
                List<String> yearMQList = DateYearMqUtils.inspectYear("2015Q1", indicatorDTO.getYear() + indicatorDTO.getQuarter());
                List<Future> futureList = Lists.newArrayList();
                for (String key : yearMQList) {
                    for (CicsIndustryLevelDTO levelDTO : levelList) {
                        if (!stringList.contains(levelDTO.getId() + "-" + key)) {
                            ExecutorBuilderUtil.workQueueYield();
                            Future future = ExecutorBuilderUtil.pool.submit(() -> complementInsert(baseFieldDTO.getLeftFieldId(), Double.valueOf(0), levelDTO.getId(), key, maxVersion));
                            futureList.add(future);
                        }
                    }
                }
                FutureGetUtil.futureGet(futureList);
            }
        }
    }

    @Override
    public void maxValueCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        for (BaseFieldDTO baseFieldDTO : fieldList) {
            //年度季度
            BaseEntityDTO maxYQ = cleanIndicatorDao.findCicsByMaxYQ(baseFieldDTO.getLeftFieldId());
            if (BeanUtil.isEmpty(maxYQ)) {
                return;
            }
            List<String> yearMQList = DateYearMqUtils.inspectYear("2015Q1", maxYQ.getYear() + maxYQ.getQuarter());
            List<Future> futureList = Lists.newArrayList();
            //根据季度处理数据
            for (String key : yearMQList) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() -> maxValueInsert(baseFieldDTO, key));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }
    }

    private void maxValueInsert(BaseFieldDTO baseFieldDTO, String key) {
        String year = StrUtil.sub(key, 0, 4);
        String mq = StrUtil.sub(key, 4, 6);
        //根据年度季度和fieldId 获取list
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", year);
        queryWrapper.eq("quarter", mq);
        queryWrapper.eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> list = getEntityList(queryWrapper);
        List<CleanIndicatorDTO> results = Lists.newArrayList();
        Double maxValue = list.stream().max(Comparator.comparingDouble(a -> a.getIndicatorValue())).get().getIndicatorValue();
        for (CleanIndicatorDTO cleanIndicatorDTO : list) {
            CleanIndicatorDTO result = new CleanIndicatorDTO();
            BeanUtil.copyProperties(cleanIndicatorDTO, result);
            result.setId(null);
            result.setFieldId(baseFieldDTO.getRightFieldId());
            result.setCreateTime(new Date());
            result.setUpdateTime(new Date());
            result.setIndicatorValue(maxValue - cleanIndicatorDTO.getIndicatorValue());

            results.add(result);
        }
//        QueryWrapper<CleanIndicatorDTO> queryRight = new QueryWrapper<>();
//        queryRight.eq("year", year);
//        queryRight.eq("quarter", mq);
//        queryRight.eq("field_id", baseFieldDTO.getRightFieldId());
//        deleteEntities(queryRight);
        //
        for (CleanIndicatorDTO result : results) {
            cicsIndicatorDao.insertOrUpdate(result);
        }
//        deleteEntities(queryRight);
//       saveBatch(results);
    }

    @Override
    public List<CleanIndicatorDTO> findByLevelList(String year, String quarter, Integer fieldId) {
        return cleanIndicatorDao.findByLevelList(year, quarter, fieldId);
    }

    private void complementInsert(Integer fieldId, Double value, Integer cicsId, String key, Integer version) {
        String year = StrUtil.sub(key, 0, 4);
        String mq = StrUtil.sub(key, 4, 6);
        CleanIndicatorDTO cleanIndicatorDTO = new CleanIndicatorDTO();
        cleanIndicatorDTO.setIndicatorValue(value);
        cleanIndicatorDTO.setFieldId(fieldId);
        cleanIndicatorDTO.setCicsId(cicsId);
        cleanIndicatorDTO.setVersion(version);
        cleanIndicatorDTO.setYear(year);
        cleanIndicatorDTO.setQuarter(mq);
        cleanIndicatorDTO.setCreateTime(new Date());
        cleanIndicatorDTO.setUpdateTime(new Date());
        cleanIndicatorDTO.setId(null);
        cleanIndicatorDao.insert(cleanIndicatorDTO);
    }

    /**
     * map 固定字段转换
     *
     * @param map
     * @return
     */
    private CleanIndicatorDTO convertMapToCleanIndicatorDTO(Map map, SysFieldDTO sysFieldDTO) {
        CleanIndicatorDTO result = new CleanIndicatorDTO();
        result.setId(Convert.toLong(map.get("id")));//如果是null 则保存null
        result.setCicsId(Convert.toInt(map.get("cics_id")));
        result.setYear(Convert.toStr(map.get("year")));
        result.setQuarter(Convert.toStr(map.get("quarter")));
        result.setFieldId(sysFieldDTO.getId());
        result.setVersion(Convert.toInt(map.get(sysFieldDTO.getCode() + "_version")));//根据field 生成相应字段version
        result.setIndicatorValue(Convert.toDouble(map.get(sysFieldDTO.getCode())));//根据field 生成相应字段version
        return result;
    }


    /**
     * map 固定字段转换
     * 双字段相除得到第三个字段
     *
     * @param map
     * @return
     */
    private CleanIndicatorDTO convertMultiMapToCleanIndicatorDTOByDevide(Map map, SysFieldDTO divisorSysFieldDTO, SysFieldDTO dividendSysFieldDTO, SysFieldDTO resultSysFieldDTO) {
        CleanIndicatorDTO result = new CleanIndicatorDTO();
        //如果是null 则保存null
        result.setId(Convert.toLong(map.get("id")));
        result.setCicsId(Convert.toInt(map.get("cics_id")));
        result.setYear(Convert.toStr(map.get("year")));
        result.setQuarter(Convert.toStr(map.get("quarter")));
        Double divisor = Convert.toDouble(map.get(divisorSysFieldDTO.getCode()));
        Double dividend = Convert.toDouble(map.get(dividendSysFieldDTO.getCode()));
        if (Objects.nonNull(divisor) && Objects.nonNull(dividend) && NumberUtil.compare(dividend, 0D) != 0) {
            result.setIndicatorValue(divisor / dividend);//根据field 生成相应字段version
        } else {
            result.setIndicatorValue(0D);
        }
        Integer maxVersion = Convert.toInt(map.get(divisorSysFieldDTO.getCode() + "_version")) >= Convert.toInt(map.get(dividendSysFieldDTO.getCode() + "_version")) ? Convert.toInt(map.get(divisorSysFieldDTO.getCode() + "_version")) : Convert.toInt(map.get(dividendSysFieldDTO.getCode() + "_version"));
        result.setVersion(maxVersion);//根据field 生成相应字段version
        result.setFieldId(resultSysFieldDTO.getId());
        return result;
    }

    private void standardTwo(List<BaseEntityDTO> list, List<BaseFieldDTO> fieldList) {
        Map<String, List<CicsSummaryDTO>> map = Maps.newConcurrentMap();
        Map<String, Double> pijMap = Maps.newConcurrentMap();
        Map<String, Double> addMap = Maps.newConcurrentMap();
        Map<String, Double> subMap = Maps.newConcurrentMap();
        Map<String, Double> qzMap = Maps.newConcurrentMap();
        for (BaseEntityDTO baseEntityDTO : list) {
            //查询分组
            BaseEntityDTO summaryEntityDTO = DateYearMqUtils.summaryLast(baseEntityDTO);
            String key = summaryEntityDTO.getYear() + summaryEntityDTO.getQuarter();
            if (!map.containsKey(key)) {
                QueryWrapper<CicsSummaryDTO> queryWrapper = new QueryWrapper<>();
                queryWrapper.eq("year", summaryEntityDTO.getYear()).eq("quarter", summaryEntityDTO.getQuarter())
                    .isNotNull("assets_type").isNotNull("growth_type");
                List<CicsSummaryDTO> summaryList = cicsSummaryService.getEntityList(queryWrapper);
                map.put(key, summaryList);
            }
            List<CicsSummaryDTO> summaryList = map.get(key);
            Map<String, List<CicsSummaryDTO>> summaryMap = summaryList.stream().collect(Collectors.groupingBy(a -> a.getAssetsType() + "" + a.getGrowthType()));
            for (BaseFieldDTO baseFieldDTO : fieldList) {
                standardTwoBZH(summaryMap, baseFieldDTO, baseEntityDTO, pijMap, addMap, subMap);
            }
            if (Objects.equals(baseEntityDTO.getQuarter(), PublicEnum.PUBLIC_QUARTER)) {
                standardTwoQZ(pijMap, baseEntityDTO, fieldList, qzMap);
            }
            standardTwoInsert(summaryMap, summaryEntityDTO, baseEntityDTO, fieldList, qzMap, addMap, subMap);

        }
    }

    private void standardTwoInsert(Map<String, List<CicsSummaryDTO>> summaryMap, BaseEntityDTO summaryEntityDTO, BaseEntityDTO baseEntityDTO, List<BaseFieldDTO> fieldList, Map<String, Double> qzMap, Map<String, Double> addMap, Map<String, Double> subMap) {
        Map<Integer, Double> map = Maps.newConcurrentMap();
        int size = 0;
        for (BaseFieldDTO baseFieldDTO : fieldList) {
            String key = summaryEntityDTO.getYear() + summaryEntityDTO.getQuarter() + baseFieldDTO.getLeftFieldId();
            if (qzMap.containsKey(key)) {
                map.put(baseFieldDTO.getLeftFieldId(), qzMap.get(key));
                size += 1;
            }
        }
        boolean status = false;
        if (size == fieldList.size()) {
            status = true;
        }
        for (List<CicsSummaryDTO> summaryList : summaryMap.values()) {
            List<Integer> sList = summaryList.stream().map(a -> a.getCicsId()).collect(Collectors.toList());

            standardTwoSCR(sList, fieldList, baseEntityDTO, map, addMap, subMap, status);
        }
    }


    private void standardTwoQZ(Map<String, Double> pijMap, BaseEntityDTO baseEntityDTO, List<BaseFieldDTO> list, Map<String, Double> qzMap) {
        Double v1 = Double.valueOf(0);
        for (BaseFieldDTO baseFieldDTO : list) {
            String key = baseEntityDTO.getYear() + baseEntityDTO.getQuarter() + baseFieldDTO.getLeftFieldId();
            if (pijMap.containsKey(key)) {
                Double z1 = pijMap.get(key);
                Double z2 = 1 - z1;
                v1 += z2;
            }
        }
        if(Objects.equals(baseEntityDTO.getYear(),"2022")){
            System.out.println("v6:"+v1);
        }
        for (BaseFieldDTO baseFieldDTO : list) {
            String key = baseEntityDTO.getYear() + baseEntityDTO.getQuarter() + baseFieldDTO.getLeftFieldId();
            if (pijMap.containsKey(key)) {
                Double z1 = pijMap.get(key);
                Double z2 = 1 - z1;
                Double z3 = Double.valueOf(0);
                if (NumberUtil.compare(v1, 0) != 0) {
                    z3 = z2 / v1;
                }
                qzMap.put(key, z3);
            }
        }
    }

    private void standardTwoBZH(Map<String, List<CicsSummaryDTO>> summaryMap, BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO, Map<String, Double> pijMap, Map<String, Double> addMap, Map<String, Double> subMap) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", baseFieldDTO.getLeftFieldId());
        List<CleanIndicatorDTO> list = getEntityList(queryWrapper);
        List<CleanIndicatorDTO> fList = Lists.newArrayList();
        for (List<CicsSummaryDTO> summaryList : summaryMap.values()) {
            List<Integer> sList = summaryList.stream().map(a -> a.getCicsId()).collect(Collectors.toList());
            List<CleanIndicatorDTO> gList = list.stream().filter(a -> sList.contains(a.getCicsId())).collect(Collectors.toList());
            standardTwoBZHInsert(gList, baseFieldDTO, baseEntityDTO, addMap, subMap, fList);
        }
        if (Objects.equals(baseEntityDTO.getQuarter(), PublicEnum.PUBLIC_QUARTER)) {
            standardPIJ(fList, baseEntityDTO, baseFieldDTO, pijMap);
        }

    }

    private void standardPIJ(List<CleanIndicatorDTO> fList, BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO, Map<String, Double> map) {
        if (fList.size() > 0) {
            Double minV = fList.stream().min(Comparator.comparingDouble(a -> a.getIndicatorValue())).get().getIndicatorValue();
            Double maxV = fList.stream().max(Comparator.comparingDouble(a -> a.getIndicatorValue())).get().getIndicatorValue();
            Double subV = NumberUtil.sub(maxV, minV);
            AtomicReference<Double> v1 = new AtomicReference<>(Double.valueOf(0));
            fList.forEach(a -> {
                if (NumberUtil.compare(subV, 0) != 0) {
                    Double z1 = a.getIndicatorValue() - minV;
                    Double z2 = z1 / subV;
                    a.setIndicatorValue(z2);
                    v1.updateAndGet(v -> v + z2);
                }
            });
            Double v2 = v1.get();
            Double v3 = Double.valueOf(0);
            for (CleanIndicatorDTO indicatorDTO : fList) {
                if (NumberUtil.compare(v2, 0) != 0) {
                    Double z1 = NumberUtil.div(indicatorDTO.getIndicatorValue(), v2);
                    if (NumberUtil.compare(z1, 0) != 0) {
                        Double z2 = Math.log(z1) * z1;
                        v3 += z2;
                    }
                }
            }
            Double v4 = -1 / Math.log(10);
            Double v5 = v4 * v3;
            if(Objects.equals(baseEntityDTO.getYear(),"2022")){
                System.out.println(baseFieldDTO.getLeftFieldId()+":v3:"+v3);
                System.out.println(baseFieldDTO.getLeftFieldId()+":v4:"+v4);
                System.out.println(baseFieldDTO.getLeftFieldId()+":v5:"+v5);
            }
            map.put(baseEntityDTO.getYear() + baseEntityDTO.getQuarter() + baseFieldDTO.getLeftFieldId(), v5);
        }
    }


    /**
     * 标准化计算并写入
     *
     * @param list
     */
    private void standardTwoBZHInsert(List<CleanIndicatorDTO> list, BaseFieldDTO baseFieldDTO, BaseEntityDTO baseEntityDTO, Map<String, Double> addMap, Map<String, Double> subMap, List<CleanIndicatorDTO> fList) {
        if (list.size() > 0) {
            Double sumValue = Double.valueOf(0);
            //求所有数据二次方之和: 3-6
            for (CleanIndicatorDTO cleanIndicatorDTO : list) {
                sumValue += NumberUtil.mul(cleanIndicatorDTO.getIndicatorValue(), cleanIndicatorDTO.getIndicatorValue());
            }
//            平方根处理:3-6
            Double finalSumValue = Math.sqrt(sumValue);
//            设置标准化值：3-6
            list.forEach(a -> {
                if (NumberUtil.compare(finalSumValue, Double.valueOf(0)) != 0) {
                    a.setIndicatorValue(NumberUtil.div(a.getIndicatorValue(), finalSumValue));
                } else {
                    a.setIndicatorValue(Double.valueOf(0));
                }

            });
            fList.addAll(list);
//        获取最小值
            Double minValue = list.stream().min(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//        获取最大值
            Double maxValue = list.stream().max(Comparator.comparing(CleanIndicatorDTO::getIndicatorValue)).get().getIndicatorValue();
//            获取最大版本
            Integer version = list.stream().max(Comparator.comparing(CleanIndicatorDTO::getVersion)).get().getVersion();
            baseEntityDTO.setVersion(version);
//        求每一个行业字段劣距和优距值
            list.forEach(a -> {
                addMap.put(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + a.getCicsId(), NumberUtil.sub(a.getIndicatorValue(), maxValue));
                subMap.put(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + a.getCicsId(), NumberUtil.sub(a.getIndicatorValue(), minValue));
            });
//        标准化指标值
            List<Future> futureList = Lists.newArrayList();
            for (CleanIndicatorDTO cleanIndicatorDTO : list) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() -> insertToTopsis(baseEntityDTO, baseFieldDTO.getRightFieldId(), cleanIndicatorDTO.getIndicatorValue(), cleanIndicatorDTO.getCicsId()));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }

    }


    /**
     * 获取指标值和最优距和最劣距值
     *
     * @param cicsList
     * @param fieldList
     */
    private void standardTwoSCR(List<Integer> cicsList, List<BaseFieldDTO> fieldList, BaseEntityDTO entityDTO, Map<Integer, Double> qzMap, Map<String, Double> addMap, Map<String, Double> subMap, boolean status) {
        BaseEntityDTO baseEntityDTO = new BaseEntityDTO();
        baseEntityDTO = entityDTO;
        List<CleanIndicatorDTO> list = Lists.newArrayList();
        for (Integer cicsId : cicsList) {
            ExecutorBuilderUtil.incrementAndGet(50, true);
//            最优距之和
            Double addValue = Double.valueOf(0);
//            最劣距之和
            Double subValue = Double.valueOf(0);
//            循环字段
            for (BaseFieldDTO baseFieldDTO : fieldList) {

//                获取当前字段和行业值
                Double getAddValue = addMap.get(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + cicsId);
                Double getSubValue = subMap.get(baseEntityDTO.getYear() + ":" + baseEntityDTO.getQuarter() + ":" + baseFieldDTO.getLeftFieldId() + ":" + cicsId);
//                获取优距劣距二次方值
                Double getAddValueSq = NumberUtil.mul(getAddValue, getAddValue);
                Double getSubValueSq = NumberUtil.mul(getSubValue, getSubValue);
                if (cicsId == 240 && Objects.equals(baseEntityDTO.getYear(), "2023") && Objects.equals(baseEntityDTO.getQuarter(), "Q1")) {
                    System.out.println("getSubValue:" + getSubValue);
                    System.out.println("getSubValueSq:" + getSubValueSq);
                    System.out.println("qzMap:"+qzMap.get(baseFieldDTO.getLeftFieldId()));
                    if (!status) {
                        System.out.println("one-subValue:" + NumberUtil.div(getSubValueSq, Double.valueOf(fieldList.size())));
                    } else {
                        System.out.println("two-subValue:" + NumberUtil.mul(getSubValueSq, qzMap.get(baseFieldDTO.getLeftFieldId())));
                    }

                }
//                写入优距劣距之和字段
                if (!status) {
                    addValue += NumberUtil.div(getAddValueSq, Double.valueOf(fieldList.size()));
                    subValue += NumberUtil.div(getSubValueSq, Double.valueOf(fieldList.size()));
                } else {
                    addValue += NumberUtil.mul(getAddValueSq, qzMap.get(baseFieldDTO.getLeftFieldId()));
                    subValue += NumberUtil.mul(getSubValueSq, qzMap.get(baseFieldDTO.getLeftFieldId()));
                }


            }

//            优距劣距平方根算最劣距，最优距值
            Double radicalAddValue = Math.sqrt(addValue);
            Double radicalSubValue = Math.sqrt(subValue);
            if (radicalAddValue.isNaN() || radicalSubValue.isNaN()) {
                continue;
            }
            Double radicalAddAndSubValue = Double.valueOf(0);
            if (radicalSubValue != 0) { // Character N is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
                radicalAddAndSubValue = NumberUtil.add(radicalAddValue, radicalSubValue);
            }
//            行业指标值
            Double srcValue = Double.valueOf(0);
            if (radicalAddAndSubValue != 0) {
                srcValue = NumberUtil.div(radicalSubValue, radicalAddAndSubValue);
            }

//            删除写入
            insertToTopsis(baseEntityDTO, addFieldIdCalculation, radicalAddValue, cicsId);
//            删除写入
            insertToTopsis(baseEntityDTO, subFieldIdCalculation, radicalSubValue, cicsId);
//            删除写入


            insertToTopsisList(baseEntityDTO, scrUnitFieldIdCalculation, srcValue, cicsId, list);
            ExecutorBuilderUtil.decrementAndGet();
        }


//        行业所属分组内排行
        topsisCicsSort(list, baseEntityDTO);
    }

}
