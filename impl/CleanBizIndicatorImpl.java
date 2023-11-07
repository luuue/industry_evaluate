package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.chilunyc.process.dao.enterprise.CleanBizIndicatorDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizCompressDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizDefectDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSortDTO;
import com.chilunyc.process.entity.DTO.system.SysFieldDTO;
import com.chilunyc.process.entity.ENUM.PublicEnum;
import com.chilunyc.process.service.enterprise.BizIndicatorService;
import com.chilunyc.process.service.enterprise.CleanBizCompressService;
import com.chilunyc.process.service.enterprise.CleanBizDefectService;
import com.chilunyc.process.service.enterprise.CleanBizIndicatorService;
import com.chilunyc.process.service.industry.CicsSortService;
import com.chilunyc.process.service.system.SysFieldService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("cleanBizIndicatorImpl")
public class CleanBizIndicatorImpl extends BaseServiceImpl<CleanBizIndicatorDao, CleanBizIndicatorDTO> implements CleanBizIndicatorService {

    @Autowired
    private CicsSortService cicsSortService;
    @Autowired
    private CleanBizIndicatorDao cleanBizIndicatorDao;
    @Autowired
    SysFieldService sysFieldService;
    @Autowired
    private BizIndicatorService bizIndicatorService;
    @Autowired
    private CleanBizDefectService cleanBizDefectService;
    @Autowired
    private CleanBizCompressService cleanBizCompressService;
    private final String PUBLIC_QUARTER = "Q4";

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<Future> futureList = Lists.newArrayList();
        for (BaseFieldDTO fieldDTO : fieldList) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> findByCicsCleanYQ(fieldDTO));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }

    private void findByCicsCleanYQ(BaseFieldDTO fieldDTO) {
//        获取排序表最小年份
        Map<String, String> map = cicsSortService.findByCicsCLeanMinYear(fieldDTO.getLeftFieldId());
        if (Objects.nonNull(map)) {
            //            读取按照年，季度，行业维度加工数据和企业行业排序新增数据
            baseAllList(fieldDTO, map);

        }
    }

    private void baseAllList(BaseFieldDTO fieldDTO, Map<String, String> map) {
        List<String> yearList = inspectYear(map.get("minYear"), map.get("maxYear"));
        for (String yearMQ : yearList) {
            String year = StrUtil.sub(yearMQ, 0, 4);
            String quarter = StrUtil.sub(yearMQ, 4, 6);
            BaseEntityDTO entityDTO = new BaseEntityDTO();
            entityDTO.setYear(year);
            entityDTO.setQuarter(quarter);
            List<BaseEntityDTO> baseList = cicsSortService.findByCicsCleanYQ(fieldDTO, entityDTO);
            for (BaseEntityDTO baseEntityDTO : baseList) {
                findByCicsSort(baseEntityDTO, fieldDTO, map.get("year"));
                defectInsert(baseEntityDTO, fieldDTO.getRightFieldId());
            }
        }
    }

    private void defectInsert(BaseEntityDTO baseEntityDTO, Integer fieldId) {
        long count = cleanBizIndicatorDao.findByEnterpriseBizListCount(fieldId, baseEntityDTO.getYear(), baseEntityDTO.getQuarter());

        //todo 超时 findByEnterpriseListCount
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
        defectDTO.setProcessingMissingRate(v1);
        defectDTO.setUpdateTime(new Date());
        defectDTO.setVersion(1000);
        if (BeanUtil.isEmpty(cleanBizDefectDTO)) {
            defectDTO.setCreateTime(new Date());
            cleanBizDefectService.createEntity(defectDTO);
        } else {
            defectDTO.setId(cleanBizDefectDTO.getId());
            cleanBizDefectService.updateEntity(defectDTO);
        }
    }

    @Override
    public List<BaseEntityDTO> findByCleanYQ(BaseFieldDTO baseFieldDTO) {
        return cleanBizIndicatorDao.findByCleanYQ(baseFieldDTO);
    }

    @Override
    public List<BaseEntityDTO> findByCleanYQCics(BaseFieldDTO baseFieldDTO, String year, String quarter, String lastYear) {
        return cleanBizIndicatorDao.findByCleanYQCics(baseFieldDTO, year, quarter, lastYear);
    }

    @Override
    public List<CleanBizIndicatorDTO> findByCleanBizIndicatorList(BaseEntityDTO baseEntityDTO, String lastYear) {
        return cleanBizIndicatorDao.findByCleanBizIndicatorList(baseEntityDTO, lastYear);
    }

    /**
     * 读取企业所属行业分组数据，然后补数据
     *
     * @param baseEntityDTO
     */
    private void findByCicsSort(BaseEntityDTO baseEntityDTO, BaseFieldDTO baseFieldDTO, String year) {

        List<CicsSortDTO> list = cicsSortService.findByCicsCleanLastYear(baseFieldDTO.getLeftFieldId(), baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), lastYear(baseEntityDTO.getYear(), year), PUBLIC_QUARTER, baseEntityDTO.getCicsId());
        long count = list.stream().filter(a -> ObjectUtil.isEmpty(a.getIndicatorValue())).count();
        List<CicsSortDTO> handleList = Lists.newArrayList();
        if (count != 0) {
            for (int i = 0; i < list.size(); i++) {
                if (i == 0) {
                    headRepair(list.get(i), list, handleList);
                } else if (i == list.size() - 1) {
                    tailRepair(list.get(i), list, handleList);
                } else {
                    middleRepair(list.get(i), list, handleList, i);
                }
            }

        }
        for (CicsSortDTO cicsSortDTO : handleList) {
            list.forEach(a -> {
                    if (Objects.equals(a.getId(), cicsSortDTO.getId())) {
                        a.setIndicatorValue(cicsSortDTO.getIndicatorValue());
                    }
                }
            );
            compressInsert(cicsSortDTO, 0, null, null, null, null, null);
        }
        list = list.stream().filter(a -> Objects.nonNull(a.getIndicatorValue())).collect(Collectors.toList());
        if (list.size() > 0) {
//        获取平均值
            Double average = averageValue(list);
//        获取极值
            Double extremum = extremumValue(list, average);
//        获取双倍值
            Double twoExtremum = NumberUtil.mul(extremum, Double.valueOf(2));
//        获取最大值
            Double maxValue = NumberUtil.add(average, twoExtremum);
//        获取最小值
            Double minValue = NumberUtil.sub(average, twoExtremum);
            //        处理极值
            list.forEach(a -> {
                if (a.getIndicatorValue() > maxValue) {
                    compressInsert(a, 1, maxValue, minValue, average, extremum, maxValue);
                    a.setIndicatorValue(maxValue);

                } else if (a.getIndicatorValue() < minValue) {
                    compressInsert(a, 1, maxValue, minValue, average, extremum, minValue);
                    a.setIndicatorValue(minValue);

                }
            });
        }
        baseEntityDTO.setFieldId(baseFieldDTO.getRightFieldId());
        List<Integer> idList = cleanBizIndicatorDao.findByCleanBizIndicatorIdList(baseEntityDTO);
        if (idList.size() > 0) {
            cleanBizIndicatorDao.deleteBatchIds(idList);
        }
        for (CicsSortDTO cicsSortDTO : list) {
            InsertToUpdate(cicsSortDTO, baseFieldDTO);
        }

    }

    private void InsertToUpdate(CicsSortDTO cicsSortDTO, BaseFieldDTO baseFieldDTO) {
        CleanBizIndicatorDTO cleanBizIndicatorDTO = new CleanBizIndicatorDTO();
        BeanUtil.copyProperties(cicsSortDTO, cleanBizIndicatorDTO);
        cleanBizIndicatorDTO.setId(null);
        cleanBizIndicatorDTO.setCreateTime(new Date());
        cleanBizIndicatorDTO.setFieldId(baseFieldDTO.getRightFieldId());
        cleanBizIndicatorDao.insert(cleanBizIndicatorDTO);
    }

    private void compressInsert(CicsSortDTO cicsSortDTO, int status, Double maxValue, Double minValue, Double avgValue, Double extremumValue, Double compressValue) {
        CleanBizCompressDTO cleanBizCompressDTO = new CleanBizCompressDTO();
        BeanUtil.copyProperties(cicsSortDTO, cleanBizCompressDTO);
        cleanBizCompressDTO.setCompressValue(compressValue);
        cleanBizCompressDTO.setAvgValue(avgValue);
        cleanBizCompressDTO.setExtremumValue(extremumValue);
        cleanBizCompressDTO.setMaxValue(maxValue);
        cleanBizCompressDTO.setMinValue(minValue);
        cleanBizCompressDTO.setStatus(status);
        cleanBizCompressDTO.setSortId(cicsSortDTO.getId().intValue());
        cleanBizCompressDTO.setId(null);
        cleanBizCompressService.createEntity(cleanBizCompressDTO);

    }

    /**
     * 获取平均值
     *
     * @param list
     * @return
     */
    private Double averageValue(List<CicsSortDTO> list) {
        Double value = list.stream().mapToDouble(CicsSortDTO::getIndicatorValue).sum();
        Double size = Double.valueOf(list.size());
        Double average = NumberUtil.div(value, size);
        return average;
    }

    /**
     * 获取极值
     *
     * @param list
     * @param average
     * @return
     */
    private Double extremumValue(List<CicsSortDTO> list, Double average) {
        Double sum = Double.valueOf(0);
        for (CicsSortDTO cicsSortDTO : list) {
            Double sumValue = NumberUtil.sub(average, cicsSortDTO.getIndicatorValue());
            Double power = NumberUtil.mul(sumValue, sumValue);
            sum = NumberUtil.add(power, sum);
        }
        Double size = Double.valueOf(list.size());
        Double divValue = NumberUtil.div(sum, size);
        Double value = Math.sqrt(divValue);
        return value;
    }

    /**
     * 头部缺失补数据
     *
     * @param cicsSortDTO
     * @param list
     * @param handleList
     * @return
     */
    private void headRepair(CicsSortDTO cicsSortDTO, List<CicsSortDTO> list, List<CicsSortDTO> handleList) {
        if (ObjectUtil.isEmpty(cicsSortDTO.getIndicatorValue())) {
            CicsSortDTO returnDTO = recursionRepairAdd(1, list);
            if (!BeanUtil.isEmpty(returnDTO)) {
                Double value = returnDTO.getIndicatorValue();
                if (ObjectUtil.isNotEmpty(value)) {
                    cicsSortDTO.setIndicatorValue(value);
                    handleList.add(cicsSortDTO);
                }
            }
        }
    }

    /**
     * 尾部缺失补数据
     *
     * @param cicsSortDTO
     * @param list
     * @param handleList
     */
    private void tailRepair(CicsSortDTO cicsSortDTO, List<CicsSortDTO> list, List<CicsSortDTO> handleList) {
        if (ObjectUtil.isEmpty(cicsSortDTO.getIndicatorValue())) {
            CicsSortDTO returnDTO = recursionRepairSub(list.size() - 2, list);
            if (!BeanUtil.isEmpty(returnDTO)) {
                Double value = returnDTO.getIndicatorValue();
                if (ObjectUtil.isNotEmpty(value)) {
                    cicsSortDTO.setIndicatorValue(value);
                    handleList.add(cicsSortDTO);
                }
            }
        }
    }

    /**
     * 中间数据缺失补数据
     *
     * @param cicsSortDTO
     * @param list
     * @param handleList
     */
    private void middleRepair(CicsSortDTO cicsSortDTO, List<CicsSortDTO> list, List<CicsSortDTO> handleList, int i) {
        if (ObjectUtil.isEmpty(cicsSortDTO.getIndicatorValue()) && Objects.nonNull(cicsSortDTO.getIndicatorSort())) {
           List<CicsSortDTO> fList= list.stream().filter(a->Objects.nonNull(a.getIndicatorSort())).sorted(Comparator.comparing(CicsSortDTO::getIndicatorSort)).collect(Collectors.toList());
            CicsSortDTO returnDTOSub = recursionRepairSub(fList,cicsSortDTO.getLastDesirability());
            CicsSortDTO returnDTOAdd = recursionRepairAdd(fList,cicsSortDTO.getLastDesirability());
            Double value = null;
            if (!BeanUtil.isEmpty(returnDTOSub) && !BeanUtil.isEmpty(returnDTOAdd)) {
//                中间算法
//               步骤一： 获取i--归一值
                Double subDesirability = returnDTOSub.getDesirability();
//               步骤二： 获取i--指标值
                Double subIndicatorValue = returnDTOSub.getIndicatorValue();
//               步骤三： 获取i++归一值
                Double addDesirability = returnDTOAdd.getDesirability();
//               步骤四： 获取i++指标值
                Double addIndicatorValue = returnDTOAdd.getIndicatorValue();
//               步骤五： 计算上年锚定归一值和i--归一值差
                Double subDifference = NumberUtil.sub(cicsSortDTO.getLastDesirability(), subDesirability);
//               步骤六： 计算上年锚定归一值和i++归一值差
                Double addDifference = NumberUtil.sub(addDesirability, cicsSortDTO.getLastDesirability());
//               步骤七： 计算i++归一值和i--归一值差
                double difference = NumberUtil.sub(addDesirability, subDesirability);


                double subValue = Double.valueOf(0);
                double addValue = Double.valueOf(0);
                if (NumberUtil.compare(difference, Double.valueOf(0)) != 0) {
                    //                  步骤八：计算步骤二 * 步骤五 / 步骤七
                    subValue = NumberUtil.div(NumberUtil.mul(subIndicatorValue, subDifference), difference);
                    //                  步骤九：计算步骤四 * 步骤六 / 步骤七
                    addValue = NumberUtil.div(NumberUtil.mul(addIndicatorValue, addDifference), difference);
                }
//                步骤十：计算步骤八+步骤九
                value = NumberUtil.add(subValue, addValue);
            } else if (BeanUtil.isEmpty(returnDTOSub) && !BeanUtil.isEmpty(returnDTOAdd)) {

                value = returnDTOAdd.getIndicatorValue();
            } else if (!BeanUtil.isEmpty(returnDTOSub) && BeanUtil.isEmpty(returnDTOAdd)) {
                value = returnDTOSub.getIndicatorValue();
            }

            if (ObjectUtil.isNotEmpty(value)) {
                cicsSortDTO.setIndicatorValue(value);
                handleList.add(cicsSortDTO);
            }
        }
    }

    private CicsSortDTO recursionRepairSub(List<CicsSortDTO> list,Double value){
        CicsSortDTO sortDTO=null;
       for(CicsSortDTO cicsSortDTO:list){
           if(Objects.nonNull(cicsSortDTO.getDesirability())) {
               if (value < cicsSortDTO.getDesirability()){
                   break;
               }else{
                   sortDTO=cicsSortDTO;
               }
           }
       }
       return sortDTO;
    }
    private CicsSortDTO recursionRepairAdd(List<CicsSortDTO> list,Double value){
        CicsSortDTO sortDTO=null;
        for(CicsSortDTO cicsSortDTO:list){
            if(Objects.nonNull(cicsSortDTO.getDesirability())) {
                if (value > cicsSortDTO.getDesirability()){
                    sortDTO=cicsSortDTO;
                    break;
                }
            }
        }
        return sortDTO;
    }

    /**
     * i++方式递归获取有值数据
     *
     * @param i
     * @param list
     * @return
     */
    private CicsSortDTO recursionRepairAdd(int i, List<CicsSortDTO> list) {
        if (list.size() > i) {
            if (ObjectUtil.isEmpty(list.get(i).getIndicatorValue())) {
                return recursionRepairAdd(i + 1, list);
            } else {
                return list.get(i);
            }
        }
        return null;
    }


    /**
     * i--方式递归获取有值数据
     *
     * @param i
     * @param list
     * @return
     */
    private CicsSortDTO recursionRepairSub(int i, List<CicsSortDTO> list) {
        if (i >= 0) {
            if (ObjectUtil.isEmpty(list.get(i).getIndicatorValue())) {
                return recursionRepairSub(i - 1, list);
            } else {
                return list.get(i);
            }
        }
        return null;
    }

    /**
     * 获取上一年
     *
     * @param year
     * @return
     */
    private String lastYear(String year, String minYear) {
        if (Objects.equals(year, minYear)) {
            return year;
        } else {
            return String.valueOf(Integer.parseInt(year) - 1);
        }

    }


    /**
     * 根据企业id和field 删除数据
     */

    void deleteByFieldAndEnterprise(Integer enterpriseId, Integer fieldId) {
        QueryWrapper<CleanBizIndicatorDTO> queryWrapper = new QueryWrapper();
        queryWrapper.eq("enterprise_id", enterpriseId);
        queryWrapper.eq("field_id", fieldId);
        deleteEntities(queryWrapper);
    }

    /**
     * 根据企业id和field 获取数据
     */

    List<CleanBizIndicatorDTO> FindByFieldAndEnterprise(Integer enterpriseId, Integer fieldId) {
        QueryWrapper<CleanBizIndicatorDTO> queryWrapper = new QueryWrapper();
        queryWrapper.eq("enterprise_id", enterpriseId);
        queryWrapper.eq("field_id", fieldId);
        List oder = Lists.newArrayList("year", "quarter");
        return list(queryWrapper);
    }


    /**
     * 用于计算企业季度数据比例算法
     *
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void enterpriseIndicatorDivide(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        Integer resultSysFieldId = rightFieldId;
        Integer divisorSysFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.DIVISOR_SYSFIELD.name());
        Integer dividendSysFieldId = enumField(enumFieldList, PublicEnum.FIELD_ENUM_PARAM.DIVIDEND_SYSFIELD.name());
        SysFieldDTO divisorSysFieldDTO = sysFieldService.getEntity(divisorSysFieldId);
        SysFieldDTO dividendSysFieldDTO = sysFieldService.getEntity(dividendSysFieldId);
        SysFieldDTO resultSysFieldDTO = sysFieldService.getEntity(resultSysFieldId);
        List<SysFieldDTO> leftList = new ArrayList<>();
//        leftList.add(divisorSysFieldDTO);
        leftList.add(dividendSysFieldDTO);
        List<Map<String, Object>> enterpriseChangeList = cleanBizIndicatorDao.getChangeListBymultiFiled(divisorSysFieldDTO, leftList, resultSysFieldDTO);
        if (enterpriseChangeList.size() > 0) {
//            List<SysFieldDTO> allList = new ArrayList<>();
//            allList.addAll(leftList);
//            allList.add(resultSysFieldDTO);
//            List<Map<String, Object>> dataByFields = cleanBizIndicatorDao.getDataByFields(divisorSysFieldDTO, allList, null, null, null);
            List<CleanBizIndicatorDTO> result = enterpriseChangeList.stream()
                .filter(a -> Objects.nonNull(a.get(divisorSysFieldDTO.getCode() + "_version")) && Objects.nonNull(a.get(dividendSysFieldDTO.getCode() + "_version")))
                .map(a -> convertMultiMapToCleanIndicatorDTOByDevide(a, divisorSysFieldDTO, dividendSysFieldDTO, resultSysFieldDTO)).collect(Collectors.toList());
//            List<Future> futureList = Lists.newArrayList();
//            for(CleanBizIndicatorDTO cleanBizIndicatorDTO:result){//  报线程溢出
//                ExecutorBuilderUtil.workQueueYield();
//                Future future = ExecutorBuilderUtil.pool.submit(() ->
//                    saveOrUpdate(cleanBizIndicatorDTO));
//                        cleanBizIndicatorDao.insertOrUpdate(cleanBizIndicatorDTO));
//                    futureList.add(future);
//            }
//            FutureGetUtil.futureGet(futureList);
            int batch = result.size() / 1000;
            for (int i = 0; i<=batch;i++){//分组插入
                int startIndex=i*1000;
                int endIndex=(i+1)*1000;
                if(endIndex>result.size()){
                    endIndex=result.size();
                }
                List<Future> futureList = Lists.newArrayList();
                result.subList(startIndex,endIndex).forEach(cleanBizIndicatorDTO->{
                                        Future future = ExecutorBuilderUtil.pool.submit(() ->
                        cleanBizIndicatorDao.insertOrUpdate(cleanBizIndicatorDTO));
                    futureList.add(future);
                    }
                );
                FutureGetUtil.futureGet(futureList);
            }
        }
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
     * map 固定字段转换
     * 双字段相除得到第三个字段
     *
     * @param map
     * @return
     */
    private CleanBizIndicatorDTO convertMultiMapToCleanIndicatorDTOByDevide(Map map, SysFieldDTO divisorSysFieldDTO, SysFieldDTO dividendSysFieldDTO, SysFieldDTO resultSysFieldDTO) {
        CleanBizIndicatorDTO result = new CleanBizIndicatorDTO();
        //如果是null 则保存null
        result.setId(Convert.toLong(map.get("id")));
        result.setEnterpriseId(Convert.toInt(map.get("enterprise_id")));
        result.setYear(Convert.toStr(map.get("year")));
        result.setFieldId(resultSysFieldDTO.getId());
        result.setQuarter(Convert.toStr(map.get("quarter")));
        Double divisor = Convert.toDouble(map.get(divisorSysFieldDTO.getCode()));
        Double dividend = Convert.toDouble(map.get(dividendSysFieldDTO.getCode()));
        result.setIndicatorValue(0.0);
        result.setFieldId(resultSysFieldDTO.getId());
        if (Objects.nonNull(divisor) && Objects.nonNull(dividend) && Math.abs(dividend) > 0.0) {
            result.setIndicatorValue(divisor / dividend);//根据field 生成相应字段version
        }
        Integer maxVersion = Convert.toInt(map.get(divisorSysFieldDTO.getCode() + "_version")) >= Convert.toInt(map.get(dividendSysFieldDTO.getCode() + "_version")) ? Convert.toInt(map.get(divisorSysFieldDTO.getCode() + "_version")) : Convert.toInt(map.get(dividendSysFieldDTO.getCode() + "_version"));
        result.setVersion(maxVersion);//根据field 生成相应字段version


//        if(Objects.isNull(maxVersion) ){
//            System.out.println("map = " + map + ", divisorSysFieldDTO = " + divisorSysFieldDTO + ", dividendSysFieldDTO = " + dividendSysFieldDTO + ", resultSysFieldDTO = " + resultSysFieldDTO);
//        }

        return result;
    }

    /**
     * 同一指标 上季度与本季度变动率
     *
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void enterpriseRateIndicatorChange(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        BaseFieldDTO baseFieldDTO = fieldList.stream().findFirst().get();
        List<SysFieldDTO> leftList = new ArrayList<>();
        SysFieldDTO leftField = sysFieldService.getEntity(baseFieldDTO.getLeftFieldId());
        SysFieldDTO rightField = sysFieldService.getEntity(baseFieldDTO.getRightFieldId());
//        leftList.add(leftField);
        List<Map<String, Object>> enterpriseChangeList = cleanBizIndicatorDao.getChangeListBymultiFiled(leftField, leftList, rightField);
        if(enterpriseChangeList.size()>0){
            Integer maxVersion = enterpriseChangeList.stream().map(a -> Convert.toInt(a.get(leftField.getCode() + "_version"))).max(Integer::compareTo).get();
            List<Integer> enterpriseIds = enterpriseChangeList.stream().map(a -> Convert.toInt(a.get("enterprise_id"))).distinct().collect(Collectors.toList());
            List<Future> futureList = Lists.newArrayList();
            for (Integer enterpriseId : enterpriseIds) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() -> enterpriseRateIndicatorChangeInsert(enterpriseId, baseFieldDTO, maxVersion));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }
    }

    private void enterpriseRateIndicatorChangeInsert(Integer enterpriseId, BaseFieldDTO baseFieldDTO, Integer maxVersion) {
        //获取左侧指标
        List<CleanBizIndicatorDTO> cleanBizIndicatorDTOS = FindByFieldAndEnterprise(enterpriseId, baseFieldDTO.getLeftFieldId());
        List<CleanBizIndicatorDTO> resultList = Lists.newArrayList();
        for (int i = 0; i < cleanBizIndicatorDTOS.size(); i++) {
            CleanBizIndicatorDTO result = new CleanBizIndicatorDTO();
            CleanBizIndicatorDTO currentFrom = cleanBizIndicatorDTOS.get(i);
            BeanUtil.copyProperties(cleanBizIndicatorDTOS.get(i), result);
            result.setId(null);
            result.setFieldId(baseFieldDTO.getRightFieldId());
            result.setVersion(maxVersion);
            if (i >0) {
//                result.setIndicatorValue(currentFrom.getIndicatorValue());
//            } else {
                CleanBizIndicatorDTO lastFrom = cleanBizIndicatorDTOS.get(i - 1);
                if (Math.abs(lastFrom.getIndicatorValue()) > 0) {
                    result.setIndicatorValue(currentFrom.getIndicatorValue() / lastFrom.getIndicatorValue() - 1);

                }else{
                    result.setIndicatorValue(1D);
                }
                resultList.add(result);
            }


        }
        if (resultList.size() > 0) {
            deleteByFieldAndEnterprise(enterpriseId, baseFieldDTO.getRightFieldId());
            saveBatch(resultList);
        }
    }

    @Override
    public List<CleanBizIndicatorDTO> findByfield(Integer filedId) {
        QueryWrapper<CleanBizIndicatorDTO> query = new QueryWrapper<>();
        query.eq("field_id", filedId);
        return list(query);
    }

    @Override
    public Page<CleanBizIndicatorDTO> findByfield(Integer filedId, Integer pageNo, Integer pageSize) {

        Page<CleanBizIndicatorDTO> page = new Page<>();
        page.setCurrent(pageNo);
        page.setSize(pageSize);
        QueryWrapper<CleanBizIndicatorDTO> query = new QueryWrapper<>();
        query.eq("field_id", filedId);
        return page(page, query);
    }

    @Override
    public Long findByEnterpriseBizListCount(Integer fieldId, String year, String quarter) {
        return cleanBizIndicatorDao.findByEnterpriseBizListCount(fieldId, year, quarter);
    }


    //比例补充算法
    @Override
    public void fillDivdData(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
//        List futureList = Lists.newArrayList();
        for (BaseFieldDTO baseFieldDTO : fieldList) {
            //获取最大\最小季度
            Map<String, String> maxYearQuarterMap = cleanBizIndicatorDao.findMaxYearQuarter(baseFieldDTO.getLeftFieldId());
            //获取所有有数据的企业
            List<CleanBizIndicatorDTO> allEnterPrise = getAllEnterPrise(baseFieldDTO.getLeftFieldId());
//            for (CleanBizIndicatorDTO cleanBizIndicatorDTO : allEnterPrise) {// 报线程溢出
//                ExecutorBuilderUtil.workQueueYield();
//                Future future = ExecutorBuilderUtil.pool.submit(() -> fillData(cleanBizIndicatorDTO, maxYearQuarterMap));
//                futureList.add(future);
//            }
            int batch = allEnterPrise.size() / 1000;
            for (int i = 0; i<=batch;i++){//分组插入
                int startIndex=i*1000;
                int endIndex=(i+1)*1000;
                if(endIndex>allEnterPrise.size()){
                    endIndex=allEnterPrise.size();
                }
                List<Future> futureList = Lists.newArrayList();
                List<CleanBizIndicatorDTO> result=Lists.newArrayList();
                allEnterPrise.subList(startIndex,endIndex).forEach(cleanBizIndicatorDTO->{
                        Future future = ExecutorBuilderUtil.pool.submit(() ->
                            fillData(cleanBizIndicatorDTO, maxYearQuarterMap,result));
                        futureList.add(future);
                    }
                );
                FutureGetUtil.futureGet(futureList);
                saveOrUpdateBatch(result);
            }



        }
//        FutureGetUtil.futureGet(futureList);
    }

    @Override
    public String findMaxVersion(Integer rightFieldId) {
        String maxVersion = cleanBizIndicatorDao.findMaxVersion(rightFieldId);
        if(Objects.isNull(maxVersion)){
            maxVersion="0";
        }
        return maxVersion;
    }

    @Override
    public List<CleanBizIndicatorDTO> findByYear(Integer year,Integer fieldId) {
        QueryWrapper<CleanBizIndicatorDTO> qery=new QueryWrapper();
        qery.eq("field_id",fieldId);
        qery.eq("year",year);
        return list(qery);
    }

    @Override
    public List<CleanBizIndicatorDTO> findByYearQuarter(String year, String quarter, Integer fieldId) {
        QueryWrapper<CleanBizIndicatorDTO> qery=new QueryWrapper();
        qery.eq("field_id",fieldId);
        qery.eq("year",year);
        qery.eq("quarter",quarter);
        return list(qery);
    }

    private void fillData(CleanBizIndicatorDTO cleanBizIndicatorDTO, Map<String, String> maxYearQuarterMap,List<CleanBizIndicatorDTO> result) {
        //获取这个企业的所有数据
        String maxYearQuarter = maxYearQuarterMap.get("maxYear");
        List<CleanBizIndicatorDTO> allDataByEnterprise = getAllDataByEnterprise(cleanBizIndicatorDTO.getFieldId(), cleanBizIndicatorDTO.getEnterpriseId());
        CleanBizIndicatorDTO first = allDataByEnterprise.get(0);
        String minYearQuarter=first.getYear()+first.getQuarter();
        //将list转为以year 和季度为key 的map
        Map<String,CleanBizIndicatorDTO>allDataByEnterpriseMap  =new HashMap<>();
        allDataByEnterprise.stream().forEach(a->{
            allDataByEnterpriseMap.put(a.getYear()+a.getQuarter(),a);
        });
        Integer firstYear = Integer.valueOf(first.getYear());
        for(int i=firstYear;;i++){
            String thisQuarter=minYearQuarter;
            for(int j=1;j<=4;j++){
                thisQuarter= StrUtil.toString(i)+"Q"+ StrUtil.toString(j);
                if(StrUtil.compare(thisQuarter,minYearQuarter,false)>=0&&StrUtil.compare(thisQuarter,maxYearQuarter,false)<=0){
                    if(allDataByEnterpriseMap.containsKey(thisQuarter)){
                        continue;
                    }else{
                        String lastYearQuarter=minYearQuarter;
                        if(j==1){
                            lastYearQuarter=   StrUtil.toString(i-1)+ "Q4";
                        }else{
                            lastYearQuarter=   StrUtil.toString(i)+ "Q"+StrUtil.toString(j-1);
                        }
                        CleanBizIndicatorDTO bizIndicatorDTO = allDataByEnterpriseMap.get(lastYearQuarter);
                        if(Objects.nonNull(bizIndicatorDTO)){
                            CleanBizIndicatorDTO newIndicatorDTO=new CleanBizIndicatorDTO();
                            BeanUtil.copyProperties(bizIndicatorDTO,newIndicatorDTO);
                            newIndicatorDTO.setYear(StrUtil.toString(i));
                            newIndicatorDTO.setQuarter("Q"+StrUtil.toString(j));
                            newIndicatorDTO.setId(null);
                            newIndicatorDTO.setCreateTime(new Date());
                            newIndicatorDTO.setUpdateTime(new Date());
                            allDataByEnterpriseMap.put(thisQuarter,newIndicatorDTO);
                            result.add(newIndicatorDTO);
                        }
                    }
                }
            }
            if(StrUtil.compare(thisQuarter,maxYearQuarter,false)>0){
                break;
            }
        }
//        Collection<CleanBizIndicatorDTO> values = allDataByEnterpriseMap.values();

    }

    private   List<CleanBizIndicatorDTO> getAllEnterPrise(Integer fieldId){
        QueryWrapper<CleanBizIndicatorDTO> query=new QueryWrapper<>();
        query.eq("field_id",fieldId);
        query.groupBy("enterprise_id");
        return list(query);
    }
    private List<CleanBizIndicatorDTO> getAllDataByEnterprise(Integer fieldId,Integer enterpriseId){
        QueryWrapper<CleanBizIndicatorDTO> query=new QueryWrapper<>();
        query.eq("field_id",fieldId);
        query.eq("enterprise_id",enterpriseId);
        List<String> list = Lists.newArrayList();
        list.add("year");
        list.add("quarter");
        query.orderByAsc(list);
        return list(query);
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




    /**
     * 同一指标 本季度与上季度比例
     *
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void enterpriseRateIndicator(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        BaseFieldDTO baseFieldDTO = fieldList.stream().findFirst().get();
        List<SysFieldDTO> leftList = new ArrayList<>();
        SysFieldDTO leftField = sysFieldService.getEntity(baseFieldDTO.getLeftFieldId());
        SysFieldDTO rightField = sysFieldService.getEntity(baseFieldDTO.getRightFieldId());
//        leftList.add(leftField);
        List<Map<String, Object>> enterpriseChangeList = cleanBizIndicatorDao.getChangeListBymultiFiled(leftField, leftList, rightField);
        if(enterpriseChangeList.size()>0){
            Integer maxVersion = enterpriseChangeList.stream().map(a -> Convert.toInt(a.get(leftField.getCode() + "_version"))).max(Integer::compareTo).get();
            List<Integer> enterpriseIds = enterpriseChangeList.stream().map(a -> Convert.toInt(a.get("enterprise_id"))).distinct().collect(Collectors.toList());
            List<Future> futureList = Lists.newArrayList();
            for (Integer enterpriseId : enterpriseIds) {
                ExecutorBuilderUtil.workQueueYield();
                Future future = ExecutorBuilderUtil.pool.submit(() -> enterpriseRateIndicatorChange(enterpriseId, baseFieldDTO, maxVersion));
                futureList.add(future);
            }
            FutureGetUtil.futureGet(futureList);
        }
    }

    @Override
    public List<BaseEntityDTO> findByCicsCountEnterprise(String year, String quarter, List<Integer> fieldList) {
        return cleanBizIndicatorDao.findByCicsCountEnterprise(year, quarter, fieldList);
    }

    private void enterpriseRateIndicatorChange(Integer enterpriseId, BaseFieldDTO baseFieldDTO, Integer maxVersion) {
        //获取左侧指标
        List<CleanBizIndicatorDTO> cleanBizIndicatorDTOS = FindByFieldAndEnterprise(enterpriseId, baseFieldDTO.getLeftFieldId());
        List<CleanBizIndicatorDTO> resultList = Lists.newArrayList();
        for (int i = 0; i < cleanBizIndicatorDTOS.size(); i++) {
            CleanBizIndicatorDTO result = new CleanBizIndicatorDTO();
            CleanBizIndicatorDTO currentFrom = cleanBizIndicatorDTOS.get(i);
            BeanUtil.copyProperties(cleanBizIndicatorDTOS.get(i), result);
            result.setId(null);
            result.setFieldId(baseFieldDTO.getRightFieldId());
            result.setVersion(maxVersion);
            if (i>0) {

                CleanBizIndicatorDTO lastFrom = cleanBizIndicatorDTOS.get(i - 1);
                if (Math.abs(lastFrom.getIndicatorValue()) > 0) {
                    result.setIndicatorValue(currentFrom.getIndicatorValue() / lastFrom.getIndicatorValue());
                }else{
                    result.setIndicatorValue(1D);
                }
                resultList.add(result);
            }

        }
        if (resultList.size() > 0) {
            deleteByFieldAndEnterprise(enterpriseId, baseFieldDTO.getRightFieldId());
            saveBatch(resultList);
        }
    }
}
