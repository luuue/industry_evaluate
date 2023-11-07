package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.NumberUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EnterpriseStockDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseClosigDataDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseStockDataDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EnterpriseStockService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("enterpriseStockServiceImpl")
public class EnterpriseStockServiceImpl extends BaseServiceImpl<EnterpriseStockDao, EnterpriseStockDataDTO> implements EnterpriseStockService {
    @Autowired
    private EnterpriseStockDao enterpriseStockDao;
    @Autowired
    CleanBizIndicatorImpl bizIndicatorService;

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
//        //计算企业季度收盘价
//        getEnterpriseCLosePrice();
//        //计算企业季度成交量
//        getEnterpriseTradingVolume();
//        //获取企业季度市值
//         getEnterpriseMarketValue();
    }

    public void cLosePriceCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //计算企业季度收盘价
        getEnterpriseCLosePrice(rightFieldId);
    }

    public void tradingVolumeCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //计算企业季度成交量
        getEnterpriseTradingVolume(rightFieldId);
    }

    public void marketValueCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        //获取企业季度市值
        getEnterpriseMarketValue(rightFieldId);
    }

    @Override
    public void PECalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<EnterpriseStockDataDTO> list = enterpriseStockDao.findByPEAverage();

        Map<String, List<EnterpriseStockDataDTO>> map = list.stream().collect(Collectors.groupingBy(a -> a.getYear()));
        for (String key : map.keySet()) {
            PEDeleteToInsert(key,map.get(key),rightFieldId);
        }

    }


    private void PEDeleteToInsert(String year, List<EnterpriseStockDataDTO> list, Integer fieldId) {
        QueryWrapper<CleanBizIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", year).eq("field_id", fieldId);
        bizIndicatorService.deleteEntities(queryWrapper);
        List<Future> futureList = Lists.newArrayList();
        for (EnterpriseStockDataDTO enterpriseStockDataDTO : list) {
            CleanBizIndicatorDTO cleanBizIndicatorDTO = new CleanBizIndicatorDTO();
            BeanUtil.copyProperties(enterpriseStockDataDTO, cleanBizIndicatorDTO);
            cleanBizIndicatorDTO.setFieldId(fieldId);
            cleanBizIndicatorDTO.setIndicatorValue(enterpriseStockDataDTO.getPbRatio().doubleValue());
            cleanBizIndicatorDTO.setCreateTime(new Date());
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
            bizIndicatorService.createEntity(cleanBizIndicatorDTO));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);

    }


    //获取需要转换的企业季度成交量信息
    private void getEnterpriseCLosePrice(Integer rightFieldId) {
        List<Future> futureList = Lists.newArrayList();
        //获取需要更新的企业信息
        String maxVersion = bizIndicatorService.findMaxVersion(rightFieldId);
        Map<String,Long> maxMinYear=  enterpriseStockDao.findMaxMinYear(maxVersion);
        Integer maxYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("max_year"))?1900:maxMinYear.get("max_year").intValue();
        Integer minYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("min_year"))?1901: maxMinYear.get("min_year").intValue();
        for(int i=minYear;i<=maxYear;i++){
            int finalI = i;
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
            updateAndSaveClose(finalI,maxVersion,rightFieldId));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    //获取需要转换的企业季度成交量信息
    private void getEnterpriseTradingVolume(Integer rightFieldId) {
        List<Future> futureList = Lists.newArrayList();
        //获取需要更新的企业信息
        String maxVersion = bizIndicatorService.findMaxVersion(rightFieldId);
        Map<String,Long> maxMinYear=  enterpriseStockDao.findMaxMinYear(maxVersion);
        Integer maxYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("max_year"))?1900:maxMinYear.get("max_year").intValue();
        Integer minYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("min_year"))?1901: maxMinYear.get("min_year").intValue();
        for(int i=minYear;i<=maxYear;i++){
            int finalI = i;
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() ->
            updateAndSaveTradingVolume(finalI,maxVersion,rightFieldId));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }
   //获取企业季度市值
   private void getEnterpriseMarketValue(Integer rightFieldId){
       List<Future> futureList = Lists.newArrayList();
       //获取需要更新的企业信息
       String maxVersion = bizIndicatorService.findMaxVersion(rightFieldId);

    Map<String,Long> maxMinYear=  enterpriseStockDao.findMaxMinYear(maxVersion);
       Integer maxYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("max_year"))?1900: maxMinYear.get("max_year").intValue();
       Integer minYear=Objects.isNull(maxMinYear)||Objects.isNull(maxMinYear.get("min_year"))?1901: maxMinYear.get("min_year").intValue();
       for(int i=minYear;i<=maxYear;i++){
           int finalI = i;
           ExecutorBuilderUtil.workQueueYield();
           Future future = ExecutorBuilderUtil.pool.submit(() ->
           updateAndSaveMarketValue(finalI,maxVersion,rightFieldId));
           futureList.add(future);
       }
       FutureGetUtil.futureGet(futureList);
   }
    void updateAndSaveMarketValue(Integer year,String maxVersion,Integer rightFieldId){
        List<CleanBizIndicatorDTO> byYearAndVersion = enterpriseStockDao.findtotalMarketValueByYearAndVersion(year, maxVersion);
        List<CleanBizIndicatorDTO> hasValueList  =  bizIndicatorService.findByYear(year,rightFieldId);
        List<CleanBizIndicatorDTO> list =   byYearAndVersion.stream().map(a->{
            a.setFieldId(rightFieldId);
            Optional<CleanBizIndicatorDTO> first = hasValueList.stream().filter(b -> b.getEnterpriseId().equals(a.getEnterpriseId()) && b.getYear().equals(a.getYear()) && a.getQuarter().equals(b.getQuarter())).findFirst();
            if(first.isPresent()){
                a.setId(first.get().getId());
            }else{
                a.setId(null);
                a.setCreateTime(new Date());
            }
            a.setUpdateTime(new Date());
            return a;
        }).collect(Collectors.toList());
        bizIndicatorService.saveOrUpdateBatch(list);
    }
  void updateAndSaveClose(Integer year,String maxVersion,Integer rightFieldId){
       List<CleanBizIndicatorDTO> byYearAndVersion = enterpriseStockDao.findByYearAndVersion(year, maxVersion);
       List<CleanBizIndicatorDTO> hasValueList  =  bizIndicatorService.findByYear(year,rightFieldId);
       List<CleanBizIndicatorDTO> list =   byYearAndVersion.stream().map(a->{
           a.setFieldId(rightFieldId);
           Optional<CleanBizIndicatorDTO> first = hasValueList.stream().filter(b -> b.getEnterpriseId().equals(a.getEnterpriseId()) && b.getYear().equals(a.getYear()) && a.getQuarter().equals(b.getQuarter())).findFirst();
           if(first.isPresent()){
               a.setId(first.get().getId());
           }else{
               a.setId(null);
               a.setCreateTime(new Date());
           }
           a.setUpdateTime(new Date());
           return a;
       }).collect(Collectors.toList());
       bizIndicatorService.saveOrUpdateBatch(list);
   }
    void updateAndSaveTradingVolume(Integer year,String maxVersion,Integer rightFieldId){
        List<CleanBizIndicatorDTO> byYearAndVersion = enterpriseStockDao.findTradingVolumeByYearAndVersion(year, maxVersion);
        List<CleanBizIndicatorDTO> hasValueList  =  bizIndicatorService.findByYear(year,rightFieldId);
        List<CleanBizIndicatorDTO> list =   byYearAndVersion.stream().map(a->{
            a.setFieldId(rightFieldId);
            Optional<CleanBizIndicatorDTO> first = hasValueList.stream().filter(b -> b.getEnterpriseId().equals(a.getEnterpriseId()) && b.getYear().equals(a.getYear()) && a.getQuarter().equals(b.getQuarter())).findFirst();
            if(first.isPresent()){
                a.setId(first.get().getId());
            }else{
                a.setId(null);
                a.setCreateTime(new Date());
            }
            a.setUpdateTime(new Date());
            return a;
        }).collect(Collectors.toList());
        bizIndicatorService.saveOrUpdateBatch(list);
    }
    /**
     * 求平均值
     *
     * @param colsePrice
     * @param fieldId
     * @return
     */
    private CleanBizIndicatorDTO convertToCleanBizIndicatorAverage(EnterpriseClosigDataDTO colsePrice, Integer fieldId) {

        CleanBizIndicatorDTO bizIndicatorDTO=new CleanBizIndicatorDTO();
            if(Objects.nonNull(colsePrice)){
                bizIndicatorDTO.setEnterpriseId(colsePrice.getEnterpriseId());
                BigDecimal indicatorValue=null;//设置为null  即定为无数据  =》二次加权求数据时无数据不参与计算
                BigDecimal closingPriceSum = colsePrice.getIndicatorSum();
                Integer closingPriceCount = colsePrice.getIndicatorCount();
                if(Objects.nonNull(closingPriceSum)&&Objects.nonNull(closingPriceCount)&&closingPriceCount!=0){
                    indicatorValue=closingPriceSum.divide(BigDecimal.valueOf(closingPriceCount),4, RoundingMode.HALF_UP);//用于平均有无数据的月份 .multiply(BigDecimal.valueOf(3)
                }
                bizIndicatorDTO.setIndicatorValue(indicatorValue.doubleValue());//value 类型待商榷
                bizIndicatorDTO.setQuarter(colsePrice.getQuarter());
                bizIndicatorDTO.setYear(colsePrice.getYear());
                bizIndicatorDTO.setVersion(colsePrice.getMaxVersion());
                bizIndicatorDTO.setFieldId(fieldId);
                bizIndicatorDTO.setIsDelete(0);
                bizIndicatorDTO.setState(0);
                if(Objects.nonNull(colsePrice.getEnterpriceIndicatorId())){
                    System.out.println("colsePrice = " + colsePrice.getEnterpriceIndicatorId() + ", fieldId = " + fieldId);
                    bizIndicatorDTO.setId(colsePrice.getEnterpriceIndicatorId());
                    bizIndicatorDTO.setUpdateTime(new Date());
                }else{
                    bizIndicatorDTO.setCreateTime(new Date());
                }
            }
            return bizIndicatorDTO;

    }

    /**
     * 平均后求和(即乘以3)
     *
     * @param colsePrice
     * @param fieldId
     * @return
     */
    private CleanBizIndicatorDTO convertToCleanBizIndicatorSum(EnterpriseClosigDataDTO colsePrice, Integer fieldId) {

        CleanBizIndicatorDTO bizIndicatorDTO = new CleanBizIndicatorDTO();
        if (Objects.nonNull(colsePrice)) {
            bizIndicatorDTO.setEnterpriseId(colsePrice.getEnterpriseId());
            BigDecimal indicatorValue = null;//设置为null  即定为无数据  =》二次加权求数据时无数据不参与计算
            BigDecimal closingPriceSum = colsePrice.getIndicatorSum();
            Integer indicatorCount = colsePrice.getIndicatorCount();
            if(Objects.nonNull(closingPriceSum)&&indicatorCount!=0){
                indicatorValue=closingPriceSum.divide(BigDecimal.valueOf(indicatorCount).multiply(BigDecimal.valueOf(3)),4, RoundingMode.HALF_UP);//用于平均有无数据的月份
            }
            bizIndicatorDTO.setIndicatorValue(indicatorValue.doubleValue());//value 类型待商榷
            bizIndicatorDTO.setQuarter(colsePrice.getQuarter());
            bizIndicatorDTO.setYear(colsePrice.getYear());
            bizIndicatorDTO.setVersion(colsePrice.getMaxVersion());
            bizIndicatorDTO.setFieldId(fieldId);
            bizIndicatorDTO.setIsDelete(0);
            bizIndicatorDTO.setState(0);
            if (Objects.nonNull(colsePrice.getEnterpriceIndicatorId())) {
                bizIndicatorDTO.setId(colsePrice.getEnterpriceIndicatorId());
                bizIndicatorDTO.setUpdateTime(new Date());
            } else {
                bizIndicatorDTO.setCreateTime(new Date());
            }
        }
        return bizIndicatorDTO;

    }


    @Override
    public ImportResult OriginalDataImport(List<EnterpriseStockDataDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult=deleteAndSave(dataList,dataTypeEnum);
        return importResult;
    }

    public ImportResult deleteAndSave(List<EnterpriseStockDataDTO> needAddList, DataTypeEnum dataTypeEnum) {
            ImportResult importResult=new ImportResult();
//            enterpriseStockDao.insertOrUpdate(needAddList);11111

            for(EnterpriseStockDataDTO enterpriseStockDataDTO:needAddList){
                //客户电话沟通区间为-100000 到100000
                if(Objects.nonNull(enterpriseStockDataDTO.getPbRatio())  && BigDecimal.valueOf(100000).compareTo(enterpriseStockDataDTO.getPbRatio())>0){
                    enterpriseStockDataDTO.setPbRatio(BigDecimal.valueOf(100000));
                }
                if(Objects.nonNull(enterpriseStockDataDTO.getPbRatio())  && BigDecimal.valueOf(-100000).compareTo(enterpriseStockDataDTO.getPbRatio())<0){
                    enterpriseStockDataDTO.setPbRatio(BigDecimal.valueOf(-100000));
                }
                enterpriseStockDao.insertOrUpdateSigle(enterpriseStockDataDTO);
            }
            importResult.setInsertCount(Long.valueOf(needAddList.size()));
            importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
