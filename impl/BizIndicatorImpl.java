package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.BizIndicatorDao;
import com.chilunyc.process.entity.DTO.enterprise.BizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizSummaryDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.BizIndicatorService;
import com.diboot.core.service.impl.BaseServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service("bizIndicatorImpl")
@Slf4j
public class BizIndicatorImpl extends BaseServiceImpl<BizIndicatorDao, BizIndicatorDTO> implements BizIndicatorService {
    @Autowired
    private BizIndicatorDao bizIndicatorDao;

    @Override
    public List<BizIndicatorDTO> findByCicsAll(BizIndicatorDTO bizIndicatorDTO) {
        return bizIndicatorDao.findByCicsAll(bizIndicatorDTO);
    }

    @Override
    public List<Integer> findByEnterpriseAll(Integer originalField, Integer cleanField) {
        return bizIndicatorDao.findByEnterpriseAll(originalField, cleanField);
    }

    @Override
    public ImportResult OriginalDataImport(List<BizIndicatorDTO> dataList, String endYear, String endMonth, String quarter, Integer importId, DataTypeEnum dataTypeEnum) {

        //TODO  对数据进行校验 1.list数据 2.字段数据
        //检查数据,删除list中超过设定时间的数据
//        List<BizIndicatorDTO> needAddList = checkData(dataList, endYear, endMonth, quarter, importId);
        //TODO 枚举值excel转换
        //删除历史数据，保存新数据
        ImportResult result = deleteAndSave(dataList, endYear, endMonth, quarter);
        result.setDataTypeEnum(dataTypeEnum);
        return result;
    }

    @Override
    public List<BizIndicatorDTO> findByMaxYearList(Integer fieldId) {
        return bizIndicatorDao.findByMaxYearList(fieldId);
    }

    @Override
    public Long findByEnterpriseListCount(Integer fieldId, String year, String quarter) {
        return bizIndicatorDao.findByEnterpriseListCount(fieldId, year, quarter);
    }

    @Override
    public Long findByEnterpriseBizListCount(Integer fieldId, String year, String quarter) {
        return bizIndicatorDao.findByEnterpriseBizListCount(fieldId, year, quarter);
    }

    @Override
    public Map<String, String> findByMaxMinYearMQ(Integer fieldId) {
        return bizIndicatorDao.findByMaxMinYearMQ(fieldId);
    }

    @Override
    public List<BizIndicatorDTO> findEnterpriseMaxVersionByYearAndQuarterAndFiled(Integer fieldId, String year, String quarter) {
        return bizIndicatorDao.findByEnterpriseVersionByYearAndFiled(fieldId,year,quarter);
    }


    ImportResult deleteAndSave(List<BizIndicatorDTO> needAddList, String endYear, String endMonth, String quarter) {
        //TODO 设置版本

        ImportResult importResult = new ImportResult();
        if (needAddList.size() > 0) {
            List<BizIndicatorDTO> nonullList = needAddList.stream().filter(a -> Objects.nonNull(a.getIndicatorValue())).collect(Collectors.toList());

         //单上传
          for(BizIndicatorDTO bizIndicatorDTO:nonullList){
              if(Objects.isNull(bizIndicatorDTO.getEnterpriseId())){
                  log.info("bizIndicatorDTO:{}",bizIndicatorDTO);
              }
              int i = bizIndicatorDao.insertOrUpdateByUnique(bizIndicatorDTO);
          }

          //批量上传
//            bizIndicatorDao.insertOrUpdateBacthByUnique(nonullList);
            //先查后改
//            List<BizIndicatorDTO> nonullList = needAddList.stream().filter(a -> Objects.nonNull(a.getIndicatorValue())).collect(Collectors.toList());
//            List<Future> futureList = Lists.newArrayList();
//            for(BizIndicatorDTO bizIndicatorDTO:nonullList){
//                uodateOrInsert(bizIndicatorDTO);
////                Future future1 = ExecutorBuilderUtil.pool.submit(() -> uodateOrInsert(bizIndicatorDTO));
////                futureList.add(future1);
//            }
//            FutureGetUtil.futureGet(futureList);
        } else {
            importResult.setCode(400);//无数据
            importResult.setMessage("无保存数据");
        }
        return importResult;
    }

    private void uodateOrInsert(BizIndicatorDTO bizIndicatorDTO) {
        QueryWrapper<BizSummaryDTO> query = new QueryWrapper<>();
        query.eq("year", bizIndicatorDTO.getYear());
        query.eq("quarter", bizIndicatorDTO.getQuarter());
        query.eq("enterprise_id", bizIndicatorDTO.getEnterpriseId());
        query.eq("field_id", bizIndicatorDTO.getFieldId());
        BizIndicatorDTO singleEntity = getSingleEntity(query);
        if (BeanUtil.isEmpty(singleEntity)) {
            bizIndicatorDTO.setCreateTime(new Date());
            bizIndicatorDTO.setId(null);
            createEntity(bizIndicatorDTO);
        } else {
            bizIndicatorDTO.setId(singleEntity.getId());
            bizIndicatorDTO.setUpdateTime(new Date());
            updateEntity(bizIndicatorDTO);
        }
    }

    private Long deleteByEndTime(List<BizIndicatorDTO> needAddList, String endYear, String endMonth, String quarter) {
        //删除大于截止年度的数据
        AtomicReference<Long> deleteSum = new AtomicReference<>(0L);

//            deleteSum.updateAndGet(v -> v + countYearAndQuarter);


        return deleteSum.get();
    }

    /**
     * 过滤掉超出截止时间的数据
     *
     * @param dataList
     * @param endYear
     * @param endMonth
     * @param quater
     * @param importId
     * @return
     */
    private List<BizIndicatorDTO> checkData(List<BizIndicatorDTO> dataList, String endYear, String endMonth, String quater, Integer importId) {
        return dataList.stream().filter(a -> !(endYear.compareTo(a.getYear()) <= 0 && quater.compareTo(a.getQuarter()) <= 0)).map(a -> {
            a.setFieldId(importId);
            return a;
        }).collect(Collectors.toList());
    }
}
