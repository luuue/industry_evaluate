package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EntityConstantIncrementDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseDTO;
import com.chilunyc.process.entity.DTO.enterprise.EntityConstantIncrementDTO;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EnterpriseService;
import com.chilunyc.process.service.enterprise.EntityConstantIncrementService;
import com.chilunyc.process.service.industry.CleanIndicatorService;
import com.chilunyc.process.util.DateYearMqUtils;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("entityConstantIncrementImpl")
@Slf4j
public class EntityConstantIncrementImpl extends BaseServiceImpl<EntityConstantIncrementDao, EntityConstantIncrementDTO> implements EntityConstantIncrementService<EntityConstantIncrementDTO> {
    @Autowired
    private EntityConstantIncrementDao entityConstantIncrementDao;
    @Autowired
    private CleanIndicatorService cleanIndicatorService;
    @Autowired
    EnterpriseService enterpriseService;

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<BaseEntityDTO> list = entityConstantIncrementDao.findByYearQuarter();
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> yearDeleteToInsert(baseEntityDTO, rightFieldId));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    @Override
    public void moneyCalculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<BaseEntityDTO> list = entityConstantIncrementDao.findByYearQuarter();
        List<Future> futureList = Lists.newArrayList();
        for (BaseEntityDTO baseEntityDTO : list) {
            ExecutorBuilderUtil.workQueueYield();
            Future future = ExecutorBuilderUtil.pool.submit(() -> yearSumDeleteToInsert(baseEntityDTO, rightFieldId));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }


    /**
     * 原始次数+删除+写入
     *
     * @param fieldId
     */
    private void yearDeleteToInsert(BaseEntityDTO baseEntityDTO, Integer fieldId) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId);
        cleanIndicatorService.deleteEntities(queryWrapper);
        BaseEntityDTO lastEntityDTO = DateYearMqUtils.summaryLast(baseEntityDTO);
        List<BaseEntityDTO> list = entityConstantIncrementDao.findByYearQuarterCount(baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), lastEntityDTO.getYear());
        for (BaseEntityDTO entityDTO : list) {
            CleanIndicatorDTO bizIndicatorDTO = new CleanIndicatorDTO();
            BeanUtil.copyProperties(entityDTO, bizIndicatorDTO);
            bizIndicatorDTO.setFieldId(fieldId);
            bizIndicatorDTO.setIndicatorValue(entityDTO.getNumber().doubleValue());
            cleanIndicatorService.createEntity(bizIndicatorDTO);
        }
    }


    /**
     * 原始额度+删除+写入
     *
     * @param fieldId
     */
    private void yearSumDeleteToInsert(BaseEntityDTO baseEntityDTO, Integer fieldId) {
        QueryWrapper<CleanIndicatorDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", baseEntityDTO.getYear()).eq("quarter", baseEntityDTO.getQuarter()).eq("field_id", fieldId);
        cleanIndicatorService.deleteEntities(queryWrapper);
        BaseEntityDTO lastEntityDTO = DateYearMqUtils.summaryLast(baseEntityDTO);
        List<EntityConstantIncrementDTO> list = entityConstantIncrementDao.findByYearQuarterSum(baseEntityDTO.getYear(), baseEntityDTO.getQuarter(), lastEntityDTO.getYear());
        for (EntityConstantIncrementDTO entityConstantIncrementDTO : list) {
            CleanIndicatorDTO bizIndicatorDTO = new CleanIndicatorDTO();
            BeanUtil.copyProperties(entityConstantIncrementDTO, bizIndicatorDTO);
            bizIndicatorDTO.setFieldId(fieldId);
            cleanIndicatorService.createEntity(bizIndicatorDTO);
        }
    }


    @Override
    public ImportResult OriginalDataImport(List<EntityConstantIncrementDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        log.info("定增执行插入操作{}", dataList.size());
        List<EntityConstantIncrementDTO> lastResultList = dataList.stream().map(a -> {
            EnterpriseDTO byAstockCode = enterpriseService.getByAstockCode(a.getCode());
            if (Objects.nonNull(byAstockCode)) {
                a.setEnterpriseId(byAstockCode.getId());
            }
            return a;
        }).collect(Collectors.toList());
        ImportResult importResult = new ImportResult();
//        entityConstantIncrementDao.insertOrUpdate(lastResultList);
        for (EntityConstantIncrementDTO entityConstantIncrementDTO : lastResultList) {
            entityConstantIncrementDao.insertOrUpdateSingle(entityConstantIncrementDTO);
        }
        importResult.setInsertCount(Long.valueOf(dataList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }


}
