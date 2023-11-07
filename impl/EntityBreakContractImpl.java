package com.chilunyc.process.service.enterprise.impl;

import com.chilunyc.process.dao.enterprise.EntityBreakContractDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.enterprise.EntityBreakContractDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EntityBreakContractService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service("entityBreakContractImpl")
public class EntityBreakContractImpl extends BaseServiceImpl<EntityBreakContractDao, EntityBreakContractDTO> implements EntityBreakContractService<EntityBreakContractDTO> {
    @Autowired
    private EntityBreakContractDao entityBreakContractDao;
    @Override
    public List<BaseEntityDTO> findByGroupYQ(List<Integer> fieldIds) {
        return entityBreakContractDao.findByGroupYQ(fieldIds);
    }

    @Override
    public List<EntityBreakContractDTO> findByYQ(String year, String quarter) {
        return entityBreakContractDao.findByYQ(year, quarter);
    }

    @Override
    public ImportResult OriginalDataImport(List<EntityBreakContractDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        //删除超期数据
        ImportResult importResult=deleteAndSave(dataList, dataTypeEnum);
        return importResult;
    }

    private ImportResult deleteAndSave(List<EntityBreakContractDTO> needAddList, DataTypeEnum dataTypeEnum) {
        //张听确认替换规则 债券代码+发行人确定唯一记录，替换其他字段
        ImportResult importResult=new ImportResult();
        importResult.setDataTypeEnum(dataTypeEnum);
        importResult.setInsertCount(Long.valueOf(needAddList.size()));
//        entityBreakContractDao.insertOrUpdate(needAddList);11111

        for(EntityBreakContractDTO entityBreakContractDTO:needAddList){
            if(Objects.nonNull(entityBreakContractDTO.getIndicatorValue())&&Objects.nonNull(entityBreakContractDTO.getIndicatorValue())){
                entityBreakContractDTO.setIndicatorValue( entityBreakContractDTO.getIndicatorValue()+entityBreakContractDTO.getIndicatorOtherValue()/10000D);
            }



            entityBreakContractDao.insertOrUpdateSigle(entityBreakContractDTO);
        }
        return importResult;


    }

}
