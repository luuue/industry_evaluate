package com.chilunyc.process.service.enterprise.impl;

import com.chilunyc.process.dao.enterprise.EntityIssuanceBondsDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.enterprise.EntityIssuanceBondsDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EntityIssuanceBondsService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("entityIssuanceBondsImpl")
public class EntityIssuanceBondsImpl extends BaseServiceImpl<EntityIssuanceBondsDao, EntityIssuanceBondsDTO> implements EntityIssuanceBondsService<EntityIssuanceBondsDTO> {
    @Autowired
    private EntityIssuanceBondsDao entityIssuanceBondsDao;

    @Override
    public List<BaseEntityDTO> findByGroupYQ(Integer fieldId) {
        return entityIssuanceBondsDao.findByGroupYQ(fieldId);
    }

    @Override
    public List<EntityIssuanceBondsDTO> findByYQ(String year, String quarter) {
        return entityIssuanceBondsDao.findByYQ(year, quarter);
    }

    @Override
    public ImportResult OriginalDataImport(List<EntityIssuanceBondsDTO>  dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult = new ImportResult();
//        entityIssuanceBondsDao.insertOrUpdate(dataList);1111

        for(EntityIssuanceBondsDTO entityIssuanceBondsDTO:dataList){
            entityIssuanceBondsDao.insertOrUpdateSigle(entityIssuanceBondsDTO);
        }
        importResult.setInsertCount(Long.valueOf(dataList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
