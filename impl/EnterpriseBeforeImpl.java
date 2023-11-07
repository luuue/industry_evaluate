package com.chilunyc.process.service.enterprise.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EnterpriseBeforeDao;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseBeforeDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EnterpriseBeforeService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service("enterpriseBeforeImpl")
public class EnterpriseBeforeImpl extends BaseServiceImpl<EnterpriseBeforeDao, EnterpriseBeforeDTO> implements EnterpriseBeforeService<EnterpriseBeforeDTO> {
    @Autowired
    EnterpriseBeforeDao enterpriseBeforeDao;

    @Override
    public List<EnterpriseBeforeDTO> findSTEnterprise() {
        QueryWrapper<EnterpriseBeforeDTO> wrapper = new QueryWrapper<>();
        wrapper.select("DISTINCT  enterprise_id")
            .like("name", "st");
        return getEntityList(wrapper);
    }

    @Override
    public List<EnterpriseBeforeDTO> getEnterpriseTime(Integer enterpriseId) {
        QueryWrapper<EnterpriseBeforeDTO> wrapper = new QueryWrapper<>();
        wrapper.eq("enterprise_id", enterpriseId)
            .groupBy("start_date")
            .orderByAsc("start_date");
        return list(wrapper);
    }

    @Override
    public ImportResult OriginalDataImport(List<EnterpriseBeforeDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {

        ImportResult importResult = deleteAndSave(dataList, endYear, endMonth, quater, importId, dataTypeEnum);

        return importResult;
    }

    public ImportResult deleteAndSave(List<EnterpriseBeforeDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult = new ImportResult();
        List<EnterpriseBeforeDTO> collect1 = dataList.stream().filter(a -> Objects.isNull(a.getId())).collect(Collectors.toList());

        List<EnterpriseBeforeDTO> collect = dataList.stream().filter(a -> Objects.nonNull(a.getEnterpriseId())).collect(Collectors.toList());

//        enterpriseBeforeDao.insertOrUpdate(collect);

        for(EnterpriseBeforeDTO enterpriseBeforeDTO:collect){
            enterpriseBeforeDao.insertOrUpdateSigle(enterpriseBeforeDTO);
        }
        importResult.setInsertCount(Long.valueOf(dataList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
