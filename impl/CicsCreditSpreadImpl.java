package com.chilunyc.process.service.industry.impl;

import com.chilunyc.process.dao.industry.CicsCreditSpreadDao;
import com.chilunyc.process.dao.industry.CicsSimilarDao;
import com.chilunyc.process.entity.DTO.industry.CicsCreditSpreadDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSimilarDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.industry.CicsCreditSpreadService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("cicsCreditSpreadImpl")
public class CicsCreditSpreadImpl extends BaseServiceImpl<CicsSimilarDao, CicsSimilarDTO>  implements CicsCreditSpreadService {
    @Autowired
    private CicsCreditSpreadDao cicsCreditSpreadDao;
    @Override
    public List<CicsCreditSpreadDTO> findByVersionYQ(Integer fieldId) {
        return cicsCreditSpreadDao.findByVersionYQ(fieldId);
    }

    @Override
    public ImportResult OriginalDataImport(List<CicsCreditSpreadDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult = new ImportResult();
//        cicsCreditSpreadDao.insertOrUpdate(dataList);
        for(CicsCreditSpreadDTO cicsCreditSpreadDTO:dataList){
            cicsCreditSpreadDao.insertOrUpdateSingle(cicsCreditSpreadDTO);
        }
        importResult.setInsertCount(Long.valueOf(dataList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
