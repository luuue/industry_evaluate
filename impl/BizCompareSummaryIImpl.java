package com.chilunyc.process.service.enterprise.impl;

import com.chilunyc.process.dao.enterprise.BizCompareSummaryDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizCompareSummaryDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizSummaryDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.BizCompareSummaryService;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service("bizCompareSummaryIImpl")
public class BizCompareSummaryIImpl extends BaseServiceImpl<BizCompareSummaryDao, BizCompareSummaryDTO> implements BizCompareSummaryService {
    @Autowired
    private BizCompareSummaryDao bizCompareSummaryDao;
    @Autowired
    CicsIndustryLevelService cicsIndustryLevelService;
    @Override
    public List<BaseEntityDTO> findByYearList(String year) {
        return bizCompareSummaryDao.findByYearList(year);
    }
    @Override
    public ImportResult OriginalDataImport(List<BizCompareSummaryDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        //只有年度
//        List<BizSummaryDTO> needAddList = dataList.stream().filter(a -> endYear.compareTo(a.getYear()) > 0).collect(Collectors.toList());
        //删除并保存数据
        ImportResult importResult = deleteAndSave(dataList, endYear, endMonth, quater, importId, dataTypeEnum);
        return importResult;
    }

    public ImportResult deleteAndSave(List<BizCompareSummaryDTO> needAddList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult = new ImportResult();
        needAddList=needAddList.stream().filter(a->Objects.nonNull(a.getEnterpriseId())).collect(Collectors.toList());
        System.out.println("needAddList = " + needAddList.size());
        for(BizCompareSummaryDTO bizSummaryDTO:needAddList){
            if(Objects.nonNull(bizSummaryDTO.getCics1Name())){
                CicsIndustryLevelDTO cics= cicsIndustryLevelService.getEntityByCicsLevelAndCicsName("CICS1", bizSummaryDTO.getCics1Name());
                if(Objects.nonNull(cics)){
                    bizSummaryDTO.setCics1Id(cics.getId());
                    bizSummaryDTO.setCics1Code(cics.getCode());
                }

            }
            if(Objects.nonNull(bizSummaryDTO.getCics2Name())){
                CicsIndustryLevelDTO cics= cicsIndustryLevelService.getEntityByCicsLevelAndCicsName("CICS2", bizSummaryDTO.getCics2Name());
                if(Objects.nonNull(cics)){
                    bizSummaryDTO.setCics2Id(cics.getId());
                    bizSummaryDTO.setCics2Code(cics.getCode());
                }
            }

            if(Objects.nonNull(bizSummaryDTO.getCics3Name())){
                CicsIndustryLevelDTO cics= cicsIndustryLevelService.getEntityByCicsLevelAndCicsName("CICS3", bizSummaryDTO.getCics3Name());
                if(Objects.nonNull(cics)){
                    bizSummaryDTO.setCics3Id(cics.getId());
                    bizSummaryDTO.setCics3Code(cics.getCode());
                }
            }
            if(Objects.nonNull(bizSummaryDTO.getCics4Name())){
                CicsIndustryLevelDTO cics= cicsIndustryLevelService.getEntityByCicsLevelAndCicsName("CICS4", bizSummaryDTO.getCics4Name());
                if(Objects.nonNull(cics)){
                    bizSummaryDTO.setCics4Id(cics.getId());
                    bizSummaryDTO.setCics4Code(cics.getCode());
                }
            }
            bizCompareSummaryDao.inseetOrupDateSigile(bizSummaryDTO);
        }
        importResult.setInsertCount(Long.valueOf(needAddList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
