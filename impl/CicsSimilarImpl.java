package com.chilunyc.process.service.industry.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsSimilarDao;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseBeforeDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.industry.CicsSimilarDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.industry.CicsSimilarService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service("cicsSimilarImpl")
public class CicsSimilarImpl extends BaseServiceImpl<CicsSimilarDao, CicsSimilarDTO> implements CicsSimilarService {
    @Autowired
    private CicsSimilarDao cicsSimilarDao;


    @Override
    public List<Integer> getSimilarCicsIdsByCicsIds(List<Integer> cicsIds) {
        QueryWrapper<CicsSimilarDTO> query=new QueryWrapper();
        query.in("cics_id",cicsIds);
        List<CicsSimilarDTO> list = list(query);
        return  list.stream().map(o->o.getCicsSimilarId()).collect(Collectors.toList());
    }

    @Override
    public List<Integer> getSimilarCicsIdByCicsId(Integer cicsId) {
        QueryWrapper<CicsSimilarDTO> query=new QueryWrapper();
        query.eq("cics_id",cicsId);
        List<CicsSimilarDTO> list = list(query);
        return  list.stream().map(o->o.getCicsSimilarId()).collect(Collectors.toList());
    }

    @Override
    public ImportResult OriginalDataImport(List<CicsSimilarDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult  = deleteAndSave(dataList,   endYear,   endMonth,   quater,   importId,   dataTypeEnum);

        return importResult;
    }

    @Override
    public List<CicsSimilarDTO> findByEnterpriseYearInCics(Integer enterpriseId, String year, List<Integer> cicsList) {
        return cicsSimilarDao.findByEnterpriseYearInCics(enterpriseId, year, cicsList);
    }
    @Override
    public List<CicsSimilarDTO> findByEnterpriseYearInCicsV1(Integer enterpriseId, String year, List<Integer> cicsList) {
        return cicsSimilarDao.findByEnterpriseYearInCicsV1(enterpriseId, year, cicsList);
    }
    @Transactional
    public ImportResult deleteAndSave(List<CicsSimilarDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        // 按cicsId 删除 并添加
        ImportResult importResult  =new ImportResult();
        List<Integer> cicsIds = dataList.stream().map(a -> a.getCicsId()).distinct().collect(Collectors.toList());
        QueryWrapper<CicsSimilarDTO>  wrapper=new QueryWrapper<>();
        wrapper.in("cics_id",cicsIds);
        long count = count(wrapper);
        boolean b = deleteEntities(wrapper);
        boolean b1 = saveBatch(dataList);
        if(b1){
            importResult.setDeleteCount(count);
            importResult.setInsertCount(Long.valueOf(dataList.size()));
            importResult.setDataTypeEnum(dataTypeEnum);
        }
        return importResult;
    }
}


