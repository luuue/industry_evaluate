package com.chilunyc.process.service.industry.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsIndustryLevelDao;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseStockDataDTO;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * todo 后续如果对data_original_cics_industry_level 表有修改操作 注意对缓存的处理
 */
@Service("cicsIndustryLevelImpl")
public class CicsIndustryLevelImpl extends BaseServiceImpl<CicsIndustryLevelDao, CicsIndustryLevelDTO> implements CicsIndustryLevelService {


    @Override
    public ImportResult OriginalDataImport(List<CicsIndustryLevelDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        //TODO
        return null;
    }

    @Override
    public CicsIndustryLevelDTO getEntityByCicsLevelAndCicsName(String cicsLevel, String cicsName) {
        QueryWrapper<CicsIndustryLevelDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.eq("level",cicsLevel);
        queryWrapper.eq("name",cicsName);
        return getSingleEntity(queryWrapper);
    }

    @Override
    public CicsIndustryLevelDTO getById(Integer id) {
        return getEntity(id);
    }

    @Override
    public List<CicsIndustryLevelDTO> getAllList() {
        return list();
    }

    @Override
    public List<CicsIndustryLevelDTO> getEntityListByParentId(List<Integer> cicsId) {
        QueryWrapper<CicsIndustryLevelDTO> queryWrapper=new QueryWrapper<>();
        queryWrapper.in("parent_id",cicsId);
        return list(queryWrapper);
    }

}
