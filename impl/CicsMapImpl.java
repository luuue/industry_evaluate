package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsMapDao;
import com.chilunyc.process.entity.DTO.industry.CicsIndustryLevelDTO;
import com.chilunyc.process.entity.DTO.industry.CicsMapDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.industry.CicsIndustryLevelService;
import com.chilunyc.process.service.industry.CicsMapService;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("cicsMapImpl")
public class CicsMapImpl extends BaseServiceImpl<CicsMapDao, CicsMapDTO> implements CicsMapService {

    @Autowired
    CicsIndustryLevelService cicsIndustryLevelService;
    @Autowired
    CicsMapDao cicsMapDao;
    @Override
    public ImportResult OriginalDataImport(List<CicsMapDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
       //todo 按cicsId删除

        ImportResult importResult  = deleteAndSave(dataList,   endYear,   endMonth,   quater,   importId,   dataTypeEnum);
        List<CicsMapDTO> childrenList=new ArrayList<>();
        dataList.stream().filter(a->Objects.nonNull(a.getCicsId())).forEach(
            a->{
                List<Integer> list= Lists.newArrayList();
                List<Integer> findList= Lists.newArrayList();
                findList.add(a.getCicsId());
                getCicsIdsByCicsId(list,findList);
                for(Integer childrenId:list){
                    CicsMapDTO cicsMapDTO=new CicsMapDTO();
                    BeanUtil.copyProperties(a,cicsMapDTO);
                    cicsMapDTO.setCicsId(childrenId);
                    childrenList.add(cicsMapDTO);
                }
            }
        );
        dataList.addAll(childrenList);
        dataList.stream().filter(a-> Objects.nonNull(a.getName())).forEach(cicsMapDTO->cicsMapDao.insertOrUpdateSigle(cicsMapDTO));
//        cicsMapDao.insertOrUpdate(dataList);11111
//        for(CicsMapDTO cicsMapDTO:dataList){
//            cicsMapDao.insertOrUpdateSigle(cicsMapDTO);
//        }
            importResult.setDataTypeEnum(dataTypeEnum);
        importResult.setInsertCount(Long.valueOf(dataList.size()));
        return importResult;
    }



    public ImportResult deleteAndSave(List<CicsMapDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult  =new ImportResult();
            importResult.setInsertCount(Long.valueOf(dataList.size()));
            importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
    //  获取cicsId
   private void  getCicsIdsByCicsId( List<Integer> result, List<Integer> findList){
       List<CicsIndustryLevelDTO> listIndustry = cicsIndustryLevelService.getEntityListByParentId(findList);
       if(listIndustry.size()>0){
       List<Integer>  list = listIndustry.stream().map(CicsIndustryLevelDTO::getId).collect(Collectors.toList());
       result.addAll(list);
       getCicsIdsByCicsId(result,list);
       }
   }


    @Override
    public CicsMapDTO getEntityByCicsName(String name,Integer type) {
        QueryWrapper<CicsMapDTO> query=new QueryWrapper();
        query.eq("name",name);
        query.eq("type",type);
        return getSingleEntity(query);
    }
}
