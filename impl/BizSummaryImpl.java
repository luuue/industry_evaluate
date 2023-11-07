package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.platform.common.util.FileUtil;
import com.chilunyc.process.dao.enterprise.BizSummaryDao;
import com.chilunyc.process.entity.DTO.BaseEntityDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizSummaryDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.BizSummaryService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.binding.query.dynamic.ExtQueryWrapper;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("bizSummaryImpl")
public class BizSummaryImpl extends BaseServiceImpl<BizSummaryDao, BizSummaryDTO> implements BizSummaryService {
    @Autowired
    private BizSummaryDao bizSummaryDao;
    @Override
    public List<BizSummaryDTO> getCompareBizSummary(Integer enterpriseId, Integer year) {
        QueryWrapper<BizSummaryDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("enterprise_id", enterpriseId);
        queryWrapper.eq("year", year);
        return list(queryWrapper);
    }

    @Override
    public List<BizSummaryDTO> getCompareBizSummaryByCicsId(List<Integer> cicsIds, Integer year) {
        QueryWrapper<BizSummaryDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("year", year);
        queryWrapper.in("cics_id", cicsIds);
        return list(queryWrapper);
    }

    @Override
    public ImportResult OriginalDataImport(List<BizSummaryDTO> dataList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        //只有年度
//        List<BizSummaryDTO> needAddList = dataList.stream().filter(a -> endYear.compareTo(a.getYear()) > 0).collect(Collectors.toList());
        //删除并保存数据
        ImportResult importResult = deleteAndSave(dataList, endYear, endMonth, quater, importId, dataTypeEnum);
        return importResult;
    }

    @Override
    public List<BaseEntityDTO> findByYearList(String year) {
        return bizSummaryDao.findByYearList(year);
    }

    @Override
    public List<BizSummaryDTO> findByEnterpriseYearCics4(String year, Integer enterpriseId) {
        return bizSummaryDao.findByEnterpriseYearCics4(year, enterpriseId);
    }

    @Override
    public List<BizSummaryDTO> findByYearCics4(String year) {
        return bizSummaryDao.findByYearCics4(year);
    }

    @Override
    public List<BizSummaryDTO> getAllList() {
        return list();
    }

    @Override
    public List<BizSummaryDTO> findCicsFistByEnterprise(Integer enterpriseId,String year) {
        return bizSummaryDao.findCicsFistByEnterprise(enterpriseId,year);
    }

    @Override
    public List<BaseEntityDTO> findByCicsYearGLThree() {
        return bizSummaryDao.findByCicsYearGLThree();
    }


    public ImportResult deleteAndSave(List<BizSummaryDTO> needAddList, String endYear, String endMonth, String quater, Integer importId, DataTypeEnum dataTypeEnum) {
        ImportResult importResult = new ImportResult();
//        bizSummaryDao.inseetOrupDate(needAddList);
        for(BizSummaryDTO bizSummaryDTO:needAddList){
            bizSummaryDao.inseetOrupDateSigile(bizSummaryDTO);
        }
//        List<Future> futureList = Lists.newArrayList();
//        for(BizSummaryDTO bizSummaryDTO:needAddList){
//            Future future1 = ExecutorBuilderUtil.pool.submit(() -> uodateOrInsert(bizSummaryDTO));
//            futureList.add(future1);
//        }
//        FutureGetUtil.futureGet(futureList);
        importResult.setInsertCount(Long.valueOf(needAddList.size()));
        importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }


   private void uodateOrInsert(BizSummaryDTO bizSummaryDTO){
       QueryWrapper<BizSummaryDTO> query=new QueryWrapper<>();
       query.eq("year",bizSummaryDTO.getYear());
       query.eq("quarter",bizSummaryDTO.getQuarter());
       query.eq("enterprise_id",bizSummaryDTO.getEnterpriseId());
       query.eq("field_id",bizSummaryDTO.getFieldId());
       BizSummaryDTO singleEntity = getSingleEntity(query);
       if(BeanUtil.isEmpty(singleEntity)){
           bizSummaryDTO.setCreateTime(new Date());
           bizSummaryDTO.setId(null);
           createEntity(bizSummaryDTO);
       }else{
           bizSummaryDTO.setId(singleEntity.getId());
           bizSummaryDTO.setUpdateTime(new Date());
           updateEntity(bizSummaryDTO);
       }
    }

}
