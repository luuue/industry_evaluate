package com.chilunyc.process.service.industry.impl;

import cn.hutool.core.convert.Convert;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.industry.CicsOrAreaDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.industry.CicsOrAreaDTO;
import com.chilunyc.process.entity.DTO.industry.CleanIndicatorDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.industry.CicsOrAreaService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service("cicsOrAreaServiceImpl")
public class CicsOrAreaServiceImpl extends BaseServiceImpl<CicsOrAreaDao, CicsOrAreaDTO> implements CicsOrAreaService {

    @Autowired
    CicsOrAreaDao cicsOrAreaDao;

    @Autowired
    CleanIndicatorImpl cleanIndicatorImpl;
    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {

        //查询要变更的cics
        List<Map> changedCicsList = cicsOrAreaDao.getChangedCicsList(rightFieldId);

        //对cics 进行计算
        for(Map map:changedCicsList){

            //查询某个cics相应年度季度 区域得分
            List<CicsOrAreaDTO> cicsAndYearAndQuater = findByCicsAndYearAndQuater(map);
            IntSummaryStatistics collect = cicsAndYearAndQuater.stream().filter(a -> Objects.nonNull(a.getScore())).collect(Collectors.summarizingInt(CicsOrAreaDTO::getScore));
            //平均值
            double average = collect.getAverage();
            long count = collect.getCount();
            double sum= cicsAndYearAndQuater.stream().filter(a -> Objects.nonNull(a.getScore())).mapToDouble(a -> Math.pow(a.getScore() - average, 2)).sum();
            double variance=sum/count;
            //插入 加工字段表
            CleanIndicatorDTO cleanBizIndicatorDTO=new CleanIndicatorDTO();
            cleanBizIndicatorDTO.setFieldId(rightFieldId);
            cleanBizIndicatorDTO.setVersion(Convert.toInt(map.get("version")));
            cleanBizIndicatorDTO.setCicsId(Convert.toInt(map.get("cics_id")));
            cleanBizIndicatorDTO.setQuarter(Convert.toStr(map.get("quarter")));
            cleanBizIndicatorDTO.setYear(Convert.toStr(map.get("year")));

            //删除行业方差 增加方差
            QueryWrapper<CleanIndicatorDTO> query=new QueryWrapper();
            query.eq("cics_id", Convert.toInt(map.get("cics_id")));
            query.eq("year", Convert.toStr(map.get("year")));
            query.eq("quarter", Convert.toStr(map.get("quarter")));
            query.eq("field_id",rightFieldId);
            cleanBizIndicatorDTO.setIndicatorValue(variance);
            cleanIndicatorImpl.deleteEntities(query);
            cleanIndicatorImpl.save(cleanBizIndicatorDTO);
        }
    }


    List<CicsOrAreaDTO> findByCicsAndYearAndQuater(Map map){
        QueryWrapper<CicsOrAreaDTO> query=new QueryWrapper<>();
        query.eq("cics_id", Convert.toInt(map.get("cics_id")));
        query.eq("year", Convert.toStr(map.get("year")));
        query.eq("quarter", Convert.toStr(map.get("quarter")));
        return   list(query);
    }

    //todo 给的数据没有年度和季度 所以需要最终数据去改变写法
    @Override
    public ImportResult OriginalDataImport(List<CicsOrAreaDTO> dataList, String endYear, String endMonth, String quarter, Integer importId, DataTypeEnum dataTypeEnum) {
        //TODO
        //过滤年度及季度
        if(Objects.isNull(quarter)){
            quarter="Q4";
        }
        String finalQuarter = quarter;
      ImportResult importResult=  deleteAndSave(dataList,endYear,endMonth,quarter,dataTypeEnum);

        return importResult;
    }

    /**
     * 根据年度和cicid 查询区域得分并按得分排序2：区域促进行业发展 1：区域适宜行业发展0：区域对行业发展无明显影响-1：区域不适宜行业发展 -2：区域遏制行业发展
     * @param cicsId
     * @param year
     * @return
     */
    @Override
    public List<CicsOrAreaDTO> findByCicsId(Integer cicsId, String year) {
        QueryWrapper<CicsOrAreaDTO> query=new QueryWrapper<>();
        query.eq("cics_id",cicsId);
        query.eq("year",year);
        query.orderByAsc("score");
        return list(query);
    }

    @Override
    public List<CicsOrAreaDTO> getAllListByYear(String year) {
            QueryWrapper queryWrapper=new QueryWrapper();
            queryWrapper.eq("year",year);
           return list(queryWrapper);
    }


    public ImportResult deleteAndSave(List<CicsOrAreaDTO> needAddList, String endYear, String endMonth, String quarter, DataTypeEnum dataTypeEnum) {
        ImportResult importResult= new ImportResult();
        needAddList.stream().forEach(
            a->cicsOrAreaDao.insertOrUpdateSigle(a)
        );

            importResult.setInsertCount(Long.valueOf(needAddList.size()));
            importResult.setDataTypeEnum(dataTypeEnum);
        return importResult;
    }
}
