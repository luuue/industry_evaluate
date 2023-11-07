package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.BizIndicatorDao;
import com.chilunyc.process.dao.enterprise.CleanBizIndicatorDao;
import com.chilunyc.process.dao.enterprise.EnterpriseSTDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.BizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseBeforeDTO;
import com.chilunyc.process.service.enterprise.BizIndicatorService;
import com.chilunyc.process.service.enterprise.EnterpriseBeforeService;
import com.chilunyc.process.service.enterprise.EnterpriseLoanService;
import com.chilunyc.process.service.enterprise.EnterpriseSTService;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.A;
import org.checkerframework.checker.units.qual.C;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service("enterpriseLoanServiceImpl")
public class EnterpriseLoanServiceImpl extends BaseServiceImpl<BizIndicatorDao, BizIndicatorDTO> implements EnterpriseLoanService {

    @Autowired
    BizIndicatorService bizIndicatorService;

    @Autowired
    CleanBizIndicatorImpl cleanBizIndicator;

    /**
     * 贷款填充算法
     * @param fieldList
     * @param rightFieldId
     * @param enumFieldList
     */
    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        BaseFieldDTO baseFieldDTO = fieldList.get(0);
        //查询原始数据和清洗后数据版本
        List<Integer> enterpriseIds = bizIndicatorService.findByEnterpriseAll(baseFieldDTO.getLeftFieldId(), baseFieldDTO.getRightFieldId());
        //根据企业信息对数据进行补充
        fillEnterPriseData(enterpriseIds,baseFieldDTO);
    }

    /**
     * 填充数据
     * @param enterpriseIds
     * @param baseFieldDTO
     */
    private void fillEnterPriseData(List<Integer> enterpriseIds, BaseFieldDTO baseFieldDTO) {
        for(Integer enterprise:enterpriseIds){
            //根据企业查询原始数据所对应字段的
            List<BizIndicatorDTO> enterpriseIndicator = getEnterpriseIndicator(enterprise, baseFieldDTO);
            List<CleanBizIndicatorDTO> filldata = filldata(enterpriseIndicator,baseFieldDTO.getRightFieldId());
            //删除原来的数据
            cleanBizIndicator.deleteByFieldAndEnterprise(enterprise,baseFieldDTO.getRightFieldId());
            //保存新数据
            cleanBizIndicator.saveBatch(filldata);
        }
    }

    /**
     * 获取企业所有相关指标的所有季度数据 按年度季度升序排列
     * @param enterpriseId
     * @param baseFieldDTO
     * @return
     */
  List<BizIndicatorDTO>  getEnterpriseIndicator(Integer enterpriseId,BaseFieldDTO baseFieldDTO){
         QueryWrapper<BizIndicatorDTO> queryWrapper=new QueryWrapper();
         queryWrapper.eq("enterprise_id",enterpriseId);
         queryWrapper.eq("field_id",baseFieldDTO.getLeftFieldId());
         queryWrapper.select( "id","indicator_value","year","version","enterprise_id","quarter","state","is_delete","create_time","update_time","field_id" );
         List orders= Lists.newArrayList("year","quarter");
         queryWrapper.orderByAsc(orders);
         return   list(queryWrapper);
  }

    /**
     * 填充数据
     * @param enterpriseIndicator
     * @param rightFieldId
     * @return
     */
    List<CleanBizIndicatorDTO>  filldata(List<BizIndicatorDTO> enterpriseIndicator, Integer rightFieldId){
        List<CleanBizIndicatorDTO> list= new ArrayList<>();
            for(int i=0;i<enterpriseIndicator.size();i++){
                CleanBizIndicatorDTO clean=new CleanBizIndicatorDTO();
                BizIndicatorDTO bizIndicatorDTO = enterpriseIndicator.get(i);
                bizIndicatorDTO.setFieldId(rightFieldId);
                //如果是null或者空  将i-1的值赋值给i
                if(Objects.isNull(bizIndicatorDTO.getIndicatorValue())&&i>0){
                    BizIndicatorDTO lastBizIndicatorDTO = enterpriseIndicator.get(i - 1);
                    bizIndicatorDTO.setIndicatorValue(lastBizIndicatorDTO.getIndicatorValue());
                }
                BeanUtil.copyProperties(bizIndicatorDTO,clean);
                clean.setUpdateTime(new Date());
                clean.setCreateTime(new Date());
                clean.setId(null);
                list.add(clean);
            }
    return list;
  }

}
