package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.CleanBizIndicatorDao;
import com.chilunyc.process.dao.enterprise.EnterpriseSTDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseBeforeDTO;
import com.chilunyc.process.service.enterprise.EnterpriseBeforeService;
import com.chilunyc.process.service.enterprise.EnterpriseSTService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service("enterpriseSTServiceImpl")
public class EnterpriseSTServiceImpl  extends BaseServiceImpl<CleanBizIndicatorDao, CleanBizIndicatorDTO> implements EnterpriseSTService {
    @Autowired
    EnterpriseSTDao enterpriseSTDao;
    @Autowired
    EnterpriseBeforeService enterpriseBeforeService;


    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
    //检查版本号
        Integer fieldId=rightFieldId;
        Boolean aBoolean = checkVersion(fieldId);
        if(aBoolean){//需要进行迭代
            //EnterpriseBeforeImpl中查询存在st的企业Enterprise_id
            List<EnterpriseBeforeDTO> stEnterprise = enterpriseBeforeService.findSTEnterprise();
            //遍历并对存在st情况的企业进行分期处理
            List<CleanBizIndicatorDTO> result=new ArrayList<>();
            for(EnterpriseBeforeDTO enterpriseBeforeDTO :stEnterprise){
                List<CleanBizIndicatorDTO> enterpriseSTQuater = getEnterpriseSTQuater(enterpriseBeforeDTO, fieldId);
                result.addAll(enterpriseSTQuater);
            }
            //删除原来的记录calculation
            QueryWrapper<EnterpriseBeforeDTO>  wrapper=new QueryWrapper();
            wrapper.eq("field_id",fieldId);
            wrapper.eq("is_delete",0);
            deleteEntities(wrapper);
            //新增
            saveBatch(result);
        }

    }

    //检查版本号
    private  Boolean checkVersion(Integer fieldId){
     return    enterpriseSTDao.checkVersion(fieldId);
    }
    //根据企业id st 日期
      List<CleanBizIndicatorDTO>  getEnterpriseSTQuater(EnterpriseBeforeDTO enterpriseBeforeDTO,Integer fieldId){
        //查询企业相关所有改名信息,已按start_date 进行去重排序
          List<EnterpriseBeforeDTO> enterpriseTime = enterpriseBeforeService.getEnterpriseTime(enterpriseBeforeDTO.getEnterpriseId());
            Date startDate=null;
            Date endDate=null;
          Optional<Integer> maxVersion =
              enterpriseTime.stream().max(Comparator.comparing(EnterpriseBeforeDTO::getVersion)).map(EnterpriseBeforeDTO::getVersion);
          List<CleanBizIndicatorDTO>  result=new ArrayList<>();
          EnterpriseBeforeDTO  currentTime= null;
          for(int i=0;i<enterpriseTime.size();i++){
                  currentTime= enterpriseTime.get(i);
                //st开始记时
                if(currentTime.getName().toLowerCase().contains("st")){
                    if(i==0){
                        startDate=currentTime.getStartDate();
                    }else {
                        EnterpriseBeforeDTO  lastTime= enterpriseTime.get(i-1);
                        if(!lastTime.getName().toLowerCase().contains("st")){//上一时间段不是st
                            startDate=currentTime.getStartDate();
                        }
                    }
                }
                //st 及时结束
                if(!currentTime.getName().toLowerCase().contains("st")){
                    if(i==0){
                       continue;
                    }else {
                        EnterpriseBeforeDTO  lastTime= enterpriseTime.get(i-1);
                        if(lastTime.getName().toLowerCase().contains("st")){//上一时间段是st
                            endDate=currentTime.getStartDate();
                            //去计算周期并返回CleanBizIndicatorDTO list
                            currentTime.setVersion(maxVersion.get());
                            List<CleanBizIndicatorDTO> cleanBizIndicatorDTOS = convertTimeToQuater(startDate, endDate, currentTime, fieldId);
                            result.addAll(cleanBizIndicatorDTOS);
                            startDate=null;
                            endDate=null;
                        }
                    }
                }
            }
          //如果到现在位置还是st，则没有结束时间
          if(Objects.nonNull(startDate)&&Objects.isNull(endDate)&&Objects.nonNull(currentTime)){
              endDate=new Date();
              List<CleanBizIndicatorDTO> cleanBizIndicatorDTOS = convertTimeToQuater(startDate, endDate, currentTime, fieldId);
              result.addAll(cleanBizIndicatorDTOS);
          }
          return result.stream().distinct().collect(Collectors.toList());

      }
      List<CleanBizIndicatorDTO>  convertTimeToQuater(Date startTime, Date endTime, EnterpriseBeforeDTO enterprise, Integer fieldId){
          List<CleanBizIndicatorDTO> list= new ArrayList<>();
          if(null==startTime||null==endTime){
                return list;
          }
//          LinkedHashSet<String> quarters = DateUtil.yearAndQuarter(startTime, endTime);//需要确认具体返还
          Set<String> quarters=getQuartersBetween(  DateTime.of(startTime),   DateTime.of(endTime));

          for(String quarter: quarters){
              CleanBizIndicatorDTO cleanBizIndicatorDTO=new CleanBizIndicatorDTO();
              String year = quarter.substring(0, 4);
              String indicatorQuarter =quarter.substring(4);
              cleanBizIndicatorDTO.setEnterpriseId(enterprise.getEnterpriseId());
              cleanBizIndicatorDTO.setVersion(enterprise.getVersion());
              cleanBizIndicatorDTO.setYear(year);
              cleanBizIndicatorDTO.setQuarter(indicatorQuarter);
              cleanBizIndicatorDTO.setFieldId(fieldId);//TODO  此处需要全局处理,并且需要根据实际数据进行替换
              cleanBizIndicatorDTO.setIndicatorValue(Double.valueOf(1));//TODO 此处需要商榷
              cleanBizIndicatorDTO.setCreateTime(new Date());
              cleanBizIndicatorDTO.setUpdateTime(new Date());
              list.add(cleanBizIndicatorDTO);
          }
          return list;
      }
        //计算季度
        public  Set<String> getQuartersBetween(DateTime startDate, DateTime endDate) {
            Set<String> quarters = new HashSet<>();
            while (DateUtil.compare(startDate,endDate)==-1) {
                String quarter =DateUtil.year(startDate) +"Q" + (DateUtil.month(startDate) / 3 + 1);
                quarters.add(quarter);
                startDate.offset(DateField.MONTH,3);
                if (startDate.isAfter(endDate)) {
                     quarter =DateUtil.year(endDate) +"Q" + (DateUtil.month(endDate) / 3 + 1);
                    quarters.add(quarter);
                }
            }

            return quarters;
        }

}
