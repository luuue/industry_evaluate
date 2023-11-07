package com.chilunyc.process.service.enterprise.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.chilunyc.process.dao.enterprise.EnterpriseDao;
import com.chilunyc.process.entity.DTO.BaseFieldDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizIndicatorDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseListDTO;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseUnionDTO;
import com.chilunyc.process.entity.DTO.rawImport.ImportResult;
import com.chilunyc.process.entity.ENUM.DataTypeEnum;
import com.chilunyc.process.service.enterprise.EnterpriseListService;
import com.chilunyc.process.service.enterprise.EnterpriseService;
import com.chilunyc.process.service.system.SysOriginalImportService;
import com.chilunyc.process.util.ExecutorBuilderUtil;
import com.chilunyc.process.util.FutureGetUtil;
import com.diboot.core.service.impl.BaseServiceImpl;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service("enterpriseServiceImpl")
public class EnterpriseServiceImpl extends BaseServiceImpl<EnterpriseDao, EnterpriseDTO> implements EnterpriseService {
    @Autowired
    private EnterpriseDao enterpriseDao;
    @Autowired
    private CleanBizIndicatorImpl bizIndicatorService;
    @Autowired
    private EnterpriseListService enterpriseListService;
    @Autowired
    SysOriginalImportService sysOriginalImportService;

    @Override
    public void calculation(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        BaseFieldDTO baseFieldDTO = fieldList.stream().findFirst().get();
        //将行业上市时间 转化为季度存入cleanBizIndicator
        getListedQuarter(baseFieldDTO.getRightFieldId());
    }


    //将行业上市时间 转化为季度存入cleanBizIndicator
    private void getListedQuarter(Integer fieldId) {
        //获取需要更新的企业信息

        List<EnterpriseUnionDTO> listedEnterprises = enterpriseDao.getListedEnterprises(fieldId);
        //cleanBizIndicator
        List<CleanBizIndicatorDTO> collect = listedEnterprises.stream().map(a -> {
            CleanBizIndicatorDTO bizIndicatorDTO = new CleanBizIndicatorDTO();
            if (Objects.nonNull(a.getIndicatorId())) {
                bizIndicatorDTO.setId(a.getIndicatorId());
            }
//            1002-1995-Q1-2027
//            INSERT INTO data_clean_entity_biz_indicator ( indicator_value, year, version, enterprise_id, quarter, state, is_delete, field_id ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )
            bizIndicatorDTO.setVersion(a.getVersion());
            bizIndicatorDTO.setYear(a.getYear());
            bizIndicatorDTO.setQuarter(a.getQuarter());
            bizIndicatorDTO.setIndicatorValue(Double.valueOf(1));//是否为当季度当季度上市
            bizIndicatorDTO.setEnterpriseId(a.getId());
            bizIndicatorDTO.setFieldId(fieldId);
            return bizIndicatorDTO;
        }).collect(Collectors.toList());
        bizIndicatorService.saveOrUpdateBatch(collect);
    }

    @Override
    public ImportResult OriginalDataImport(List<EnterpriseDTO> dataList, String endYear, String endMonth, String quarter, Integer importId, DataTypeEnum dataTypeEnum) {
        Integer insertCount=0;
        Integer updateCount=0;

        ImportResult result = new ImportResult();
        //  主体企业只能增加修改，不可删除
//        List<EnterpriseDTO> oldList = list();//查询现有企业
//        List<EnterpriseDTO> resultList = dataList.stream().map(a -> {
//            Optional<EnterpriseDTO> first = oldList.stream().filter(b -> b.getCode().equals(a.getCode())).findFirst();
//            if (first.isPresent()) {
//                a.setId(first.get().getId());
//            } else {
//                a.setId(null);
//            }
//            return a;
//        }).collect(Collectors.toList());
//        boolean b = saveOrUpdateBatch(resultList);
//        if (b) {
//            result.setInsertCount(Long.valueOf(resultList.size()));
//        } else {
//            result.setMessage("保存失败");
//            result.setCode(500);
//        }
        result.setInsertCount(Long.valueOf(dataList.size()));

//        enterpriseDao.inseetOrUpdate(dataList);
        for(EnterpriseDTO enterpriseDTO:dataList){
            if(Objects.isNull(enterpriseDTO.getAStockCode())){
                enterpriseDTO.setAStockCode("");
            }
            if(Objects.isNull(enterpriseDTO.getCreditCode())){
                enterpriseDTO.setCreditCode("");
            }
            int i = enterpriseDao.insertOrUpdateSigle(enterpriseDTO);
            if(i==1){
                insertCount++;
            }
            if(i==2){
                updateCount++;
            }
        }
        sysOriginalImportService.updateCount(0,insertCount,updateCount,dataList.size(),null,importId);
        return result;
    }
    public void updateIBCode(){
        List<EnterpriseDTO> byCodeIsNull = enterpriseDao.findByCodeIsNull();
        List<EnterpriseDTO> IBList = byCodeIsNull.stream().map(a -> {
            Integer id = a.getId();
            String s = NumberUtil.toStr(id);
            if (s.length() < 6) {
                int lenth = 6 - s.length();
                for (int i = 0; i < lenth; i++) {
                    s = "0" + s;
                }
            }
            s = "IB" + s;
            a.setCode(s);
            return a;
        }).collect(Collectors.toList());
        updateBatchById(IBList);
    }

    @Override
    public void upDateImportStatus() {
        enterpriseDao.updateImportStatusZero();
    }

    @Override
    public List<EnterpriseDTO> getListByImportStatus() {
        QueryWrapper<EnterpriseDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("import_status", 1);



        return null;
    }

    @Override
    public EnterpriseDTO getByAstockCode(String newValue) {
        QueryWrapper<EnterpriseDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("a_stock_code", newValue);
        return getSingleEntity(queryWrapper);
    }

    @Override
    public List<EnterpriseDTO> getByCreditCode(String newValue) {
        QueryWrapper<EnterpriseDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("credit_code", newValue);
        return list(queryWrapper);
    }

    @Override
    public EnterpriseDTO getByOldCode(String oldCode) {
        QueryWrapper<EnterpriseDTO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("old_code", oldCode);
        return getSingleEntity(queryWrapper);
    }

    @Override
    public EnterpriseDTO getById(Integer id) {
        return getEntity(id);
    }

    @Override
    public void calculationList(List<BaseFieldDTO> fieldList, Integer rightFieldId, List<BaseFieldDTO> enumFieldList) {
        List<EnterpriseDTO> list = enterpriseDao.findByListEnterpriseListing();
        List<Future> futureList = Lists.newArrayList();
        for (EnterpriseDTO enterpriseDTO : list) {
            Future future = ExecutorBuilderUtil.pool.submit(() -> enterpriseCalculation(enterpriseDTO));
            futureList.add(future);
        }
        FutureGetUtil.futureGet(futureList);
    }

    private void enterpriseCalculation(EnterpriseDTO enterpriseDTO) {
        List<String> list = inspectYear(enterpriseDTO.getStartMonth(), enterpriseDTO.getEndMonth());
        if (list.size() > 0) {

            String rightMQ = list.get(list.size() - 1);
            String rightQ = monthChangeQuarter(rightMQ);
            for (String yearMQ : list) {
                EnterpriseListDTO enterpriseListDTO = new EnterpriseListDTO();
                enterpriseListDTO.setEnterpriseId(enterpriseDTO.getId());
                enterpriseListDTO.setMonth(yearMQ);
                String key = monthChangeQuarter(yearMQ);
                if (!Objects.equals(rightQ, key)) {
                    String year = StrUtil.sub(key, 0, 4);
                    String mq = StrUtil.sub(key, 4, 6);
                    enterpriseListDTO.setYear(year);
                    enterpriseListDTO.setQuarter(mq);
                }
                enterpriseListDTO.setVersion(enterpriseDTO.getVersion());
                QueryWrapper<EnterpriseListDTO> queryWrapper = new QueryWrapper<>();
                queryWrapper.eq("enterprise_id", enterpriseDTO.getId()).eq("month", yearMQ);
                EnterpriseListDTO listDTO = enterpriseListService.getSingleEntity(queryWrapper);
                if (!BeanUtil.isEmpty(listDTO)) {
                    enterpriseListDTO.setId(listDTO.getId());
                } else {
                    enterpriseListDTO.setCreateTime(new Date());
                }
                enterpriseListDTO.setUpdateTime(new Date());
                enterpriseListService.createOrUpdateEntity(enterpriseListDTO);
            }

        }
    }


    /**
     * 获取周期内所有季度或月度
     *
     * @param startDate
     * @param endDate
     */
    private List<String> inspectYear(String startDate, String endDate) {
        int size = 12;
        List<String> yearMQList = Lists.newArrayList();
        if (Objects.nonNull(startDate) && Objects.nonNull(endDate)) {
            String leftYear = StrUtil.sub(startDate, 0, 4);
            String leftMQ = StrUtil.sub(startDate, 4, 6);
            String rightYear = StrUtil.sub(endDate, 0, 4);
            String rightMQ = StrUtil.sub(endDate, 4, 6);
            boolean zStatus = StrUtil.contains(leftMQ, 'Q');
            if (zStatus) {
                size = 4;
                leftMQ = StrUtil.sub(leftMQ, 1, 2);
                rightMQ = StrUtil.sub(rightMQ, 1, 2);
            }
            if (NumberUtil.isInteger(leftYear) && NumberUtil.isInteger(rightYear)) {
                for (int i = Integer.valueOf(leftYear); i <= Integer.valueOf(rightYear); i++) {

                    for (int j = 1; j <= size; j++) {
                        String mq = null;
                        if (zStatus) {
                            mq = "Q" + j;
                        } else {
                            if (j < 10) {
                                mq = "0" + j;
                            } else {
                                mq = "" + j;
                            }

                        }
                        if (Objects.equals(leftYear, rightYear)) {
                            if (j >= Integer.valueOf(leftMQ) && j <= Integer.valueOf(rightMQ)) {
                                yearMQList.add(i + mq);
                            }
                        } else {
                            if (Objects.equals(i + "", leftYear)) {
                                if (j >= Integer.valueOf(leftMQ)) {
                                    yearMQList.add(i + mq);
                                }
                            } else if (Objects.equals(i + "", rightYear)) {
                                if (j <= Integer.valueOf(rightMQ)) {
                                    yearMQList.add(i + mq);
                                }
                            } else {
                                yearMQList.add(i + mq);

                            }
                        }
                    }
                }

            }
        }
        return yearMQList;
    }

    /**
     * 月份转季度
     *
     * @param date
     * @return
     */
    private String monthChangeQuarter(String date) {
        if (Objects.nonNull(date)) {
            String year = StrUtil.sub(date, 0, 4);
            String mq = StrUtil.sub(date, 4, 6);
            Integer mqInt = Integer.valueOf(mq);
            Double value = Double.valueOf(mqInt) / 3;
            int quarter = (int) (Math.ceil(value));
            return year + "Q" + quarter;
        }
        return null;
    }




}
