package com.chilunyc.process.service.enterprise.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.chilunyc.process.dao.enterprise.CleanBizCompressDao;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizCompressDTO;
import com.chilunyc.process.entity.DTO.enterprise.CleanBizCompressPageDTO;
import com.chilunyc.process.service.enterprise.CleanBizCompressService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CleanBizCompressImpl extends BaseServiceImpl<CleanBizCompressDao, CleanBizCompressDTO> implements CleanBizCompressService {

    @Autowired
    CleanBizCompressDao cleanBizCompressDao;


    public IPage<CleanBizCompressPageDTO> getListByFieldAndStatus(Integer fieldId,Integer status,IPage<CleanBizCompressPageDTO> page) {


        IPage<CleanBizCompressPageDTO> list=  cleanBizCompressDao.getListByFieldAndStatus(fieldId,status, page);
        return list;
    }
}
