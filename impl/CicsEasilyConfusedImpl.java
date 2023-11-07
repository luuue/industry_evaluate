package com.chilunyc.process.service.industry.impl;

import com.chilunyc.process.dao.industry.CicsEasilyConfusedDao;
import com.chilunyc.process.entity.DTO.industry.CicsEasilyConfusedDTO;
import com.chilunyc.process.service.industry.CicsEasilyConfusedService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("cicsEasilyConfusedImpl")
public class CicsEasilyConfusedImpl extends BaseServiceImpl<CicsEasilyConfusedDao, CicsEasilyConfusedDTO> implements CicsEasilyConfusedService {


    @Override
    public List<CicsEasilyConfusedDTO> getAllList() {
        return list();
    }
}
