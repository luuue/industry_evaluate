package com.chilunyc.process.service.enterprise.impl;

import com.chilunyc.process.dao.enterprise.EnterpriseListDao;
import com.chilunyc.process.entity.DTO.enterprise.EnterpriseListDTO;
import com.chilunyc.process.service.enterprise.EnterpriseListService;
import com.diboot.core.service.impl.BaseServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class EnterpriseListImpl extends BaseServiceImpl<EnterpriseListDao, EnterpriseListDTO> implements EnterpriseListService {
}
