package com.caron.gmall.publisher.service;

import java.util.Map;

/**
 * @author Caron
 * @create 2020-07-24-11:19
 * @Description
 * @Version
 */
public interface DauService {
    public Long getDauTotal(String date);
    //日活分时
    public Map getDauHourCount(String date);
}
