package com.retailersv.domain;

/**
 * @Package com.retailersv.domain
 * @Author xiaoye
 * @Date 2025/8/21 18:28
 * @description:
 */
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
