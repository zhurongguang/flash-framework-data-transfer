package com.flash.framework.datatransfer.model;

import lombok.Data;

import java.util.Date;

/**
 * @author
 * @date 2019/4/25 - 下午2:20
 */
@Data
public class User {

    private Long id;

    private String name;

    private String createBy;

    private Date createdAt;

    private String updateBy;

    private Date updatedAt;

    private int deleted;

    private String extra;
}