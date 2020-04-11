package com.itszt.domain;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Setter
@Getter
@ToString
public class User implements Serializable {

    private String name;
    private Integer age;
    private LocalDateTime birth;
    private Integer sex;
    private String card;
    private String home;
    private Integer Score;

}
