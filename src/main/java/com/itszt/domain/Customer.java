package com.itszt.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;


@Data
@Setter
@Getter
@ToString
public class Customer {
    private String username;
    private String sex;
    private Integer age;
    private String home;
    @JsonIgnore
    private LocalDateTime birth;
    private String time;
}
