package com.itszt.common;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class ResultData {

    private Integer code = 0;
    private String message = "success";
    private Object data;


}
