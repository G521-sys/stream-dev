package com.stream.common.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    String sourceTable;
    String sourceType;
    String sinkTable;
    String sinkColumns;
    String op;
}
