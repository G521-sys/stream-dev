package com.retailersv1.dws.comment;

import com.retailersv1.dws.utils.KeyWordsUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordsUDTF extends TableFunction<Row>{
    public void eval(String text)
    {
        for (String word : KeyWordsUtils.analyze(text))
        {
            collect(Row.of(word));
        }
    }
}
