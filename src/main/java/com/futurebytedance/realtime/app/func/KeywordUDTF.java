package com.futurebytedance.realtime.app.func;

import com.futurebytedance.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/29 - 16:12
 * @Description 自定义UDTF函数去实现分次操作
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        //使用工具类进行分词
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
//            Row row = new Row(1);
//            row.setField(0, keyword);
//            collect(row);
            collect(Row.of(keyword));
        }
    }
}
