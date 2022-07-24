package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词器工具类
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.utils
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/17 23:07
 */
public class KeywordUtil {
    public static List<String> analyze(String text) throws IOException {
        List<String> keywordList = new ArrayList<>();
        //将string转成stringReader流
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme lexeme = null;
        while ((lexeme = ikSegmenter.next()) != null) {
            String keyword = lexeme.getLexemeText();
            keywordList.add(keyword);
        }
        return keywordList;
    }
}
