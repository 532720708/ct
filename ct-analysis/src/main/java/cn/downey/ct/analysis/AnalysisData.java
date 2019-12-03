package cn.downey.ct.analysis;

import cn.downey.ct.analysis.tool.AnalysisBeanTool;
import cn.downey.ct.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
        //int result = ToolRunner.run(new AnalysisTextTool(), args);
        int result = ToolRunner.run(new AnalysisBeanTool(), args);
    }
}
