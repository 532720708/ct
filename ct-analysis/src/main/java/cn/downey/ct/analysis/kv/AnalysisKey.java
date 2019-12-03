package cn.downey.ct.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分许数据key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {

    private String tel;
    private String date;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel,String date) {
        this.tel = tel;
        this.date = date;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }


    /**
     * 比较tel,date
     * @param key
     * @return
     */
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());
        if (result == 0) {
            result = date.compareTo(key.getDate());
        }
        return result;
    }

    /**
     * 写数据
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeUTF(date);
    }

    /**
     * 读数据
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        //顺序不能错
        tel = dataInput.readUTF();
        date = dataInput.readUTF();

    }
}
