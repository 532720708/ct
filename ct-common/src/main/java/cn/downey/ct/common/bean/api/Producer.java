package cn.downey.ct.common.bean.api;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {

    public void SetIn(DataIn in);

    public void SetOut(DataOut out);

    /**
     * 生产接口
     */
    public void produce();
}
