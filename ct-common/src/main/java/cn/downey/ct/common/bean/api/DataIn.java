package cn.downey.ct.common.bean.api;

import cn.downey.ct.common.bean.entity.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface DataIn extends Closeable {

    public void setPath(String path);

    public Object read() throws IOException;

    public <T extends Data>List<T> read(Class<T> cls) throws IOException;


}
