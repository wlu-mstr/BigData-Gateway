package datasource.kafka;


import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-12
 */
public class ZKStringSerializer implements ZkSerializer {

    private static final ZKStringSerializer instance = new ZKStringSerializer();

    private ZKStringSerializer() {
    }

    public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data == null) {
            return null;
        }
        try {
            return ((String)data).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            return bytes == null ? null : new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    /**
     * @return the instance
     */
    public static ZKStringSerializer getInstance() {
        return instance;
    }

}