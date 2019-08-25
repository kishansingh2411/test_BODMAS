package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import config.MyFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by srisri on 15/05/15.
 * Search System
 */
public class MapperUtil {
    private static Logger log = LoggerFactory.getLogger(MapperUtil.class);

    private static ObjectMapper mapper = JsonUtil.getInstance().getMapper();
    private static JsonNode defaultNode = JsonUtil.getInstance().getDefaultNode();

    public static <T> T fromStream(InputStream stream, Class<T> valueType) {
        T request = null;
        try {
            //noinspection unchecked
            request = mapper.readValue(stream, valueType);
        } catch (IOException e) {
            log.error(MyFields.LOG_DEVELOPER, e);
        }
        return request;
    }

    public static <T> T fromStream(String filePath, Class<T> valueType) {
        InputStream stream = MapperUtil.class.getClassLoader().getResourceAsStream(filePath);
        T request = null;
        try {
            //noinspection unchecked
            request = mapper.readValue(stream, valueType);
        } catch (IOException e) {
            log.error(MyFields.LOG_DEVELOPER, e);
        } finally {
            try {
                if (stream != null)
                    stream.close();
            } catch (Exception e) {
                log.error(MyFields.LOG_DEVELOPER, e);
            }
        }
        return request;
    }

    public static <T> T fromObject(Object fromValue, Class<T> valueType) {

        T request = null;
        try {
            request = mapper.convertValue(fromValue, valueType);
        } catch (Exception e) {
            log.error(MyFields.LOG_DEVELOPER, e);
        }
        return request;
    }


    public static <T> T fromMap(Map<String, Object> map, Class<T> valueType) {
        T t;
        //            t = valueType.newInstance();
        t = mapper.convertValue(map, valueType);
        return t;
    }

    public static JsonNode getJsonNodeFromString(String s) {
        JsonNode jsonNode = defaultNode;
        try {
            jsonNode = mapper.readTree(s);
        } catch (Exception e) {
            log.error(MyFields.LOG_DEVELOPER, e);
        }
        return jsonNode;
    }

    //todo in content return, ignore sending null valued fields :)
    public static <T> JsonNode getJsonNodeFromObject(T t) {
        JsonNode jsonNode = defaultNode;
        try {
            jsonNode = mapper.valueToTree(t);
        } catch (Exception e) {
            log.error(MyFields.LOG_DEVELOPER, e);
        }
        return jsonNode;
    }


}
