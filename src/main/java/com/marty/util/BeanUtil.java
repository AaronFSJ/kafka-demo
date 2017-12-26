package com.marty.util;



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class BeanUtil {

    /**
     * @Author: Aaron
     * @Descprition: 对象转字节数组
     * @Date: Create in 2017/12/25 15:12
     * @Modyfied By:
     */
    public static byte[] ObjectToBytes(Object obj){
        byte[] bytes = null;
        ByteArrayOutputStream bo = null;
        ObjectOutputStream oo = null;
        try {
            bo = new ByteArrayOutputStream();
            oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bytes = bo.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(bo!=null){
                    bo.close();
                }
                if(oo!=null){
                    oo.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }

    /**
     * @Description 字节数组转对象
     * @param bytes 字节数组
     * @return Object Object对象
     * @author SUNBIN
     * @date 2017年9月8日
     */
    public static Object BytesToObject(byte[] bytes){
        Object obj = null;
        ByteArrayInputStream bi = null;
        ObjectInputStream oi = null;
        try {
            bi =new ByteArrayInputStream(bytes);
            oi =new ObjectInputStream(bi);
            obj = oi.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(bi!=null){
                    bi.close();
                }
                if(oi!=null){
                    oi.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return obj;
    }
}