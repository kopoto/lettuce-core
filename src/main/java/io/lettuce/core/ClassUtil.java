package io.lettuce.core;

import java.lang.reflect.Field;

/**
 * @author yinghonghui
 * @version 1.0
 * @date 2019/12/27 10:28
 */
public class ClassUtil {

    /**
     * 通过反射获取对象属性
     *
     * @param target
     * @param fieldName
     * @return
     * @throws IllegalAccessException
     */
    public static Object fetchPrivateField(Object target, String fieldName) {
        Class className = target.getClass();
        for (; className != Object.class; className = className.getSuperclass()) {//获取本身和父级对象
            Field[] fields = className.getDeclaredFields();//获取所有私有字段
            for (Field field : fields) {
                if (field.getName().equals(fieldName)) {
                    field.setAccessible(true);
                    try {
                        return field.get(target);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return null;
    }


    public static class Student {
        public Student(String name, String year) {
            this.name = name;
            this.year = year;
        }

        private String name;
        private String year;
    }

    public static void main(String[] args) throws IllegalAccessException {
        Student student = new Student("test", "11");
        Object name = fetchPrivateField(student, "name");
        if (name != null) {
            System.out.println(name);
        }
        Object op = fetchPrivateField(student, "op");
        if (op != null) {
            System.out.println(op);
        }
    }

}
