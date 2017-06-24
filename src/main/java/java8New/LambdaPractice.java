package java8New;

import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @Package java8New
 * @Description: 练习lambda表达式
 * @Author elwyn
 * @Date 2017/6/21 21:16
 * @Email elonyong@163.com
 */
public class LambdaPractice {

    List<Employee> employees = Arrays.asList(
            new Employee("张三", 5555.66, 50),
            new Employee("张er", 5355.66, 50),
            new Employee("李四", 3555.66, 20),
            new Employee("王五", 4555.66, 350),
            new Employee("赵柳", 6555.66, 450)
    );

    @Test
    public void test1() {
        employees.sort((o1, o2) -> {
            if (o1.getAge() == o2.getAge()) {
                return o1.getName().compareTo(o2.getName());
            } else {
                return Integer.compare(o1.getAge(), o2.getAge());
            }
        });
        for (Employee employee : employees) {
            System.out.println(employee);
        }
    }

    @Test
    public void test2(){
        String s="fkdsfdsa";

        Function<String,String> function= String::toUpperCase;

        System.out.println(getString(s,function));

    }


    public String getString(String s ,Function function) {
        return (String) function.apply(s);
    }
}
