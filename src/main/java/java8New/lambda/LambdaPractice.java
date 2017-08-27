package java8New.lambda;

import java8New.Employee;
import org.junit.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @Package java8New
 * @Description: 练习lambda表达式
 * @Author elwyn
 * @Date 2017/6/21 21:16
 * @Email elonyong@163.com
 * <p>
 * 左侧:Lambda 表达式的参数列表
 * 右侧:Lambda 表达式中所需执行的功能,即Lambda体
 * <p>
 * 语法格式一:无参数,无返回值
 * ()-> System.out.println("Hello Lambda!")
 * <p>
 * 语法格式二:一个参数,无返回值
 * <p>
 * 语法格式三:若只有一个参数,小括号可以省略不写
 * x -> System.out.println(x);
 * <p>
 * 语法格式四:有两个以上的参数,有返回值,并且lambda体中有多条语句
 * Comparator<Integer> comparator = (x, y) -> {
 * System.out.println("函数式接口");
 * return Integer.compare(x, y);
 * };
 * 语法格式五:若Lambda 体中只有一条语句,return 和大括号都可以不写
 * Comparator<Integer> comparator = (x, y) -> Integer.compare(x, y);
 * 语法格式六:Lambda表达式的参数列表的数据类型可以省略不写,因为jvm编译器可以通过上下文推断出,数据类型,即"类型推断"
 * <p>
 * 左右遇一括号省
 * 左侧推断类型省
 * <p>
 * 二:lambda表达式需要函数式接口
 * 函数式接口:接口中只有一个方法,
 * 可以使用@FunctionInterface 检查是否是函数式接口
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
		int num = 0;//1.7之前需要加final
		Runnable r = new Runnable() {
			@Override
			public void run() {
				System.out.println("hello world" + num);
			}
		};
		r.run();
		System.out.println("----------------------------------------");
		Runnable r1 = () -> System.out.println("Hello Lambda" + num);
		r1.run();
	}

	@Test
	public void test2() {
		Consumer<String> consumer = x -> System.out.println(x);
		consumer.accept("lambda test");

	}

	@Test
	public void test3() {
		Comparator<Integer> comparator = (x, y) -> {
			System.out.println("函数式接口");
			return Integer.compare(x, y);
		};

	}

	@Test
	public void test4() {
		Comparator<Integer> comparator = (x, y) -> Integer.compare(x, y);
	}

	@Test
	public void test5() {
		Comparator<Integer> comparator = (Integer x, Integer y) -> Integer.compare(x, y);
	}


	//需求:对一个数进行运算
	@Test
	public void test6() {
		Integer operation = operation(100, (x) -> x * x);
		System.out.println(operation);
		System.out.println(operation(200, (x) -> x + 200));
	}


	public Integer operation(Integer num, MyFun myFun) {
		return myFun.getValue(num);
	}

}
