package builder;

/**
 * @Package builder
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/6/18 11:49
 * @Email elonyong@163.com
 */
public class TestBuilder {
   /* public static void main(String[] args) {
        NutritionFacts nutritionFacts =
                new NutritionFacts.Builder(240, 8)
                        .carbohydrate(1).fat(8).sodium(100).bulid();
        System.out.println("nutritionFacts = " + nutritionFacts);
    }*/
   String s;
   public static void main(String[] args) {
      /* TestBuilder myObject = null;
       myObject.iAmStaticMethod();
       myObject.iAmNonStaticMethod();*/
      new TestBuilder(). iAmStaticMethod();
   }

    private  void iAmStaticMethod(){
        System.out.println(s.toString());
    }

    private void iAmNonStaticMethod(){
        System.out.println("I am NON static method, don't date to call me by null");
    }
}
