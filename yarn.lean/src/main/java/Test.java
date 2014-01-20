
public class Test {

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub
    System.out.println("ip:" + intToIp(Integer.valueOf(args[0])).toString());
  }

  public static String intToIp(int ipInt) {
    return new StringBuilder().append(((ipInt >> 24) & 0xff)).append('.')
        .append((ipInt >> 16) & 0xff).append('.').append((ipInt >> 8) & 0xff).append('.')
        .append((ipInt & 0xff)).toString();
  }
}
