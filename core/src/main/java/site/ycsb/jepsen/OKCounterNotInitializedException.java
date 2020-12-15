package site.ycsb.jepsen;

public class OKCounterNotInitializedException extends Exception{
  public OKCounterNotInitializedException(String s) {
    super(s);
  }
}
