package site.ycsb.jepsen;

import site.ycsb.WorkloadException;

import java.util.concurrent.atomic.AtomicInteger;

public class YCSBGeneratorWithCounter extends YCSBGenerator{
  AtomicInteger okCounter;

  public YCSBGeneratorWithCounter(int opCount, double readProportion, double writeProportion, String requestDistrib, int uniformMax) throws WorkloadException {
    super(opCount, readProportion, writeProportion, requestDistrib, uniformMax);
    this.okCounter = null;
  }

  public boolean initOKCounter(AtomicInteger atomicCounter){
    if(atomicCounter.get()!=0){
      return false;
    }else{
      this.okCounter = atomicCounter;
      return true;
    }
  }

  @Override
  public YCSBKeyValue nextOperation() throws OKCounterNotInitializedException {
    if(this.okCounter == null){
      throw new OKCounterNotInitializedException("OKCounter is not initialize");
    }

    YCSBKeyValue kv = super.nextOperation();
    Integer key = kv.getKey();
    Integer value = kv.getValue();
    if(this.okCounter.get() >= this.opCount){
      // type:ok的操作数已经达到最大的规定操作数类
      // 清理类中计数器
      this.keyCounter[key]--;
      if(value == null){
        this.readKeyCounter[key]--;
        this.realReadCount--;
      }else{
        this.writeKeyCounter[key]--;
        this.realWriteCount--;
      }
      // 返回空值
      return new YCSBKeyValue(null, null);
    }else{
      return kv;
    }
  }




  public static void main(String[] args) throws WorkloadException, OKCounterNotInitializedException {
    int opCount = 1000000;
    double readProportion = 0.5;
    double writeProportion = 0.5;
    String requestDistrib = "uniform";

    YCSBGeneratorWithCounter generator = new YCSBGeneratorWithCounter(opCount, readProportion, writeProportion, requestDistrib, 100);
    AtomicInteger counter = new AtomicInteger(0);
    generator.initOKCounter(counter);
    int realOpCounter = 0;
    for (int i = 0; i < 2*opCount; i++) {
      YCSBKeyValue kv = generator.nextOperation();
      if(kv.getKey() == null){
        System.out.println("Null");
      }else{
        counter.getAndIncrement();
        System.out.println(counter.get());
        realOpCounter++;
      }
    }
    System.out.println("Really get " + realOpCounter + " operations");

    generator.printStatus();

  }

}
