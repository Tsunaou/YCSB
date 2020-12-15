package site.ycsb.jepsen;

import site.ycsb.WorkloadException;

import java.util.concurrent.atomic.AtomicInteger;

public class ClientThread implements Runnable{
  AtomicInteger counter;
  YCSBGeneratorWithCounter generator;

  public ClientThread(AtomicInteger counter, YCSBGeneratorWithCounter generator) {
    this.counter = counter;
    this.generator = generator;
  }

  @Override
  public void run() {
    for(int i=0; i<200000; i++){
      try {
        YCSBKeyValue kv = generator.nextOperation();
        if(kv.key !=null){
          this.counter.getAndIncrement();
        }
      } catch (OKCounterNotInitializedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws WorkloadException {
    int opCount = 1000000;
    double readProportion = 0.5;
    double writeProportion = 0.5;
    String requestDistrib = "uniform";

    YCSBGeneratorWithCounter generator = new YCSBGeneratorWithCounter(opCount, readProportion, writeProportion, requestDistrib, 100);
    AtomicInteger counter = new AtomicInteger(0);
    generator.initOKCounter(counter);
    int runnerCounts = 111;

    Runnable[] runners = new Runnable[runnerCounts];
    for(int i=0; i<runnerCounts; i++){
      runners[i] = new ClientThread(counter, generator);
      runners[i].run();
    }

    generator.printStatus();
  }
}
