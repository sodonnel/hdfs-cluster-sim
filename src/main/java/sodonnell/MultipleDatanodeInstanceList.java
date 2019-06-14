package sodonnell;

import java.util.ArrayList;
import java.util.List;

public class MultipleDatanodeInstanceList {
  
  private List<MultipleDatanodeInstance> instances = 
      new ArrayList<MultipleDatanodeInstance>();
  private List<Throwable> exceptions =
      new ArrayList<Throwable>();
  
  public synchronized List<MultipleDatanodeInstance> getInstances() {
    return instances;
  }
  
  public synchronized void addInstance(MultipleDatanodeInstance dni) {
    instances.add(dni);
  }
  
  public synchronized List<Throwable> getExceptions() {
    return exceptions;
  }
  
  public synchronized void addException(Throwable e) {
    exceptions.add(e);    
  }
  
  public synchronized boolean hasExceptions() {
    if (exceptions.size() != 0) {
      return true;
    } else {
      return false;
    }
  }

}