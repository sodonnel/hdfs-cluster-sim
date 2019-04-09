package sodonnell;

import java.util.ArrayList;
import java.util.List;

public class MultipleDatanodeInstanceList {
  
  private List<MultipleDatanodeInstance> instances = 
      new ArrayList<MultipleDatanodeInstance>();
  private List<Throwable> exceptions =
      new ArrayList<Throwable>();
  
  public List<MultipleDatanodeInstance> getInstances() {
    return instances;
  }
  
  public void addInstance(MultipleDatanodeInstance dni) {
    instances.add(dni);
  }
  
  public List<Throwable> getExceptions() {
    return exceptions;
  }
  
  public void addException(Throwable e) {
    exceptions.add(e);    
  }
  
  public boolean hasExceptions() {
    if (exceptions.size() != 0) {
      return true;
    } else {
      return false;
    }
  }

}