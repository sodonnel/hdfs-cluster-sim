package sodonnell;

import org.apache.log4j.Logger;

import java.io.Console;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

public class CommandShell {

  private final static Logger LOG = Logger.getLogger(CommandShell.class);
  
  public static void main(String[] args) {
    CommandShell shell = new CommandShell();
    shell.run();
  }
  
  MultipleDatanodeCommandFactory commandFactory;
  
  CommandShell() {
    commandFactory = new MultipleDatanodeCommandFactory();    
  }
  
  public void run() {
    Console console = System.console();
    
    if (console == null) {
      System.err.println("Couldn't get Console instance");
      System.exit(0);
    }
    
    while(true) {
      String fullCmd = console.readLine("> ").trim();
      if (!fullCmd.isEmpty()) {
        CommandShellCommand cmd = commandFactory.getCommand(fullCmd);
        if (cmd != null) {
          cmd.execute();
        }
        String messages = cmd.getMessages();
        if (messages != null) {
          console.printf(messages+"\n");
        }
      }
    }
  }
  
  private class MultipleDatanodeCommandFactory {
    
 //   HashMap<String, String> commands = new HashMap<String, String>()
 //   {{
 //     put("help", "CommandFactory.Command");
 //   }};
    
    protected MultipleDatanode mdn;
    
    public MultipleDatanodeCommandFactory() {
      mdn = new MultipleDatanode();
    }
    
    public Command getCommand(String fullCmd) {
      String cmd = fullCmd.split("\\s+")[0];
      
      switch (cmd) {
        case "start":
          return new StartDatanodes(fullCmd, mdn);
        case "stop":
          return new StopDatanodes(fullCmd, mdn);
        case "help":
          return new HelpCommand(fullCmd, mdn);        
        default:
          return new UnknownCommand(fullCmd, mdn);        
      }
    }
  }
  
  private abstract class Command implements CommandShellCommand {
    protected String fullCmd;
    protected String messages;
    protected MultipleDatanode mdn;
    
    public Command(String cmd, MultipleDatanode multipleDn) {
      fullCmd = cmd;
      mdn = multipleDn;
    }
    
    public abstract boolean execute();
    
    public String getMessages() {
      return messages;
    }
    
    protected String[] commandParts() {
      return fullCmd.split("\\s+");
    }
    
    protected String stringifyStackTrace(Throwable e) {
      try {
        StringWriter sw = null;
        PrintWriter pw = null;
        try {
          sw = new StringWriter();
          pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          return sw.toString();
        } finally {
          pw.close();
          sw.close();
        }
      } catch (IOException ioe) {
        return "Error converting stack trace to string: " + ioe.toString();
      }
    }
  }
  
  private class HelpCommand extends Command {
    
    public HelpCommand(String cmd, MultipleDatanode mdn) {
      super(cmd, mdn);
    }
    
    public boolean execute() {
      messages = "Called the help command";
      return true;
    }
  }
  
  private class UnknownCommand extends Command {
    
    public UnknownCommand(String cmd, MultipleDatanode mdn) {
      super(cmd, mdn);
    }
    
    public boolean execute() {      
      messages = "Unknown command: " + commandParts()[0];
      return true;
    }
  }
  
  private class StartDatanodes extends Command {
    
    protected String usage = "start #<individual datanode id>\n" +
        "start <count of Datanodes to start>\n" +
        "eg:\nstart #4 to start a DN with ID 4\n" +
        "start 10 to start 10 DNs";
        
    public StartDatanodes(String cmd, MultipleDatanode mdn) {
      super(cmd, mdn);
    }
    
    public boolean execute() {
      String[] parts = commandParts();
      if (parts[1].matches("^#\\d+$")) {
        // eg start #4
        int dnNum = Integer.parseInt(parts[1].substring(1));
        startSingleInstance(dnNum);
      } else if (parts[1].matches("^\\d+$")) {
        int num = Integer.parseInt(parts[1]);
        startManyInstances(num);
      } else {
        messages = usage;
        return false;
      }
      return true;
    }
      
    private void startSingleInstance(int dnId) {
      try {
        MultipleDatanodeInstance i = mdn.startDataNode(dnId);
        messages = "Started "+i.toString()+"\n";
      } catch (MultipleDatanodeRunningException e) {
        messages = e.getMessage();
      } catch (MultipleDatanodeException e) {
        // TODO - include stack trace
        messages = e.getMessage();
      }
    }
    
    private void startManyInstances(int numToStart) {
      MultipleDatanodeInstanceList results = mdn.startDatanodes(numToStart);
      StringBuilder sb = new StringBuilder();
      for (MultipleDatanodeInstance i : results.getInstances()) {
        sb.append("Started "+i.toString()+"\n");
      }
      for (Throwable e : results.getExceptions()) {
        sb.append("Error: "+e.getMessage());
        sb.append(stringifyStackTrace(e));
      }
      messages = sb.toString();
    }
  }
  
  private class StopDatanodes extends Command {
    
    protected String usage = "stop dnId|port|all\n";
        
    public StopDatanodes(String cmd, MultipleDatanode mdn) {
      super(cmd, mdn);
    }
    
    public boolean execute() {
      // TODO - handle ALL and PORTS
      String[] parts = commandParts();
      if (parts[1].matches("^\\d+$")) {
        // eg stop 5
        int dnNum = Integer.parseInt(parts[1]);
        try {
          MultipleDatanodeInstance i = mdn.stopDataNode(dnNum);
          messages = "Stopped "+i.toString()+"\n";
        } catch (MultipleDatanodeRunningException e) {
          messages = e.getMessage();
        } catch (MultipleDatanodeException e) {
          // TODO - include stacktrace
          messages = e.getMessage();
        }
      } else if (parts[1].matches("^all$")) {
        // eg stop all
        
      } else {
        messages = usage;
        return false;
      }
      return true;
    }
  }

}