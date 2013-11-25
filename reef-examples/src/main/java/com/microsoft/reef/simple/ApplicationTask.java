package com.microsoft.reef.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import com.microsoft.reef.activity.ActivityMessage;
import com.microsoft.reef.activity.events.DriverMessage;

public abstract class ApplicationTask {
  // XXX this is grossly non-performant!
  private final List<Byte> buf = new ArrayList<Byte>(); 
  protected final PrintStream out = new PrintStream(new OutputStream() {
    
    @Override
    public synchronized void write(int arg0) throws IOException {
      buf.add((byte)arg0);
    }
    @Override
    public synchronized void write(byte[] arg0) throws IOException {
      for(int i = 0; i < arg0.length; i++) {
        buf.add(arg0[i]);
      }
    }
  });  
  public abstract void run(String appArgs) throws Exception;

  public byte[] getMessageForDriver() {
    if(buf.size() != 0) {
      byte[] b = new byte[buf.size()];
      for(int i = 0; i < b.length; i++) {
        b[i] = buf.get(i);
      }
      buf.clear();
      return b;
    } else {
      return null;
    }
  }

  public void onDriverMessageRecieved(DriverMessage arg0) {
  }
}
