package com.microsoft.reef.simple;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.client.ClientConfigurationOptions;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.JobMessage;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.utils.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.wake.EventHandler;

public class Client {

  @NamedParameter(short_name="application_archive", doc="The archive containing all the application master files")
  public static class AppArchive implements Name<File> {}

  @NamedParameter(short_name="application_args", doc="Application master arguments")
  public static class AppArgs implements Name<String> {}

  @NamedParameter(short_name="application_master", doc="Application master implementation class")
  public static class AppClass implements Name<ApplicationMaster> {}

  @NamedParameter(short_name="application_jar", doc="Application master jar", default_value="")
  public static class AppJar implements Name<File> {}

  @NamedParameter(short_name="container_memory", doc="Amount of memory in MB to be requested to run the task", default_value="64")
  public static class ContainerMemory implements Name<Integer> {}

  @NamedParameter(short_name="container_priority", doc="Priority for the containers")
  public static class ContainerPriority implements Name<Integer> {}

  @NamedParameter(short_name="num_containers", doc="No. of containers required for running the application tasks", default_value="1")
  public static class NumContainers implements Name<Integer> {}

  @NamedParameter(doc="Task implementation class")
  public static class TaskClass implements Name<ApplicationTask> {}

  @NamedParameter(short_name="task_jar", doc="Task jar", default_value="")
  public static class TaskJar implements Name<File> {}

  @NamedParameter()
  public static class TaskArgs implements Name<String> {}

  @NamedParameter(short_name="local", default_value="true")
  public static class Local implements Name<Boolean> { }
  
  private static final Logger LOG = Logger.getLogger(Client.class.getName());

//  private final Class<?> appClass;
  private final File appJar;
  private final String appArgs;
  private final File appArchive;
//  private final Class<?> taskClass;
  private final File taskJar;
  private final int containerMemory;
  private final int numContainers;
  private final Injector injector;
  private final Configuration runtimeConf;
  private final Configuration cmdLineConf;

  @Inject
  public Client(
      Injector injector,
      Configuration cmdLineConf,
//      @Parameter(AppJar.class) File appJar, // optional
      @Parameter(AppArgs.class) String appArgs,
//      @Parameter(AppArchive.class) File appArchive, // optional
//      @Parameter(TaskJar.class) File taskJar, // optional
      
//      @Parameter(ContainerPriority.class) int containerPriority,
      @Parameter(ContainerMemory.class) int containerMemory,
      @Parameter(NumContainers.class) int numContainers,
      @Parameter(Local.class) boolean local
      ) throws BindException {
     this.injector = injector.forkInjector();
     this.cmdLineConf = cmdLineConf;
     //this.runAppTask = runAppTask;
//     this.appClass = appClass;
     this.appJar = null;//appJar;
     if(appJar != null) {
       if(!(appJar.isFile()&&appJar.canRead())) {
         throw new IllegalArgumentException("AppJar must be a readable file.  Got: " + appJar);
       }
     }
     this.appArgs = appArgs;
     this.appArchive = null; //appArchive;
//     this.taskClass = taskClass;
     this.taskJar = null; //taskJar;
     if(taskJar != null) {
       if(!(taskJar.isFile()&&taskJar.canRead())) {
         throw new IllegalArgumentException("AppJar must be a readable file.  Got: " + taskJar);
       }
     }
     this.containerMemory = containerMemory;
     this.numContainers = numContainers;
     if(!injector.isInjectable(AppClass.class)) {
       throw new IllegalArgumentException("ApplicationMaster is not injectable!\n " + injector.getInjectionPlan(AppClass.class).toCantInjectString());
       
     }
     JavaConfigurationBuilder clientConf = 
         Tang.Factory.getTang().newConfigurationBuilder();
     clientConf.bindNamedParameter(ClientConfigurationOptions.JobMessageHandler.class,
         Client.JobMessageHandler.class);
//       ClientConfiguration.CONF
//       .set(ClientConfiguration.ON_JOB_MESSAGE, Client.JobMessageHandler.class)
//       .build();

     if(local) {
       this.runtimeConf = Tang.Factory.getTang().newConfigurationBuilder(clientConf.build(),
           LocalRuntimeConfiguration.CONF
         .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, numContainers)
         .build()).build();
     } else {
       this.runtimeConf = Tang.Factory.getTang().newConfigurationBuilder(clientConf.build(),
           YarnClientConfiguration.CONF
           .set(YarnClientConfiguration.REEF_JAR_FILE, EnvironmentUtils.getClassLocationFile(REEF.class))
           .build()).build();
     }
  }
  public LauncherStatus run() throws Exception {
    final JavaConfigurationBuilder driverConf = Tang.Factory.getTang().newConfigurationBuilder(
        EnvironmentUtils.addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "SimpleDriver")
          .set(DriverConfiguration.ON_DRIVER_STARTED, SimpleDriver.StartHandler.class)
          //.set(DriverConfiguration.ON_DRIVER_STOP, SimpleDriver.StopHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SimpleDriver.EvaluatorAllocatedHandler.class)
          //.set(DriverConfiguration.ON_EVALUATOR_COMPLETED, SimpleDriver.EvaluatorCompletedHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_FAILED, SimpleDriver.EvaluatorFailedHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_MESSAGE, SimpleDriver.ActivityMessageHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_COMPLETED, SimpleDriver.ActivityCompletedHandler.class)
          .set(DriverConfiguration.ON_ACTIVITY_FAILED, SimpleDriver.ActivityFailedHandler.class)
          //.set(DriverConfiguration.ON_ACTIVITY_RUNNING, SimpleDriver.ActivityRunningHandler.class)
          //.set(DriverConfiguration.ON_ACTIVITY_SUSPENDED, SimpleDriver.ActivityRunningHandler.class)
          .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SimpleDriver.ContextActiveHandler.class)
          //.set(DriverConfiguration.ON_CONTEXT_CLOSED, SimpleDriver.ContextClosedHandler.class)
          .set(DriverConfiguration.ON_CONTEXT_FAILED, SimpleDriver.ContextFailedHandler.class)
          //.set(DriverConfiguration.ON_CONTEXT_MESSAGE, SimpleDriver.ContextMessageHandler.class)
          //.set(DriverConfiguration.ON_CLIENT_MESSAGE, SimpleDriver.ClientMessageHandler.class)
          //.set(DriverConfiguration.ON_CLIENT_CLOSED, SimpleDriver.ClientClosedHandler.class)
          //.set(DriverConfiguration.ON_CLIENT_CLOSED_WITH_MESSAGE, SimpleDriver.ClientClosedWithMessageHandler.class)
        .build(),
        cmdLineConf
        );
    return DriverLauncher.getLauncher(runtimeConf).run(driverConf.build());

  }
  public static final class JobMessageHandler implements EventHandler<JobMessage> {
    @Inject
    JobMessageHandler() { 
    }
    @Override
    public void onNext(JobMessage arg0) {
      try {
        System.out.write(arg0.get());
        System.out.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
  }
  public static void main(String[] args) throws Exception {
    Tang t = Tang.Factory.getTang();
    JavaConfigurationBuilder cb = t.newConfigurationBuilder();
    CommandLine cl = new CommandLine(cb);
    cl.processCommandLine(args,
        AppArchive.class, AppArgs.class, AppClass.class, AppJar.class, ContainerMemory.class, ContainerPriority.class,
        NumContainers.class, TaskJar.class, Local.class);
    Injector i = t.newInjector(cb.build());
    i.bindVolatileInstance(Configuration.class, cb.build());
    final LauncherStatus status = i.getInstance(Client.class).run();
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
    System.exit(0);
  }
}

