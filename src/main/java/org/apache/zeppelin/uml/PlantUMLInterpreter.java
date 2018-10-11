/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.uml;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.sourceforge.plantuml.FileFormat;
import net.sourceforge.plantuml.FileFormatOption;
import net.sourceforge.plantuml.SourceStringReader;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.KerberosInterpreter;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shell interpreter for Zeppelin.
 */
public class PlantUMLInterpreter extends KerberosInterpreter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlantUMLInterpreter.class);

  private static final String TIMEOUT_PROPERTY = "plantuml.render.timeout.millisecs";
  private static final String DEFAULT_TIMEOUT_PROPERTY = "60000";
  private static final boolean isWindows = System.getProperty("os.name").startsWith("Windows");
  private static final String shell = isWindows ? "cmd /c" : "bash -c";

  private ConcurrentHashMap<String, Future<String>> executors;
  private ExecutorService executorService;
  private long renderTimeout = 60000L;

  public PlantUMLInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    super.open();
    executors = new ConcurrentHashMap<>();
    executorService = Executors.newCachedThreadPool();
    renderTimeout = Long.valueOf(getProperty(TIMEOUT_PROPERTY, DEFAULT_TIMEOUT_PROPERTY));
  }

  @Override
  public void close() {
    super.close();
    for (String executorKey : executors.keySet()) {
      final Future<String> executor = executors.remove(executorKey);
      if (executor != null) {
        try {
          executor.cancel(true);
        } catch (Exception e){
          LOGGER.error("error destroying executor for paragraphId: " + executorKey, e);
        }
      }
    }
  }


  @Override
  public InterpreterResult interpret(final String originalCmd,
                                     final InterpreterContext contextInterpreter) {
    try {
      final Future<String> future = executorService.submit(new Callable<String>() {
        @Override
        public String call() throws Exception {
          final SourceStringReader reader = new SourceStringReader(originalCmd);
          final ByteArrayOutputStream os = new ByteArrayOutputStream();
          final FileFormatOption fileFormatOption = new FileFormatOption(FileFormat.SVG);
          fileFormatOption.hideMetadata();
          reader.outputImage(os, fileFormatOption);
          os.close();
          return new String(os.toByteArray(), StandardCharsets.UTF_8);
        }
      });
      executors.put(contextInterpreter.getParagraphId(), future);
      return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.HTML,
              future.get(renderTimeout, TimeUnit.MILLISECONDS));
    } catch (ExecutionException e) {
      return new InterpreterResult(Code.ERROR, e.getCause().getLocalizedMessage());
    } catch (InterruptedException e) {
      return new InterpreterResult(Code.ERROR, "Rendering process is interrupted!");
    } catch (TimeoutException e) {
      return new InterpreterResult(Code.ERROR, "Rendering process is timed-out!");
    } finally {
      executors.remove(contextInterpreter.getParagraphId());
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    final Future<String> executor = executors.remove(context.getParagraphId());
    if (executor != null) {
      try {
        executor.cancel(true);
      } catch (Exception e){
        LOGGER.error("error destroying executor for paragraphId: " + context.getParagraphId(), e);
      }
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
            this.getClass().getName() + this.hashCode(), 10);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) {
    return null;
  }

  @Override
  protected boolean runKerberosLogin() {
    try {
      createSecureConfiguration();
      return true;
    } catch (Exception e) {
      LOGGER.error("Unable to run kinit for zeppelin", e);
    }
    return false;
  }

  public void createSecureConfiguration() throws InterpreterException {
    Properties properties = getProperties();
    CommandLine cmdLine = CommandLine.parse(shell);
    cmdLine.addArgument("-c", false);
    String kinitCommand = String.format("kinit -k -t %s %s",
        properties.getProperty("zeppelin.shell.keytab.location"),
        properties.getProperty("zeppelin.shell.principal"));
    cmdLine.addArgument(kinitCommand, false);
    DefaultExecutor executor = new DefaultExecutor();
    try {
      executor.execute(cmdLine);
    } catch (Exception e) {
      LOGGER.error("Unable to run kinit for zeppelin user " + kinitCommand, e);
      throw new InterpreterException(e);
    }
  }

  @Override
  protected boolean isKerboseEnabled() {
    if (!StringUtils.isAnyEmpty(getProperty("zeppelin.shell.auth.type")) && getProperty(
        "zeppelin.shell.auth.type").equalsIgnoreCase("kerberos")) {
      return true;
    }
    return false;
  }

}
