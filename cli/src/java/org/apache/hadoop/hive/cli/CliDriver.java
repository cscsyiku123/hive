/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.cli;

import com.google.common.base.Splitter;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.ArgumentCompleter.AbstractArgumentDelimiter;
import jline.console.completer.ArgumentCompleter.ArgumentDelimiter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.PersistentHistory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveInterruptUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.cli.ShellCmdExecutor;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.FetchConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * CliDriver.
 */
public class CliDriver {

    public static String prompt = null;
    public static String prompt2 = null; // when ';' is not yet seen
    public static final int LINES_TO_FETCH = 40; // number of lines to fetch in batch from remote hive server
    public static final int DELIMITED_CANDIDATE_THRESHOLD = 10;

    public static final String HIVERCFILE = ".hiverc";

    private final LogHelper console;
    protected ConsoleReader reader;
    private Configuration conf;
    private String[] args;

    // TODO_MA 注释：CliDriver构造器
    public CliDriver() {
        // TODO_MA 注释： 一个线程一个会话
        SessionState ss = SessionState.get();

        conf = (ss != null) ? ss.getConf() : new Configuration();
        Logger LOG = LoggerFactory.getLogger("CliDriver");
        if(LOG.isDebugEnabled()) {
            LOG.debug("CliDriver inited with classpath {}", System.getProperty("java.class.path"));
        }
        console = new LogHelper(LOG);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 处理一个独立的 HQL 的解析和执行工作。
     *  1、exit;  quit;
     *  2、source /opt/sql/xxx.sql
     *  3、select ... form ...
     *  4、...
     */
    public int processCmd(String cmd) {
        CliSessionState ss = (CliSessionState) SessionState.get();
        ss.setLastCommand(cmd);
        ss.updateThreadName();

        // Flush the print stream, so it doesn't include output from the last command
        ss.err.flush();

        String cmd_trimmed = cmd.trim();
        String[] tokens = tokenizeCmd(cmd_trimmed);
        int ret = 0;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 接下来是正儿八经的 SQL 命令的处理， 有四种常见的情况：
         *  1、select ...
         *  2、exit  quit
         *  3、source  ....
         *  4、!shell
         */

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 如果是 quit/exit 命令
         */
        if(cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {

            // if we have come this far - either the previous commands
            // are all successful or this is command line. in either case
            // this counts as a successful run
            ss.close();  // SessionState
            System.exit(0);


            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 如果是 source 命令
             */
        } else if(tokens[0].equalsIgnoreCase("source")) {
            String cmd_1 = getFirstCmd(cmd_trimmed, tokens[0].length());
            cmd_1 = new VariableSubstitution(new HiveVariableSource() {
                @Override
                public Map<String, String> getHiveVariable() {
                    return SessionState.get().getHiveVariables();
                }
            }).substitute(ss.getConf(), cmd_1);

            File sourceFile = new File(cmd_1);
            if(!sourceFile.isFile()) {
                console.printError("File: " + cmd_1 + " is not a file.");
                ret = 1;
            } else {
                try {
                    // TODO_MA 注释：处理一个文件： source file;
                    ret = processFile(cmd_1);
                } catch(IOException e) {
                    console.printError("Failed processing file " + cmd_1 + " " + e.getLocalizedMessage(), stringifyException(e));
                    ret = 1;
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： hive是支持在 hive交互界面去执行 HDFS 和一些 shell 操作命令的
             */
        } else if(cmd_trimmed.startsWith("!")) {

            String shell_cmd = cmd_trimmed.substring(1);
            shell_cmd = new VariableSubstitution(new HiveVariableSource() {
                @Override
                public Map<String, String> getHiveVariable() {
                    return SessionState.get().getHiveVariables();
                }
            }).substitute(ss.getConf(), shell_cmd);

            // TODO_MA 注释： 处理和执行一个 shell 操作
            // shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
            try {
                ShellCmdExecutor executor = new ShellCmdExecutor(shell_cmd, ss.out, ss.err);
                ret = executor.execute();
                if(ret != 0) {
                    console.printError("Command failed with exit code = " + ret);
                }
            } catch(Exception e) {
                console.printError("Exception raised from Shell command " + e.getLocalizedMessage(), stringifyException(e));
                ret = 1;
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 处理正儿八经的 SQL 命令 SQL select 语句
             */
        } else { // local mode
            try {
                // TODO_MA 注释：获取处理器
                CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) conf);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： processLocalCmd方法的逻辑：proc处理cmd
                 *  1、cmd SQL（create....  desc....  select ..... insert ....）
                 *  2、proc 处理器
                 *  3、ss 会话
                 */
                ret = processLocalCmd(cmd, proc, ss);

            } catch(SQLException e) {
                console.printError("Failed processing command " + tokens[0] + " " + e.getLocalizedMessage(),
                        org.apache.hadoop.util.StringUtils.stringifyException(e));
                ret = 1;
            }
        }
        ss.resetThreadName();

        // TODO_MA 注释： 返回状态结果
        return ret;
    }

    /**
     * For testing purposes to inject Configuration dependency
     *
     * @param conf to replace default
     */
    void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Extract and clean up the first command in the input.
     */
    private String getFirstCmd(String cmd, int length) {
        return cmd.substring(length).trim();
    }

    private String[] tokenizeCmd(String cmd) {
        return cmd.split("\\s+");
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 核心逻辑分四步走
     *  1、执行 SQL 拿到结果
     *  2、输出 hive.cli.print.header
     *  3、输出 结果数据
     *  4、输出任务执行信息
     */
    /**
     * // TODO_MA 注释：完整的完成一个 HQL 的命令的执行
     * // TODO_MA 注释：这段代码最大的作用，就是帮你不停的重试去执行某个操作，直到达到最大重试次数
     *
     * @param cmd
     * @param proc
     * @param ss
     * @return
     */
    int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss) {
        int tryCount = 0;
        boolean needRetry;
        int ret = 0;

        // TODO_MA 注释：一直重试， 直到 needRetry 为 false
        do {
            try {
                needRetry = false;
                if(proc != null) {

                    // TODO_MA 注释：CommandProcessor 就是 Driver, 所以走这个分支
                    if(proc instanceof Driver) {
                        Driver qp = (Driver) proc;
                        PrintStream out = ss.out;
                        long start = System.currentTimeMillis();
                        if(ss.getIsVerbose()) {
                            out.println(cmd);
                        }
                        qp.setTryCount(tryCount);

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 11111 执行HQL命令，拿到结果
                         *  CommandProcessor   Driver   qp
                         *  如果 ret有值了，这个方法返回了。就证明对应的SQL的执行结果已经有了
                         *  Driver qp ===> diver.run(sql)
                         */
                        ret = qp.run(cmd).getResponseCode();

                        if(ret != 0) {
                            qp.close();
                            return ret;
                        }

                        // query has run capture the time
                        long end = System.currentTimeMillis();
                        double timeTaken = (end - start) / 1000.0;

                        ArrayList<String> res = new ArrayList<String>();

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：22222 如果设置了
                         *  set hive.cli.print.header=true;  则会输出字段名信息
                         */
                        printHeader(qp, out);

                        // print the results
                        int counter = 0;
                        try {
                            if(out instanceof FetchConverter) {
                                ((FetchConverter) out).fetchStarted();
                            }

                            /*************************************************
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释：33333 输出 HQL 执行结果
                             */
                            while(qp.getResults(res)) {
                                // TODO_MA 注释: 这是真正的结果打印！
                                for(String r : res) {
                                    out.println(r);
                                }

                                counter += res.size();
                                res.clear();
                                if(out.checkError()) {
                                    break;
                                }
                            }
                        } catch(IOException e) {
                            console.printError("Failed with exception " + e.getClass().getName() + ":" + e.getMessage(),
                                    "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
                            ret = 1;
                        }

                        int cret = qp.close();
                        if(ret == 0) {
                            ret = cret;
                        }

                        if(out instanceof FetchConverter) {
                            ((FetchConverter) out).fetchFinished();
                        }

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：44444 每个 HQL 语句执行完成之后，有一个 执行耗时的提示，就是在这儿执行的。
                         */
                        console.printInfo("Time taken: " + timeTaken + " seconds" + (counter == 0 ? "" : ", " + "Fetched: " + counter + " row(s)"));

                    } else {
                        String firstToken = tokenizeCmd(cmd.trim())[0];
                        String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

                        if(ss.getIsVerbose()) {
                            ss.out.println(firstToken + " " + cmd_1);
                        }
                        CommandProcessorResponse res = proc.run(cmd_1);
                        if(res.getResponseCode() != 0) {
                            ss.out.println("Query returned non-zero code: " + res.getResponseCode() + ", cause: " + res.getErrorMessage());
                        }
                        if(res.getConsoleMessages() != null) {
                            for(String consoleMsg : res.getConsoleMessages()) {
                                console.printInfo(consoleMsg);
                            }
                        }
                        ret = res.getResponseCode();
                    }
                }

                // TODO_MA 注释：如果报错，则需要重试
            } catch(CommandNeedRetryException e) {
                console.printInfo("Retry query with a different approach...");
                tryCount++;
                needRetry = true;
            }

            // TODO_MA 注释：当上头的代码执行出现 异常的时候，则继续进行重试
        } while(needRetry);

        return ret;
    }

    /**
     * If enabled and applicable to this command, print the field headers for the output.
     *
     * @param qp  Driver that executed the command
     * @param out PrintStream which to send output to
     */
    private void printHeader(Driver qp, PrintStream out) {

        // TODO_MA 注释：获取字段定义
        List<FieldSchema> fieldSchemas = qp.getSchema().getFieldSchemas();

        // TODO_MA 注释：输出字段定义
        if(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CLI_PRINT_HEADER) && fieldSchemas != null) {
            // Print the column names
            boolean first_col = true;

            // TODO_MA 注释：逐个输出字段定义，多个字段之间使用 \t 作为分隔符
            for(FieldSchema fs : fieldSchemas) {
                if(!first_col) {
                    out.print('\t');
                }
                out.print(fs.getName());
                first_col = false;
            }

            // TODO_MA 注释：输出一个换行
            out.println();
        }
    }

    public int processLine(String line) {
        System.out.println("process line .. ");

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理一个单个SQL
         */
        return processLine(line, false);
    }

    /**
     * Processes a line of semicolon separated commands
     *
     * @param line              The commands to process
     * @param allowInterrupting When true the function will handle SIG_INT (Ctrl+C) by interrupting the
     *                          processing and
     *                          returning -1
     * @return 0 if ok
     * <p>
     * line由 ; 组成的多个SQL
     */
    public int processLine(String line, boolean allowInterrupting) {

        SignalHandler oldSignal = null;
        Signal interruptSignal = null;

        // TODO_MA 注释: 中断处理： Ctrl + C
        if(allowInterrupting) {
            // Remember all threads that were running at the time we started line processing.
            // Hook up the custom Ctrl+C handler while processing this line
            interruptSignal = new Signal("INT");
            oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
                private boolean interruptRequested;

                @Override
                public void handle(Signal signal) {
                    boolean initialRequest = !interruptRequested;
                    interruptRequested = true;

                    // Kill the VM on second ctrl+c
                    if(!initialRequest) {
                        console.printInfo("Exiting the JVM");
                        System.exit(127);
                    }

                    // Interrupt the CLI thread to stop the current statement and return to prompt
                    console.printInfo("Interrupting... Be patient, this might take some time.");
                    console.printInfo("Press Ctrl+C again to kill JVM");

                    // First, kill any running MR jobs
                    HadoopJobExecHelper.killRunningJobs();
                    TezJobExecHelper.killRunningJobs();
                    HiveInterruptUtils.interrupt();
                }
            });
        }

        try {
            int lastRet = 0, ret = 0;

            // TODO_MA 注释: 把由 ; 分隔的这个 HQL字符串 line  切割成多个 独立的 HQL
            // TODO_MA 注释：line 有可能是多条SQL组成，按照 ; 分割之后，command 就是一个单独的SQL
            // we can not use "split" function directline as ";" may be quoted
            List<String> commands = splitSemiColon(line);

            // TODO_MA 注释: 挨个儿执行每一条SQL语句
            String command = "";
            for(String oneCmd : commands) {
                if(StringUtils.endsWith(oneCmd, "\\")) {
                    command += StringUtils.chop(oneCmd) + ";";
                    continue;
                } else {
                    command += oneCmd;
                }
                if(StringUtils.isBlank(command)) {
                    continue;
                }

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 处理一个 独立的 HQL 的执行 command 就是一个能独立执行的SQL
                 */
                ret = processCmd(command);

                command = "";
                lastRet = ret;
                boolean ignoreErrors = HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIIGNOREERRORS);
                if(ret != 0 && !ignoreErrors) {
                    CommandProcessorFactory.clean((HiveConf) conf);
                    return ret;
                }
            }
            CommandProcessorFactory.clean((HiveConf) conf);
            return lastRet;
        } finally {
            // Once we are done processing the line, restore the old handler
            if(oldSignal != null && interruptSignal != null) {
                Signal.handle(interruptSignal, oldSignal);
            }
        }
    }

    public static List<String> splitSemiColon(String line) {
        boolean insideSingleQuote = false;
        boolean insideDoubleQuote = false;
        boolean escape = false;
        int beginIndex = 0;
        List<String> ret = new ArrayList<>();
        for(int index = 0; index < line.length(); index++) {
            if(line.charAt(index) == '\'') {
                // take a look to see if it is escaped
                if(!escape) {
                    // flip the boolean variable
                    insideSingleQuote = !insideSingleQuote;
                }
            } else if(line.charAt(index) == '\"') {
                // take a look to see if it is escaped
                if(!escape) {
                    // flip the boolean variable
                    insideDoubleQuote = !insideDoubleQuote;
                }
            } else if(line.charAt(index) == ';') {
                if(insideSingleQuote || insideDoubleQuote) {
                    // do not split
                } else {
                    // split, do not include ; itself
                    ret.add(line.substring(beginIndex, index));
                    beginIndex = index + 1;
                }
            } else {
                // nothing to do
            }
            // set the escape
            if(escape) {
                escape = false;
            } else if(line.charAt(index) == '\\') {
                escape = true;
            }
        }
        ret.add(line.substring(beginIndex));
        return ret;
    }

    public int processReader(BufferedReader r) throws IOException {
        String line;
        StringBuilder qsb = new StringBuilder();

        while((line = r.readLine()) != null) {
            // Skipping through comments
            if(!line.startsWith("--")) {
                qsb.append(line + "\n");
            }
        }

        System.out.println("do processLine ... ");
        return (processLine(qsb.toString()));
    }

    public int processFile(String fileName) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理
         */
        Path path = new Path(fileName);
        FileSystem fs;
        if(!path.toUri().isAbsolute()) {
            fs = FileSystem.getLocal(conf);
            path = fs.makeQualified(path);
        } else {
            fs = FileSystem.get(path.toUri(), conf);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 读取文件
         */
        BufferedReader bufferReader = null;
        int rc = 0;
        try {
            bufferReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            rc = processReader(bufferReader);
        } finally {
            IOUtils.closeStream(bufferReader);
        }
        return rc;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 处理一些初始文件
     */
    public void processInitFiles(CliSessionState ss) throws IOException {
        boolean saveSilent = ss.getIsSilent();
        ss.setIsSilent(true);

        // TODO_MA 注释： 处理文件
        for(String initFile : ss.initFiles) {
            int rc = processFile(initFile);
            if(rc != 0) {
                System.exit(rc);
            }
        }

        // TODO_MA 注释： 设置 HIVE_HOME 设置 HIVE_CONF_DIR 设置 user.home
        if(ss.initFiles.size() == 0) {
            if(System.getenv("HIVE_HOME") != null) {
                String hivercDefault = System.getenv("HIVE_HOME") + File.separator + "bin" + File.separator + HIVERCFILE;
                if(new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if(rc != 0) {
                        System.exit(rc);
                    }
                    console.printError(
                            "Putting the global hiverc in " + "$HIVE_HOME/bin/.hiverc is deprecated. " + "Please " + "use $HIVE_CONF_DIR" + "/.hiverc instead.");
                }
            }
            if(System.getenv("HIVE_CONF_DIR") != null) {
                String hivercDefault = System.getenv("HIVE_CONF_DIR") + File.separator + HIVERCFILE;
                if(new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if(rc != 0) {
                        System.exit(rc);
                    }
                }
            }
            if(System.getProperty("user.home") != null) {
                String hivercUser = System.getProperty("user.home") + File.separator + HIVERCFILE;
                if(new File(hivercUser).exists()) {
                    int rc = processFile(hivercUser);
                    if(rc != 0) {
                        System.exit(rc);
                    }
                }
            }
        }
        ss.setIsSilent(saveSilent);
    }

    public void processSelectDatabase(CliSessionState ss) throws IOException {
        String database = ss.database;
        if(database != null) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 发现有意思的东西: processLine 才是执行一个 SQL 语句的方法实现
             */
            int rc = processLine("use " + database + ";");

            if(rc != 0) {
                System.exit(rc);
            }
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： Tab 提示符功能
     */
    public static Completer[] getCommandCompleter() {
        // StringsCompleter matches against a pre-defined wordlist
        // We start with an empty wordlist and build it up
        List<String> candidateStrings = new ArrayList<String>();

        // TODO_MA 注释： 函数提示
        // We add Hive function names
        // For functions that aren't infix operators, we add an open
        // parenthesis at the end.
        for(String s : FunctionRegistry.getFunctionNames()) {
            if(s.matches("[a-z_]+")) {
                candidateStrings.add(s + "(");
            } else {
                candidateStrings.add(s);
            }
        }

        // TODO_MA 注释：关键字提示
        // We add Hive keywords, including lower-cased versions
        for(String s : HiveParser.getKeywords()) {
            candidateStrings.add(s);
            candidateStrings.add(s.toLowerCase());
        }

        StringsCompleter strCompleter = new StringsCompleter(candidateStrings);

        // TODO_MA 注释： 定义 ( ) [ ] 作为 定界符
        // Because we use parentheses in addition to whitespace
        // as a keyword delimiter, we need to define a new ArgumentDelimiter
        // that recognizes parenthesis as a delimiter.
        ArgumentDelimiter delim = new AbstractArgumentDelimiter() {
            @Override
            public boolean isDelimiterChar(CharSequence buffer, int pos) {
                char c = buffer.charAt(pos);
                return (Character.isWhitespace(c) || c == '(' || c == ')' || c == '[' || c == ']');
            }
        };

        // The ArgumentCompletor allows us to match multiple tokens in the same line.
        final ArgumentCompleter argCompleter = new ArgumentCompleter(delim, strCompleter);
        // By default ArgumentCompletor is in "strict" mode meaning
        // a token is only auto-completed if all prior tokens
        // match. We don't want that since there are valid tokens
        // that are not in our wordlist (eg. table and column names)
        argCompleter.setStrict(false);

        // TODO_MA 注释： 处理不必要的 空格的 情况。比如 函数调用的时候的左括号的右边的空格是不必要的
        // ArgumentCompletor always adds a space after a matched token.
        // This is undesirable for function names because a space after
        // the opening parenthesis is unnecessary (and uncommon) in Hive.
        // We stack a custom Completor on top of our ArgumentCompletor to reverse this.
        Completer customCompletor = new Completer() {
            @Override
            public int complete(String buffer, int offset, List completions) {
                List<String> comp = completions;
                int ret = argCompleter.complete(buffer, offset, completions);
                // ConsoleReader will do the substitution if and only if there
                // is exactly one valid completion, so we ignore other cases.
                if(completions.size() == 1) {
                    if(comp.get(0).endsWith("( ")) {
                        comp.set(0, comp.get(0).trim());
                    }
                }
                return ret;
            }
        };

        List<String> vars = new ArrayList<String>();
        for(HiveConf.ConfVars conf : HiveConf.ConfVars.values()) {
            vars.add(conf.varname);
        }

        StringsCompleter confCompleter = new StringsCompleter(vars) {
            @Override
            public int complete(final String buffer, final int cursor, final List<CharSequence> clist) {
                int result = super.complete(buffer, cursor, clist);
                if(clist.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
                    HiveConf.ConfVars var = HiveConf.getConfVars(buffer.substring(0, cursor - 1));
                    if(var == null) {
                        return result;
                    }
                    if(var.getValidator() instanceof Validator.StringSet) {
                        Validator.StringSet validator = (Validator.StringSet) var.getValidator();
                        clist.addAll(validator.getExpected());
                    } else if(var.getValidator() != null) {
                        clist.addAll(Arrays.asList(var.getValidator().toDescription(), ""));
                    } else {
                        clist.addAll(Arrays.asList("Expects " + var.typeString() + " type value", ""));
                    }
                    return cursor;
                }
                if(clist.size() > DELIMITED_CANDIDATE_THRESHOLD) {
                    Set<CharSequence> delimited = new LinkedHashSet<CharSequence>();
                    for(CharSequence candidate : clist) {
                        Iterator<String> it = Splitter.on(".").split(candidate.subSequence(cursor, candidate.length())).iterator();
                        if(it.hasNext()) {
                            String next = it.next();
                            if(next.isEmpty()) {
                                next = ".";
                            }
                            candidate = buffer != null ? buffer.substring(0, cursor) + next : next;
                        }
                        delimited.add(candidate);
                    }
                    clist.clear();
                    clist.addAll(delimited);
                }
                return result;
            }
        };

        StringsCompleter setCompleter = new StringsCompleter("set") {
            @Override
            public int complete(String buffer, int cursor, List<CharSequence> clist) {
                return buffer != null && buffer.equals("set") ? super.complete(buffer, cursor, clist) : -1;
            }
        };

        ArgumentCompleter propCompleter = new ArgumentCompleter(setCompleter, confCompleter) {
            @Override
            public int complete(String buffer, int offset, List<CharSequence> completions) {
                int ret = super.complete(buffer, offset, completions);
                if(completions.size() == 1) {
                    completions.set(0, ((String) completions.get(0)).trim());
                }
                return ret;
            }
        };
        return new Completer[]{propCompleter, customCompletor};
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 这是 Hive 的 SQL 执行入口
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("log4j.skipJansi", "true");

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Hive的SQL执行入口
         *  0： 正常退出
         *  5： cancel = ctrl + c
         */
        int ret = new CliDriver().run(args);

        // TODO_MA 注释： 根据执行结果的状态值，进行JVM退出
        System.exit(ret);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： SQL 的样式：
     *  1、select * from student;
     *  2、hive -e "sql" -hive.root.logger=info
     *  3、hive -f sqlFile
     *  4、source sqlfile
     */
    public int run(String[] args) throws Exception {
        this.args = args;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析 hiveconf， hive.root.logger 等参数
         *   hive hive.root.logger
         *   hive key1=value1
         *   hive ====> hive> select ....
         */
        OptionsProcessor oproc = new OptionsProcessor();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：process_stage1() 解析参数
         *  1、hive.root.logger
         *  2、define
         *  3、hivevar
         */
        if(!oproc.process_stage1(args)) {
            return 1;
        }

        // TODO_MA 注释：log4j 初始化
        // NOTE: It is critical to do this here so that log4j is reinitialized
        // before any of the other core hive classes are loaded
        boolean logInitFailed = false;
        String logInitDetailMessage;
        try {
            logInitDetailMessage = LogUtils.initHiveLog4j();
        } catch(LogInitializationException e) {
            logInitFailed = true;
            logInitDetailMessage = e.getMessage();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：初始化 SessionState， new HiveConf() 的时候，会进行各种参数的初始化和解析动作
         *  hive中的配置信息的管理对象： HiveConf
         */
        CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));

        // TODO_MA 注释：初始化各种系统输入和输出，负责接收 SQL 和 打印执行结果
        // TODO_MA 注释：因为需要接收用户输入的 sql,内部执行之后，打印结果输出，所以，必然会涉及到 System.in 和 System.out
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.info = new PrintStream(System.err, true, "UTF-8");
            ss.err = new CachingPrintStream(System.err, true, "UTF-8");
        } catch(UnsupportedEncodingException e) {
            return 3;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析 -i -f -e -S -H -hivevar 等
         */
        if(!oproc.process_stage2(ss)) {
            return 2;
        }

        // TODO_MA 注释：静默模式
        if(!ss.getIsSilent()) {
            if(logInitFailed) {
                System.err.println(logInitDetailMessage);
            } else {
                SessionState.getConsole().printInfo(logInitDetailMessage);
            }
        }

        // set all properties specified via command line
        HiveConf conf = ss.getConf();
        for(Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
            conf.set((String) item.getKey(), (String) item.getValue());
            ss.getOverriddenConfigurations().put((String) item.getKey(), (String) item.getValue());
        }

        // read prompt configuration and substitute variables.
        prompt = conf.getVar(HiveConf.ConfVars.CLIPROMPT);
        prompt = new VariableSubstitution(new HiveVariableSource() {
            @Override
            public Map<String, String> getHiveVariable() {
                return SessionState.get().getHiveVariables();
            }
        }).substitute(conf, prompt);
        prompt2 = spacesForString(prompt);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动会话
         */
        if(HiveConf.getBoolVar(conf, ConfVars.HIVE_CLI_TEZ_SESSION_ASYNC)) {
            // Start the session in a fire-and-forget manner. When the asynchronously initialized parts of
            // the session are needed, the corresponding getters and other methods will wait as needed.
            SessionState.beginStart(ss, console);
        } else {
            SessionState.start(ss);
        }

        ss.updateThreadName();

        // execute cli driver work
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 执行
             */
            return executeDriver(ss, conf, oproc);

        } finally {
            ss.resetThreadName();
            ss.close();
        }
    }

    /**
     * Execute the cli work
     *
     * @param ss    CliSessionState of the CLI driver
     * @param conf  HiveConf for the driver session
     * @param oproc Operation processor of the CLI invocation
     * @return status of the CLI command execution
     * @throws Exception
     */
    private int executeDriver(CliSessionState ss, HiveConf conf, OptionsProcessor oproc) throws Exception {

        // TODO_MA 注释： 构建 CliDriver，并且设置各种参数和变量参数
        CliDriver cli = new CliDriver();
        cli.setHiveVariables(oproc.getHiveVariables());

        // TODO_MA 注释: 如果有 use database 语句，则执行
        // TODO_MA 注释： use studentdb; select * from student;
        // TODO_MA 注释： select * from studentdb.student;
        // select * from db.table;
        // use the specified database if specified
        cli.processSelectDatabase(ss);

        // TODO_MA 注释: 初始化 -i 携带的初始配置等参数文件
        // TODO_MA 注释： hive -i initFile， initFile 一般来说写的就是一堆key_vlaue类型的配置信息
        // TODO_MA 注释：set key=value;
        // Execute -i init files (always in silent mode)
        cli.processInitFiles(ss);

        // TODO_MA 注释: 如果是单行命令 hive -e "单个SQL" 这种
        // TODO_MA 注释: ss.execString 就是 hive -e SQL 语句中的 SQL
        if(ss.execString != null) {
            int cmdProcessStatus = cli.processLine(ss.execString);
            return cmdProcessStatus;
        }

        // TODO_MA 注释: 如果是 SQL 脚本文件  hive -f "fileName"
        // TODO_MA 注释：fileName 一般写的是一堆SQL
        try {
            if(ss.fileName != null) {
                return cli.processFile(ss.fileName);
            }
        } catch(FileNotFoundException e) {
            System.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
            return 3;
        }

        // TODO_MA 注释: 引擎, 启动 Hive 的警告从这儿来的。
        if("mr".equals(HiveConf.getVar(conf, ConfVars.HIVE_EXECUTION_ENGINE))) {
            console.printInfo(HiveConf.generateMrDeprecationWarning());
        }

        // TODO_MA 注释: 初始化 读取用户输入的 SQL 命令等的 Console
        // 初始化从 System.in 和 System.out Console
        setupConsoleReader();

        String line;
        int ret = 0;
        String prefix = "";
        String curDB = getFormattedDb(conf, ss);
        String curPrompt = prompt + curDB;
        String dbSpaces = spacesForString(curDB);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 不停的读取 命令 执行
         *  line = 你输入的SQL语句
         *      show tables;
         *      select * from a ;
         *      select * from b;
         *  Scanner scanner
         *      scanner.nextLine  ===  从标准输入中读取一行数据
         *     所谓的交互式：while 死循环，不停的接收输入并且调用 processLine 来出来这输入的一个 SQL
         *     while(readLine != null){
         *         processLIne(line);
         *     }
         */
        while((line = reader.readLine(curPrompt + "> ")) != null) {

            // TODO_MA 注释：处理SQL语句中的  \n   \\   ;   --  等符号问题。
            if(!prefix.equals("")) {
                prefix += '\n';
            }
            if(line.trim().startsWith("--")) {
                continue;
            }

            // TODO_MA 注释：如果以 ; 结束，则该
            if(line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
                line = prefix + line;

                // TODO_MA 注释：处理 HQL, 这个 line 有可能是一条sql也有可能是多条SQL
                ret = cli.processLine(line, true);

                prefix = "";
                curDB = getFormattedDb(conf, ss);
                curPrompt = prompt + curDB;
                dbSpaces = dbSpaces.length() == curDB.length() ? dbSpaces : spacesForString(curDB);

            } else {
                prefix = prefix + line;
                curPrompt = prompt2 + dbSpaces;
                continue;
            }
        }

        return ret;
    }

    private void setupCmdHistory() {
        final String HISTORYFILE = ".hivehistory";
        String historyDirectory = System.getProperty("user.home");
        PersistentHistory history = null;
        try {
            if((new File(historyDirectory)).exists()) {
                String historyFile = historyDirectory + File.separator + HISTORYFILE;
                history = new FileHistory(new File(historyFile));
                reader.setHistory(history);
            } else {
                System.err.println(
                        "WARNING: Directory for Hive history file: " + historyDirectory + " does not " + "exist.   History will not be " + "available during this session.");
            }
        } catch(Exception e) {
            System.err.println(
                    "WARNING: Encountered an error while trying to initialize Hive's " + "history " + "file.  History will not be " + "available during this session.");
            System.err.println(e.getMessage());
        }

        // add shutdown hook to flush the history to history file
        ShutdownHookManager.addShutdownHook(new Runnable() {
            @Override
            public void run() {
                History h = reader.getHistory();
                if(h instanceof FileHistory) {
                    try {
                        ((FileHistory) h).flush();
                    } catch(IOException e) {
                        System.err.println("WARNING: Failed to write command history file: " + e.getMessage());
                    }
                }
            }
        });
    }

    protected void setupConsoleReader() throws IOException {

        // TODO_MA 注释：初始化 ConsoleReader
        reader = new ConsoleReader();
        reader.setExpandEvents(false);
        reader.setBellEnabled(false);

        // TODO_MA 注释： 增加 tab 提示功能
        for(Completer completer : getCommandCompleter()) {
            reader.addCompleter(completer);
        }

        // TODO_MA 注释：设置 HQL 命令历史记录相关操作
        setupCmdHistory();
    }

    /**
     * Retrieve the current database name string to display, based on the
     * configuration value.
     *
     * @param conf storing whether or not to show current db
     * @param ss   CliSessionState to query for db name
     * @return String to show user for current db value
     */
    private static String getFormattedDb(HiveConf conf, CliSessionState ss) {
        if(!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
            return "";
        }
        //BUG: This will not work in remote mode - HIVE-5153
        String currDb = SessionState.get().getCurrentDatabase();

        if(currDb == null) {
            return "";
        }

        return " (" + currDb + ")";
    }

    /**
     * Generate a string of whitespace the same length as the parameter
     *
     * @param s String for which to generate equivalent whitespace
     * @return Whitespace
     */
    private static String spacesForString(String s) {
        if(s == null || s.length() == 0) {
            return "";
        }
        return String.format("%1$-" + s.length() + "s", "");
    }

    public void setHiveVariables(Map<String, String> hiveVariables) {
        SessionState.get().setHiveVariables(hiveVariables);
    }

}
