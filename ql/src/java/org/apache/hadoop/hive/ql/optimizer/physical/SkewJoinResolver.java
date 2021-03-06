/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;


/** todo_c PhysicalPlanResolver 的实现。它使用规则调度器为其reducer operator 树迭代每个任务，
 *        对于reducer 中具有join op 的任务，它将尝试添加一个与skew join 任务列表相关联的条件任务
 * An implementation of PhysicalPlanResolver. It iterator each task with a rule
 * dispatcher for its reducer operator tree, for task with join op in reducer,
 * it will try to add a conditional task associated a list of skew join tasks.
 */
public class SkewJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Dispatcher disp = new SkewJoinTaskDispatcher(pctx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * Iterator a task with a rule dispatcher for its reducer operator tree.
   */
  class SkewJoinTaskDispatcher implements Dispatcher {

    private PhysicalContext physicalContext;

    public SkewJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;


      if (!task.isMapRedTask() || task instanceof ConditionalTask
          || ((MapredWork) task.getWork()).getReduceWork() == null) {
        return null;
      }

      SkewJoinProcCtx skewJoinProcContext = new SkewJoinProcCtx(task,
          physicalContext.getParseContext());

      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1",
        CommonJoinOperator.getOperatorName() + "%"),

        //todo_c 获取skewjoinprocess
        SkewJoinProcFactory.getJoinProc());
      //todo_c 调度程序触发与最接近的匹配规则对应的处理器并传递上下文
      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(SkewJoinProcFactory
          .getDefaultProc(), opRules, skewJoinProcContext);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      //todo_c 迭代reducer运算符树

      // iterator the reducer operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      if (((MapredWork)task.getWork()).getReduceWork() != null) {
        topNodes.add(((MapredWork) task.getWork()).getReduceWork().getReducer());
      }
      ogw.startWalking(topNodes, null);
      return null;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  /**
   * A container of current task and parse context.
   */
  public static class SkewJoinProcCtx implements NodeProcessorCtx {
    private Task<? extends Serializable> currentTask;
    private ParseContext parseCtx;

    public SkewJoinProcCtx(Task<? extends Serializable> task,
        ParseContext parseCtx) {
      currentTask = task;
      this.parseCtx = parseCtx;
    }

    public Task<? extends Serializable> getCurrentTask() {
      return currentTask;
    }

    public void setCurrentTask(Task<? extends Serializable> currentTask) {
      this.currentTask = currentTask;
    }

    public ParseContext getParseCtx() {
      return parseCtx;
    }

    public void setParseCtx(ParseContext parseCtx) {
      this.parseCtx = parseCtx;
    }
  }
}
