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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveOpConverterPostProc;
import org.apache.hadoop.hive.ql.optimizer.correlation.CorrelationOptimizer;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkDeDuplication;
import org.apache.hadoop.hive.ql.optimizer.index.RewriteGBUsingIndex;
import org.apache.hadoop.hive.ql.optimizer.lineage.Generator;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.pcr.PartitionConditionRemover;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.ppd.PredicatePushDown;
import org.apache.hadoop.hive.ql.ppd.PredicateTransitivePropagate;
import org.apache.hadoop.hive.ql.ppd.SimplePredicatePushDown;
import org.apache.hadoop.hive.ql.ppd.SyntheticJoinPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

/**
 * Implementation of the optimizer.
 */
public class Optimizer {
    private ParseContext pctx;
    private List<Transform> transformations;
    private static final Logger LOG = LoggerFactory.getLogger(Optimizer.class.getName());

    /**
     * Create the list of transformations.
     *
     * @param hiveConf
     */
    public void initialize(HiveConf hiveConf) {

        boolean isTezExecEngine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez");
        boolean isSparkExecEngine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark");
        boolean bucketMapJoinOptimizer = false;

        transformations = new ArrayList<Transform>();
        //todo_c 如果我们将 Calcite 运算符转换为 Hive 运算符，则添加所需的额外后处理转换
        // Add the additional postprocessing transformations needed if
        // we are translating Calcite operators into Hive operators.
        transformations.add(new HiveOpConverterPostProc());
        //todo_c 添加计算血缘信息的转换

        // Add the transformation that computes the lineage information.
        Set<String> postExecHooks = Sets.newHashSet(Splitter.on(",").trimResults().omitEmptyStrings()
                .split(Strings.nullToEmpty(HiveConf.getVar(hiveConf, HiveConf.ConfVars.POSTEXECHOOKS))));
        if(postExecHooks.contains("org.apache.hadoop.hive.ql.hooks.PostExecutePrinter") || postExecHooks
                .contains("org.apache.hadoop.hive.ql.hooks.LineageLogger") || postExecHooks.contains("org.apache.atlas.hive.hook.HiveHook")) {
            transformations.add(new Generator());
        }
        //todo_c 尝试先将 Filter 中的 OR 谓词转换为更简单的 IN 子句
        // Try to transform OR predicates in Filter into simpler IN clauses first
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZER) && !pctx.getContext().isCboSucceeded()) {
            //todo_c 31个or才转换为in
            final int min = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZERMIN);
            transformations.add(new PointLookupOptimizer(min));
        }
        //todo_c 从in中提取分区列

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEPARTITIONCOLUMNSEPARATOR)) {
            transformations.add(new PartitionColumnsSeparator());
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD) && !pctx.getContext().isCboSucceeded()) {
            transformations.add(new PredicateTransitivePropagate());
            if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCONSTANTPROPAGATION)) {
                transformations.add(new ConstantPropagate());
            }
            transformations.add(new SyntheticJoinPredicate());

            // TODO_MA 注释: 谓词下推
            transformations.add(new PredicatePushDown());

        } else if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD) && pctx.getContext().isCboSucceeded()) {
            transformations.add(new SyntheticJoinPredicate());
            transformations.add(new SimplePredicatePushDown());
            transformations.add(new RedundantDynamicPruningConditionsRemoval());
        }
        //todo_c 我们运行了两次常量传播，因为在谓词下推之后，过滤器表达式被组合并且可能有资格减少（比如不是空过滤器
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCONSTANTPROPAGATION) && !pctx.getContext().isCboSucceeded()) {
            // We run constant propagat                                                       ion twice because after predicate pushdown, filter expressions

            // are combined and may become eligible for reduction (like is not null filter).
            transformations.add(new ConstantPropagate());
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.DYNAMICPARTITIONING) && HiveConf
                .getVar(hiveConf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equals("nonstrict") && HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTSORTDYNAMICPARTITION) && !HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTLISTBUCKETING)) {
            transformations.add(new SortedDynPartitionOptimizer());
        }

        transformations.add(new SortedDynPartitionTimeGranularityOptimizer());

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD)) {

            // TODO_MA 注释：分区裁剪
            transformations.add(new PartitionPruner());
            transformations.add(new PartitionConditionRemover());
            if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTLISTBUCKETING)) {
                /* Add list bucketing pruner. */
                transformations.add(new ListBucketingPruner());
            }
            if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCONSTANTPROPAGATION) && !pctx.getContext().isCboSucceeded()) {
                // PartitionPruner may create more folding opportunities, run ConstantPropagate again.
                transformations.add(new ConstantPropagate());
            }
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTGROUPBY) || HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_GROUPBY_SORT)) {
            transformations.add(new GroupByOptimizer());
        }

        // TODO_MA 注释：列裁剪
        transformations.add(new ColumnPruner());

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME)) {
            if(!isTezExecEngine) {

                // TODO_MA 注释： Join的数据倾斜优化器
                transformations.add(new SkewJoinOptimizer());
            } else {
                LOG.warn("Skew join is currently not supported in tez! Disabling the skew join optimization.");
            }
        }
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTGBYUSINGINDEX)) {
            transformations.add(new RewriteGBUsingIndex());
        }
        transformations.add(new SamplePruner());

        MapJoinProcessor mapJoinProcessor = isSparkExecEngine ? new SparkMapJoinProcessor() : new MapJoinProcessor();
        transformations.add(mapJoinProcessor);

        if((HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN)) && !isTezExecEngine && !isSparkExecEngine) {
            transformations.add(new BucketMapJoinOptimizer());
            bucketMapJoinOptimizer = true;
        }

        // If optimize hive.optimize.bucketmapjoin.sortedmerge is set, add both
        // BucketMapJoinOptimizer and SortedMergeBucketMapJoinOptimizer
        if((HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)) && !isTezExecEngine && !isSparkExecEngine) {
            if(!bucketMapJoinOptimizer) {
                // No need to add BucketMapJoinOptimizer twice
                transformations.add(new BucketMapJoinOptimizer());
            }

            // TODO_MA 注释：SMB MapJoin 优化器
            transformations.add(new SortedMergeBucketMapJoinOptimizer());
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTIMIZEBUCKETINGSORTING)) {
            transformations.add(new BucketingSortingReduceSinkOptimizer());
        }

        transformations.add(new UnionProcessor());

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.NWAYJOINREORDER)) {
            transformations.add(new JoinReorder());
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.TEZ_OPTIMIZE_BUCKET_PRUNING) && HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD) && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER)) {
            final boolean compatMode = HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.TEZ_OPTIMIZE_BUCKET_PRUNING_COMPAT);
            transformations.add(new FixedBucketPruningOptimizer(compatMode));
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTREDUCEDEDUPLICATION) || pctx.hasAcidWrite()) {
            transformations.add(new ReduceSinkDeDuplication());
        }
        transformations.add(new NonBlockingOpDeDupProc());
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEIDENTITYPROJECTREMOVER) && !HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
            transformations.add(new IdentityProjectRemover());
        }
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVELIMITOPTENABLE)) {
            transformations.add(new GlobalLimitOptimizer());
        }
        //todo_c 相关优化， 如对相同的keyj进行shuffle就可以合并到一块。合并那些被多少mapreduce 使用的输入表。

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCORRELATION) && !HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVEGROUPBYSKEW) && !HiveConf
                .getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME) && !isTezExecEngine) {
            transformations.add(new CorrelationOptimizer());
        }
        if(HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVELIMITPUSHDOWNMEMORYUSAGE) > 0) {
            transformations.add(new LimitPushdownOptimizer());
        }
        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES)) {
            transformations.add(new StatsOptimizer());
        }
        if(pctx.getContext().isExplainSkipExecution() && !isTezExecEngine && !isSparkExecEngine) {
            transformations.add(new AnnotateWithStatistics());
            transformations.add(new AnnotateWithOpTraits());
        }

        if(!HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVEFETCHTASKCONVERSION).equals("none")) {
            transformations.add(new SimpleFetchOptimizer()); // must be called last
        }

        if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEFETCHTASKAGGR)) {
            transformations.add(new SimpleFetchAggregation());
        }

        if(pctx.getContext().getExplainConfig() != null && pctx.getContext().getExplainConfig().isFormatted()) {
            transformations.add(new AnnotateReduceSinkOutputOperator());
        }
    }

    /**
     * Invoke all the transformations one-by-one, and alter the query plan.
     *
     * @return ParseContext
     * @throws SemanticException
     */
    public ParseContext optimize() throws SemanticException {

        // TODO_MA 注释：刚才初始化的时候，添加了很多的优化器。transformations 有很多的优化器。
        for(Transform t : transformations) {
            t.beginPerfLogging();

            // TODO_MA 注释：真正的优化操作的执行： 真正执行对逻辑执行计划的优化
            pctx = t.transform(pctx);

            t.endPerfLogging(t.toString());
        }
        return pctx;
    }

    /**
     * @return the pctx
     */
    public ParseContext getPctx() {
        return pctx;
    }

    /**
     * @param pctx the pctx to set
     */
    public void setPctx(ParseContext pctx) {
        this.pctx = pctx;
    }

}
