/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.rowNumber;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.lang.Math.toIntExact;

public class PushdownLimitIntoRowNumber
        implements Rule<LimitNode>
{
    private static final Capture<RowNumberNode> CHILD = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies() && limit.getCount() != 0 && limit.getCount() <= Integer.MAX_VALUE)
            .with(source().matching(rowNumber().matching(rowNumber -> rowNumber.getMaxRowCountPerPartition().isPresent()).capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        RowNumberNode source = captures.get(CHILD);
        int limit = toIntExact(node.getCount());
        RowNumberNode rowNumberNode = mergeLimit(source, limit);
        if (rowNumberNode.getMaxRowCountPerPartition().isPresent()) {
            if (rowNumberNode.getPartitionBy().isEmpty()) {
                return Result.ofPlanNode(rowNumberNode);
            }
            if (rowNumberNode.getMaxRowCountPerPartition().get().equals(source.getMaxRowCountPerPartition().get())) {
                return Result.empty();
            }
            return Result.ofPlanNode(replaceChildren(node, ImmutableList.of(rowNumberNode)));
        }

        return Result.empty();
    }

    private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
    {
        if (node.getMaxRowCountPerPartition().isPresent()) {
            newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
        }
        return new RowNumberNode(
                node.getId(),
                node.getSource(),
                node.getPartitionBy(),
                node.isOrderSensitive(),
                node.getRowNumberSymbol(),
                Optional.of(newRowCountPerPartition),
                node.getHashSymbol());
    }
}
