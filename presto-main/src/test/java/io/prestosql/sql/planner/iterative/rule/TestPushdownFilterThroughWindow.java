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
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestPushdownFilterThroughWindow
        extends BaseRuleTest
{
    private static final WindowNode.Frame frame = new WindowNode.Frame(
            WindowFrame.Type.RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    @Test
    public void testEliminateFilter()
    {
        ResolvedFunction rowNumber = createWindowFunctionSignature(tester().getMetadata(), "row_number");
        tester().assertThat(new PushdownFilterThroughWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 < cast(100 as bigint)"), p.window(
                            newWindowNodeSpecification(p, "a"),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, "a")),
                            p.values(p.symbol("a"))));
                })
                .matches(node(TopNRowNumberNode.class, node(ValuesNode.class)));
    }

    @Test
    public void testNoOutputsThroughWindow()
    {
        ResolvedFunction rowNumber = createWindowFunctionSignature(tester().getMetadata(), "row_number");
        tester().assertThat(new PushdownFilterThroughWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 < cast(-100 as bigint)"), p.window(
                            newWindowNodeSpecification(p, "a"),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, "a")),
                            p.values(p.symbol("a"))));
                })
                .matches(node(ValuesNode.class));
    }

    private static WindowNode.Specification newWindowNodeSpecification(PlanBuilder planBuilder, String symbolName)
    {
        OrderingScheme orderingScheme = new OrderingScheme(
                ImmutableList.of(planBuilder.symbol(symbolName)),
                ImmutableMap.of(planBuilder.symbol(symbolName), SortOrder.ASC_NULLS_FIRST));
        return new WindowNode.Specification(ImmutableList.of(planBuilder.symbol(symbolName, BIGINT)), Optional.of(orderingScheme));
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, String... symbols)
    {
        return new WindowNode.Function(
                resolvedFunction,
                Arrays.stream(symbols).map(SymbolReference::new).collect(Collectors.toList()),
                frame,
                false);
    }

    private static ResolvedFunction createWindowFunctionSignature(Metadata metadata, String name)
    {
        return metadata.resolveFunction(QualifiedName.of(name), fromTypes());
    }
}
