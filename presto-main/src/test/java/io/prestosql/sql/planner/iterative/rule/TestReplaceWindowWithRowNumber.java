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
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestReplaceWindowWithRowNumber
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        ResolvedFunction rowNumber = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber1 = p.symbol("row_number_1");
                    return p.window(
                            new WindowNode.Specification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rowNumber1, newWindowNodeFunction(rowNumber, a.getName())),
                            p.values(a));
                })
                .matches(rowNumber(
                        pattern -> pattern.maxRowCountPerPartition(Optional.empty()),
                        node(ValuesNode.class)));
    }

    @Test
    public void testDoNotFire()
    {
        ResolvedFunction rank = tester().getMetadata().resolveFunction(QualifiedName.of("rank"), fromTypes());
        tester().assertThat(new ReplaceWindowWithRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rank1 = p.symbol("rank_1");
                    return p.window(
                            new WindowNode.Specification(ImmutableList.of(a), Optional.empty()),
                                ImmutableMap.of(rank1, newWindowNodeFunction(rank, a.getName())),
                                p.values(a));
                })
                .doesNotFire();
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, String... symbols)
    {
        return new WindowNode.Function(
                resolvedFunction,
                Arrays.stream(symbols).map(SymbolReference::new).collect(Collectors.toList()),
                new WindowNode.Frame(
                        WindowFrame.Type.RANGE,
                        UNBOUNDED_PRECEDING,
                        Optional.empty(),
                        CURRENT_ROW,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                false);
    }
}
