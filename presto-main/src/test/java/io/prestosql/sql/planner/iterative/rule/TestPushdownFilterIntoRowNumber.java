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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushdownFilterIntoRowNumber
        extends BaseRuleTest
{
    @Test
    public void testSourceRowNumber()
    {
        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 < cast(100 as bigint)"),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"))));
                })
                .matches(node(RowNumberNode.class, node(ValuesNode.class)));
    }

    @Test
    public void testNoOutputsThroughRowNumber()
    {
        tester().assertThat(new PushdownFilterIntoRowNumber(tester().getMetadata()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.filter(expression("row_number_1 < cast(-100 as bigint)"),
                            p.rowNumber(ImmutableList.of(p.symbol("a")), Optional.empty(), rowNumberSymbol,
                                    p.values(p.symbol("a"))));
                })
                .matches(node(ValuesNode.class));
    }
}
