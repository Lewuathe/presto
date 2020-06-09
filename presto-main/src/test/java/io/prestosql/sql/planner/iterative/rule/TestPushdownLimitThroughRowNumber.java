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
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;

public class TestPushdownLimitThroughRowNumber
        extends BaseRuleTest
{
    @Test
    public void testLimitAboveRowNumber()
    {
        tester().assertThat(new PushdownLimitThroughRowNumber())
                .on(p ->
                        p.limit(3, p.rowNumber(ImmutableList.of(), Optional.of(5),
                                p.symbol("row_number"), p.values(p.symbol("a")))))
                .matches(node(RowNumberNode.class, node(ValuesNode.class)));
    }

    @Test
    public void testZeroLimit()
    {
        tester().assertThat(new PushdownLimitThroughRowNumber())
                .on(p ->
                        p.limit(0, p.rowNumber(ImmutableList.of(), Optional.of(5),
                                p.symbol("row_number"), p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testTiesLimit()
    {
        tester().assertThat(new PushdownLimitThroughRowNumber())
                .on(p ->
                        p.limit(0, ImmutableList.of(p.symbol("a")),
                                p.rowNumber(ImmutableList.of(), Optional.of(5),
                                        p.symbol("row_number"), p.values(p.symbol("a")))))
                .doesNotFire();
    }
}
