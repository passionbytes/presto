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
package com.facebook.presto.sql.planner;

import com.facebook.presto.cost.StatsAndCosts;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanStage rootStage;
    private final TypeProvider types;
    private final StatsAndCosts statsAndCosts;

    public Plan(PlanStage rootStage, TypeProvider types, StatsAndCosts statsAndCosts)
    {
        this.rootStage = requireNonNull(rootStage, "rootStage is null");
        this.types = requireNonNull(types, "types is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public PlanStage getRootStage()
    {
        return rootStage;
    }

    public List<PlanStage> getStages()
    {
        Set<Reference<PlanStage>> references = new HashSet<>();
        LinkedList<Reference<PlanStage>> queue = new LinkedList<>();
        queue.add(new Reference<>(rootStage));
        while (!queue.isEmpty()) {
            Reference<PlanStage> reference = queue.poll();
            references.add(reference);
            for (Reference<PlanStage> dependency : reference.get().getDependencies()) {
                if (!references.contains(dependency)) {
                    queue.add(dependency);
                }
            }
        }
        return references.stream()
                .map(Reference::get)
                .collect(toImmutableList());
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }
}
