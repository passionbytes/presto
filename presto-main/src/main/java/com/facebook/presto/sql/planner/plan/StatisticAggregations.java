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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static java.util.Objects.requireNonNull;

public class StatisticAggregations
{
    private final Map<Symbol, Aggregation> aggregations;
    private final List<List<Symbol>> groupingSets;

    @JsonCreator
    public StatisticAggregations(
            @JsonProperty("aggregations") Map<Symbol, Aggregation> aggregations,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets)
    {
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.groupingSets = listOfListsCopy(requireNonNull(groupingSets, "groupingSets is null"));
    }

    @JsonProperty
    public Map<Symbol, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    public Parts split(SymbolAllocator symbolAllocator, FunctionRegistry functionRegistry)
    {
        ImmutableMap.Builder<Symbol, Aggregation> intermediateAggregation = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Aggregation> finalAggregation = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            Signature signature = originalAggregation.getSignature();
            InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);
            Symbol intermediateSymbol = symbolAllocator.newSymbol(signature.getName(), function.getIntermediateType());
            mappings.put(entry.getKey(), intermediateSymbol);
            intermediateAggregation.put(intermediateSymbol, new Aggregation(originalAggregation.getCall(), signature, originalAggregation.getMask()));
            finalAggregation.put(entry.getKey(),
                    new Aggregation(
                            new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())),
                            signature,
                            Optional.empty()));
        }
        groupingSets.forEach(groupingSet -> groupingSet.forEach(symbol -> mappings.put(symbol, symbol)));
        return new Parts(
                new StatisticAggregations(intermediateAggregation.build(), groupingSets),
                new StatisticAggregations(finalAggregation.build(), groupingSets),
                mappings.build());
    }

    public static class Parts
    {
        private final StatisticAggregations partialAggregation;
        private final StatisticAggregations finalAggregation;
        private final Map<Symbol, Symbol> mappings;

        public Parts(StatisticAggregations partialAggregation, StatisticAggregations finalAggregation, Map<Symbol, Symbol> mappings)
        {
            this.partialAggregation = requireNonNull(partialAggregation, "partialAggregation is null");
            this.finalAggregation = requireNonNull(finalAggregation, "finalAggregation is null");
            this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
        }

        public StatisticAggregations getPartialAggregation()
        {
            return partialAggregation;
        }

        public StatisticAggregations getFinalAggregation()
        {
            return finalAggregation;
        }

        public Map<Symbol, Symbol> getMappings()
        {
            return mappings;
        }
    }
}
