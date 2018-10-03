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
package com.facebook.presto.cost;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ComparisonExpression.Operator;

import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.util.MoreMath.firstNonNaN;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public final class ComparisonStatsCalculator
{
    private ComparisonStatsCalculator() {}

    public static Optional<PlanNodeStatsEstimate> estimateExpressionToLiteralComparison(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> expressionSymbol,
            SymbolStatsEstimate expressionStatistics,
            OptionalDouble literalValue,
            Operator comparisonOperator)
    {
        switch (comparisonOperator) {
            case EQUAL:
                return estimateExpressionToLiteralEquality(inputStatistics, expressionSymbol, expressionStatistics, literalValue);
            case NOT_EQUAL:
                return estimateExpressionToLiteralNonEquality(inputStatistics, expressionSymbol, expressionStatistics, literalValue);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return estimateExpressionToLiteralLessThan(inputStatistics, expressionSymbol, expressionStatistics, literalValue);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return estimateExpressionToLiteralGreaterThan(inputStatistics, expressionSymbol, expressionStatistics, literalValue);
            case IS_DISTINCT_FROM:
            default:
                return Optional.empty();
        }
    }

    private static Optional<PlanNodeStatsEstimate> estimateExpressionToLiteralEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        StatisticRange literalRange;
        if (literal.isPresent()) {
            literalRange = new StatisticRange(literal.getAsDouble(), literal.getAsDouble(), 1);
        }
        else {
            literalRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, literalRange);
    }

    private static Optional<PlanNodeStatsEstimate> estimateExpressionToLiteralNonEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        StatisticRange range = StatisticRange.from(expressionStats);

        StatisticRange literalRange;
        if (literal.isPresent()) {
            literalRange = new StatisticRange(literal.getAsDouble(), literal.getAsDouble(), 1);
        }
        else {
            literalRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        StatisticRange intersectRange = range.intersect(literalRange);
        double filterFactor = 1 - range.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics);
        estimate.setOutputRowCount(filterFactor * (1 - expressionStats.getNullsFraction()) * inputStatistics.getOutputRowCount());
        if (symbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate = buildFrom(expressionStats)
                    .setNullsFraction(0.0)
                    .setDistinctValuesCount(max(expressionStats.getDistinctValuesCount() - 1, 0))
                    .build();
            estimate = estimate.addSymbolStatistics(symbol.get(), symbolNewEstimate);
        }
        return Optional.of(estimate.build());
    }

    private static Optional<PlanNodeStatsEstimate> estimateExpressionToLiteralLessThan(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, new StatisticRange(NEGATIVE_INFINITY, literal.orElse(POSITIVE_INFINITY), NaN));
    }

    private static Optional<PlanNodeStatsEstimate> estimateExpressionToLiteralGreaterThan(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, new StatisticRange(literal.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN));
    }

    private static Optional<PlanNodeStatsEstimate> expressionToLiteralRangeComparison(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> expressionSymbol,
            SymbolStatsEstimate expressionStatistics,
            StatisticRange literalRange)
    {
        StatisticRange range = StatisticRange.from(expressionStatistics);
        StatisticRange intersectRange = range.intersect(literalRange);

        double filterFactor = range.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate estimate = inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - expressionStatistics.getNullsFraction()) * rowCount);
        if (expressionSymbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(expressionStatistics.getAverageRowSize())
                            .setStatisticsRange(intersectRange)
                            .setNullsFraction(0.0)
                            .build();
            estimate = estimate.mapSymbolColumnStatistics(expressionSymbol.get(), oldStats -> symbolNewEstimate);
        }
        return Optional.of(estimate);
    }

    public static Optional<PlanNodeStatsEstimate> comparisonExpressionToExpressionStats(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> left,
            SymbolStatsEstimate leftStats,
            Optional<Symbol> right,
            SymbolStatsEstimate rightStats,
            Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return expressionToExpressionEquality(inputStatistics, left, leftStats, right, rightStats);
            case NOT_EQUAL:
                return expressionToExpressionNonEquality(inputStatistics, left, leftStats, right, rightStats);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IS_DISTINCT_FROM:
            default:
                return Optional.empty();
        }
    }

    private static Optional<PlanNodeStatsEstimate> expressionToExpressionEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> left,
            SymbolStatsEstimate leftStats,
            Optional<Symbol> right,
            SymbolStatsEstimate rightStats)
    {
        if (isNaN(leftStats.getDistinctValuesCount()) || isNaN(rightStats.getDistinctValuesCount())) {
            return Optional.empty();
        }

        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange intersect = leftRange.intersect(rightRange);

        double nullsFilterFactor = (1 - leftStats.getNullsFraction()) * (1 - rightStats.getNullsFraction());
        double leftNdv = leftRange.getDistinctValuesCount();
        double rightNdv = rightRange.getDistinctValuesCount();
        double filterFactor = 1.0 / max(leftNdv, rightNdv, 1);
        double retainedNdv = min(leftNdv, rightNdv);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics)
                .setOutputRowCount(inputStatistics.getOutputRowCount() * nullsFilterFactor * filterFactor);

        SymbolStatsEstimate equalityStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(averageExcludingNaNs(leftStats.getAverageRowSize(), rightStats.getAverageRowSize()))
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        left.ifPresent(symbol -> estimate.addSymbolStatistics(symbol, equalityStats));
        right.ifPresent(symbol -> estimate.addSymbolStatistics(symbol, equalityStats));

        return Optional.of(estimate.build());
    }

    private static double averageExcludingNaNs(double first, double second)
    {
        if (isNaN(first) && isNaN(second)) {
            return NaN;
        }
        if (!isNaN(first) && !isNaN(second)) {
            return (first + second) / 2;
        }
        return firstNonNaN(first, second);
    }

    private static Optional<PlanNodeStatsEstimate> expressionToExpressionNonEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> left,
            SymbolStatsEstimate leftStats,
            Optional<Symbol> right,
            SymbolStatsEstimate rightStats)
    {
        double nullsFilterFactor = (1 - leftStats.getNullsFraction()) * (1 - rightStats.getNullsFraction());
        PlanNodeStatsEstimate inputNullsFiltered = inputStatistics.mapOutputRowCount(size -> size * nullsFilterFactor);
        SymbolStatsEstimate leftNullsFiltered = leftStats.mapNullsFraction(nullsFraction -> 0.0);
        SymbolStatsEstimate rightNullsFiltered = rightStats.mapNullsFraction(nullsFration -> 0.0);
        Optional<PlanNodeStatsEstimate> equalityStats = expressionToExpressionEquality(inputNullsFiltered, left, leftNullsFiltered, right, rightNullsFiltered);
        if (!equalityStats.isPresent()) {
            return Optional.empty();
        }
        PlanNodeStatsEstimate resultStats = inputNullsFiltered.mapOutputRowCount(rowCount -> {
            double equalityFilterFactor = equalityStats.get().getOutputRowCount() / inputNullsFiltered.getOutputRowCount();
            if (!isFinite(equalityFilterFactor)) {
                equalityFilterFactor = 0.0;
            }
            return rowCount * (1 - equalityFilterFactor);
        });
        if (left.isPresent()) {
            resultStats = resultStats.mapSymbolColumnStatistics(left.get(), stats -> leftNullsFiltered);
        }
        if (right.isPresent()) {
            resultStats = resultStats.mapSymbolColumnStatistics(right.get(), stats -> rightNullsFiltered);
        }

        return Optional.of(resultStats);
    }
}
