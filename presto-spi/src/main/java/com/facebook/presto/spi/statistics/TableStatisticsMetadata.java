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
package com.facebook.presto.spi.statistics;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class TableStatisticsMetadata
{
    public static final TableStatisticsMetadata EMPTY_STATISTICS_METADATA = new TableStatisticsMetadata(emptySet(), emptySet(), emptySet());

    private final Set<ColumnStatisticMetadata> columnStatistics;
    private final Set<TableStatisticType> tableStatistics;
    private final Set<List<String>> groupingSets;

    public TableStatisticsMetadata(
            Set<ColumnStatisticMetadata> columnStatistics,
            Set<TableStatisticType> tableStatistics,
            Set<List<String>> groupingSets)
    {
        this.columnStatistics = unmodifiableSet(requireNonNull(columnStatistics, "columnStatistics is null"));
        this.tableStatistics = unmodifiableSet(requireNonNull(tableStatistics, "tableStatistics is null"));
        this.groupingSets = unmodifiableSet(requireNonNull(groupingSets, "groupingSets is null").stream().map(Collections::unmodifiableList).collect(toSet()));
    }

    public Set<ColumnStatisticMetadata> getColumnStatistics()
    {
        return columnStatistics;
    }

    public Set<TableStatisticType> getTableStatistics()
    {
        return tableStatistics;
    }

    public Set<List<String>> getGroupingSets()
    {
        return groupingSets;
    }

    public boolean isEmpty()
    {
        return tableStatistics.isEmpty() && columnStatistics.isEmpty();
    }
}
