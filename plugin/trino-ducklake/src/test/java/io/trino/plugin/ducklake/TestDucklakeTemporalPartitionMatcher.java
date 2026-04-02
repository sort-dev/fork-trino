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
package io.trino.plugin.ducklake;

import io.trino.plugin.ducklake.catalog.DucklakePartitionTransform;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.trino.plugin.ducklake.DucklakeTemporalPartitionEncoding.CALENDAR;
import static io.trino.plugin.ducklake.DucklakeTemporalPartitionEncoding.EPOCH;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeTemporalPartitionMatcher
{
    @Test
    public void testStrictCalendarMatchesCalendarYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "2023",
                domain,
                DucklakePartitionTransform.YEAR,
                CALENDAR,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testStrictEpochRejectsCalendarYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "2023",
                domain,
                DucklakePartitionTransform.YEAR,
                EPOCH,
                false);

        assertThat(matches).isFalse();
    }

    @Test
    public void testStrictEpochMatchesEpochYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "53",
                domain,
                DucklakePartitionTransform.YEAR,
                EPOCH,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModeKeepsAmbiguousMonthWhenEitherEncodingMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 1, 15));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "1",
                domain,
                DucklakePartitionTransform.MONTH,
                EPOCH,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModeKeepsImpossibleCalendarMonthWhenEpochMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "641",
                domain,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModePrunesWhenImpossibleCalendarMonthAndEpochDoesNotMatch()
    {
        Domain domain = singleDate(LocalDate.of(2023, 1, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "641",
                domain,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                true);

        assertThat(matches).isFalse();
    }

    @Test
    public void testLenientModeKeepsAmbiguousDayWhenCalendarMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 15));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "15",
                domain,
                DucklakePartitionTransform.DAY,
                EPOCH,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModePrunesWhenImpossibleCalendarDayAndEpochDoesNotMatch()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 16));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "19523",
                domain,
                DucklakePartitionTransform.DAY,
                CALENDAR,
                true);

        assertThat(matches).isFalse();
    }

    @Test
    public void testParseFailureDoesNotPrune()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "not-a-number",
                domain,
                DucklakePartitionTransform.YEAR,
                CALENDAR,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testCalendarWrappingMonthRangeDoesNotPrune()
    {
        Domain wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "3",
                wrappedRange,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testEpochMonthRangePrunesOutOfRangeValue()
    {
        Domain wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "650",
                wrappedRange,
                DucklakePartitionTransform.MONTH,
                EPOCH,
                false);

        assertThat(matches).isFalse();
    }

    @Test
    public void testTimestampHourSupportsBothEncodingsInLenientMode()
    {
        long tsMicros = 1_704_900_600_000_000L; // 2024-01-10T15:30:00Z
        long epochHours = Math.floorDiv(tsMicros, 3_600_000_000L);
        Domain domain = Domain.singleValue(TIMESTAMP_MICROS, tsMicros);

        boolean calendarValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS,
                "15",
                domain,
                DucklakePartitionTransform.HOUR,
                EPOCH,
                true);
        boolean epochValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS,
                String.valueOf(epochHours),
                domain,
                DucklakePartitionTransform.HOUR,
                CALENDAR,
                true);

        assertThat(calendarValueMatches).isTrue();
        assertThat(epochValueMatches).isTrue();
    }

    private static Domain singleDate(LocalDate date)
    {
        return Domain.singleValue(DATE, date.toEpochDay());
    }

    private static Domain dateRange(LocalDate low, LocalDate high)
    {
        return Domain.create(
                ValueSet.ofRanges(Range.range(DATE, low.toEpochDay(), true, high.toEpochDay(), true)),
                false);
    }
}
