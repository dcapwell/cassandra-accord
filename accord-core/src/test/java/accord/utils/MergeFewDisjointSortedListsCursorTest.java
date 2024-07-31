/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

class MergeFewDisjointSortedListsCursorTest
{
    @Test
    void findExactMatch()
    {
        qt().forAll(sortedListGen()).check(list -> {
            MergeFewDisjointSortedListsCursor<Integer> cursor = toCursor(list);
            for (Integer i : list)
            {
                assertFindMatch(cursor, i, i, "stateful");
                assertFindMatch(toCursor(list), i, i, "stateless");
            }
        });
    }

    @Test
    void findOutOfBounds()
    {
        qt().forAll(sortedListGen()).check(list -> {
            MergeFewDisjointSortedListsCursor cursor = toCursor(list);
            // check before the list
            assertFindMismatch(cursor, list.get(0) - 1, list.get(0), "");

            // check after the list; this will consume the full cursor
            assertThat(cursor.find(list.get(list.size() - 1) + 1)).isFalse();
            assertThat(cursor.hasCur()).isFalse();
        });
    }

    @Test
    void findGaps()
    {
        Gen<Pair<SortedList<Integer>, List<Between>>> gen = sortedListGen()
                                                            .map(l -> Pair.create(l, findGaps(l)))
                                                            .filter(p -> !p.right.isEmpty());
        qt().forAll(gen).check(pair -> {
            SortedList<Integer> list = pair.left;
            MergeFewDisjointSortedListsCursor<Integer> statefulCursor = toCursor(list);
            for (Between gap : pair.right)
            {
                for (int i = gap.start; i <= gap.end; i++)
                {
                    assertFindMismatch(statefulCursor, i, gap.end + 1, "stateful cursor");
                    assertFindMismatch(toCursor(list), i, gap.end + 1, "stateless cursor");
                }
            }
        });
    }

    private static void assertFindMatch(MergeFewDisjointSortedListsCursor<Integer> cursor, Integer find, Integer expected, String msg, Object... args)
    {
        assertThat(cursor.find(find)).describedAs("Could not find " + find + ": " + msg, args).isTrue();
        assertThat(cursor.hasCur()).describedAs("Find consumed the full cursor: " + msg, args).isTrue();
        assertThat(cursor.cur()).describedAs("cur returned an unexpected value: " + msg, args).isEqualTo(expected);
    }

    private static void assertFindMismatch(MergeFewDisjointSortedListsCursor<Integer> cursor, Integer find, Integer expected, String msg, Object... args)
    {
        assertThat(cursor.find(find)).describedAs("Found " + find + "?: " + msg, args).isFalse();
        assertThat(cursor.hasCur()).describedAs("Find consumed the full cursor: " + msg, args).isTrue();
        assertThat(cursor.cur()).describedAs("cur returned an unexpected value: " + msg, args).isEqualTo(expected);
    }

    private static List<Between> findGaps(SortedList<Integer> list)
    {
        if (list.size() <= 1) return Collections.emptyList();
        List<Between> gaps = new ArrayList<>();
        int previous = list.get(0);
        for (int i = 1; i < list.size(); i++)
        {
            int current = list.get(i);
            if (previous + 1 != current)
                gaps.add(new Between(previous + 1, current - 1));

            previous = current;
        }
        return gaps.isEmpty() ? Collections.emptyList() : gaps;
    }

    private static Gen<SortedList<Integer>> sortedListGen()
    {
        return Gens.lists(Gens.ints().between(0, 1 << 10)).unique().ofSizeBetween(1, 20).map(l -> {
            l.sort(Comparator.naturalOrder());
            return new SortedArrays.SortedArrayList<>(l.toArray(Integer[]::new));
        });
    }

    private static <T extends Comparable<? super T>> MergeFewDisjointSortedListsCursor<T> toCursor(SortedList<T> list)
    {
        MergeFewDisjointSortedListsCursor<T> cursor = new MergeFewDisjointSortedListsCursor<>(1);
        cursor.add(list);
        cursor.init();
        return cursor;
    }

    private static class Between
    {
        final int start, end;

        Between(int start, int end)
        {
            this.start = start;
            this.end = end;
        }
    }
}