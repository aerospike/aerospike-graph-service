/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
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

package com.aerospike.firefly.util;

import com.aerospike.firefly.process.traversal.predicate.GeoPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TestFireflyGeoValue {
    @Test
    public void convertsSinglePointToGeoJsonAndBack() {
        final List<Double> input = List.of(-122.0862, 37.4220);
        final List<String> points = FireflyGeoValue.toGeoJsonPoints(input);
        Assert.assertEquals(1, points.size());
        Assert.assertTrue(points.get(0).contains("-122.0862"));
        final List<Double> roundTrip = FireflyGeoValue.fromGeoJsonPoint(points.get(0));
        Assert.assertEquals(input, roundTrip);
    }

    @Test
    public void convertsMultiPointInput() {
        final List<List<Double>> input = List.of(
                List.of(-122.0862, 37.4220),
                List.of(-122.1000, 37.4300));
        final List<String> points = FireflyGeoValue.toGeoJsonPoints(input);
        Assert.assertEquals(2, points.size());
    }

    /**
     * Coordinates must always format with a dot decimal separator. Under a locale such as Germany a comma turns the
     * two-element coordinate array into four elements, which the server stores without complaint.
     */
    @Test
    public void formatsCoordinatesWithDotSeparatorUnderCommaDecimalLocale() {
        final Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            final String point = FireflyGeoValue.toGeoJsonPoint(-122.0862, 37.4220);
            Assert.assertTrue("Coordinates must use a dot decimal separator: " + point,
                    point.contains("-122.0862") && point.contains("37.4220"));
            Assert.assertEquals(List.of(-122.0862, 37.4220), FireflyGeoValue.fromGeoJsonPoint(point));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void rejectsInvalidCoordinateLength() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> FireflyGeoValue.parseCoordinatePair(List.of(1.0, 2.0, 3.0)));
    }

    @Test
    public void geoPropertyKeyRequiresPrefixOrExplicitList() {
        Assert.assertTrue(FireflyGeoValue.isGeoPropertyKey("geoLocation", true, List.of()));
        Assert.assertFalse(FireflyGeoValue.isGeoPropertyKey("location", true, List.of()));
        Assert.assertTrue(FireflyGeoValue.isGeoPropertyKey("location", true, List.of("location")));
        Assert.assertFalse(FireflyGeoValue.isGeoPropertyKey("geoId", false, List.of()));
    }

    @Test
    public void geoPredicateRadiusMatchesNearbyPoint() {
        final GeoPredicate predicate = (GeoPredicate) GeoPredicate.withinRadius(-122.0862, 37.4220, 5000).getBiPredicate();
        final String nearby = FireflyGeoValue.toGeoJsonPoint(-122.0863, 37.4221);
        Assert.assertTrue(predicate.test("geoLocation", nearby));
    }

    @Test
    public void geoPredicateRadiusRejectsDistantPoint() {
        final GeoPredicate predicate = (GeoPredicate) GeoPredicate.withinRadius(-122.0862, 37.4220, 1000).getBiPredicate();
        final String distant = FireflyGeoValue.toGeoJsonPoint(-121.0, 38.0);
        Assert.assertFalse(predicate.test("geoLocation", distant));
    }
}
