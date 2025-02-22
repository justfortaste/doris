// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_runtime_filter_decimal256", "query_p0") {
    sql "set enable_runtime_filter_prune=false;"
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"
    sql "set parallel_pipeline_task_num = 4;"

    sql "drop table if exists test_runtime_filter_decimal256_0;"
    sql """ create table test_runtime_filter_decimal256_0(k1 int, v1 decimal(38, 6), v2 decimal(38, 6))
                DUPLICATE KEY(`k1`, `v1`, `v2`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    
    sql """insert into test_runtime_filter_decimal256_0 values 
            (10, 10.000000, 99999999999999999999999999999999.999999),
            (10, 10.000000, 0.000001),
            (10, -10.000000, -0.000001),
            (110, 110.000000, 99999999999999999999999999999999.999999),
            (110, 110.000000, 0.000001),
            (110, -110.000000, -0.000001);"""

    sql "drop table if exists test_runtime_filter_decimal256_1;"
    sql """ create table test_runtime_filter_decimal256_1(k1 int, v1 decimal(38, 6), v2 decimal(38, 6))
                DUPLICATE KEY(`k1`, `v1`, `v2`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    
    sql """insert into test_runtime_filter_decimal256_1 values 
            (11, 10.000000, 99999999999999999999999999999999.999999),
            (111, 10.000000, 99999999999999999999999999999999.999999),
            (11, 10.000000, 0.000001),
            (111, 10.000000, 0.000001),
            (11, -10.000000, -0.000001),
            (111, -10.000000, -0.000001),
            (11, 110.000000, 99999999999999999999999999999999.999999),
            (111, 110.000000, 99999999999999999999999999999999.999999),
            (11, 110.000000, 0.000001),
            (111, 110.000000, 0.000001),
            (11, -110.000000, -0.000001),
            (111, -110.000000, -0.000001);"""
    sql "sync"

    sql """ set runtime_filter_type="IN"; """
    qt_rf_in_1 """
        select
                t0.v2_cast, t1.v2_cast, t0.k1, t0.v1, t1.k1, t1.v1
        from
                (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    qt_rf_in_2 """
        select
                t0.v1_cast, t0.v2_cast, t1.v1_cast, t1.v2_cast, t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v1_cast = t1.v1_cast and t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    sql """ set runtime_filter_type="BLOOM_FILTER"; """
    qt_rf_bf_1 """
        select
                t0.v2_cast, t1.v2_cast, t0.k1, t0.v1, t1.k1, t1.v1
        from
                (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    qt_rf_bf_2 """
        select
                t0.v1_cast, t0.v2_cast, t1.v1_cast, t1.v2_cast, t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v1_cast = t1.v1_cast and t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    sql """ set runtime_filter_type="MIN_MAX"; """
    qt_rf_minmax_1 """
        select
                t0.v2_cast, t1.v2_cast, t0.k1, t0.v1, t1.k1, t1.v1
        from
                (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    qt_rf_minmax_2 """
        select
                t0.v1_cast, t0.v2_cast, t1.v1_cast, t1.v2_cast, t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v1_cast = t1.v1_cast and t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    sql """ set runtime_filter_type="IN_OR_BLOOM_FILTER"; """
    qt_rf_in_or_bf_1 """
        select
                t0.v2_cast, t1.v2_cast, t0.k1, t0.v1, t1.k1, t1.v1
        from
                (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    qt_rf_in_or_bf_2 """
        select
                t0.v1_cast, t0.v2_cast, t1.v1_cast, t1.v2_cast, t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v1_cast = t1.v1_cast and t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    sql """ set runtime_filter_type="BITMAP_FILTER"; """
    qt_rf_bitmap_1 """
        select
                t0.v2_cast, t1.v2_cast, t0.k1, t0.v1, t1.k1, t1.v1
        from
                (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                v1,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """

    qt_rf_bitmap_2 """
        select
                t0.v1_cast, t0.v2_cast, t1.v1_cast, t1.v2_cast, t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_0
                ) t0
                inner join (
                        select
                                k1,
                                cast(v1 as decimal(76, 6)) v1_cast,
                                cast(v2 as decimal(76, 6)) v2_cast
                        from
                                test_runtime_filter_decimal256_1
                ) t1 on t0.v1_cast = t1.v1_cast and t0.v2_cast = t1.v2_cast
        order by
                1,2,3,4,5,6;
    """
}