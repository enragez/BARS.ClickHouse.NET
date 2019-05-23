﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using ClickHouse.Ado;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ClickHouse.Test
{
    [TestClass]
    public class SimpleTests
    {
        private ClickHouseConnection GetConnection(string cstr= "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=ch-test.flippingbook.com;Port=9000;Database=default;User=andreya;Password=123")
        {
            var settings = new ClickHouseConnectionSettings(cstr);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }
        
        [TestMethod]
        public void SelectDecimal()
        {
            using (var cnn = GetConnection())
            using (var cmd = cnn.CreateCommand("SELECT date,dec1,dec2,dec3 FROM decimal_test"))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    PrintData(reader);
                }
            }

        }
        [TestMethod]
        public void DecimalParam()
        {
            using (var cnn = GetConnection()){
                var cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
                cmd.AddParameter("d", DbType.Decimal, 666m);
                cmd.ExecuteNonQuery();
                cmd = cnn.CreateCommand("insert into decimal_test values('1970-01-01',@d,@d,@d,@d)");
                cmd.AddParameter("d", DbType.Decimal, -666m);
                cmd.ExecuteNonQuery();
            }

        }
        

        [TestMethod]
        public void SelectIn()
        {
            using (var cnn = GetConnection())
            using (var cmd = cnn.CreateCommand("SELECT * FROM `test_data` WHERE user_id IN (@values)"))
            {
                cmd.Parameters.Add("values", DbType.UInt64, new[] {1L, 2L, 3L});
                using (var reader = cmd.ExecuteReader())
                {
                    PrintData(reader);
                }
            }

        }
        [TestMethod]
        public void SelectFromArray()
        {
            using (var cnn = GetConnection())
            using (var reader = cnn.CreateCommand("SELECT * FROM array_test").ExecuteReader())
            {
                PrintData(reader);
            }
        }

        private static void PrintData(IDataReader reader)
        {
            do
            {
                Console.Write("Fields: ");
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write("{0}:{1} ", reader.GetName(i), reader.GetDataTypeName(i));
                }
                Console.WriteLine();
                while (reader.Read())
                {
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var val = reader.GetValue(i);
                        if (val.GetType().IsArray)
                        {
                            Console.Write('[');
                            Console.Write(string.Join(", ", ((IEnumerable) val).Cast<object>()));
                            Console.Write(']');
                        }
                        else
                        {
                            Console.Write(val);
                        }
                        Console.Write(", ");
                    }
                    Console.WriteLine();
                }
                Console.WriteLine();
            } while (reader.NextResult());
        }

        [TestMethod]
        public void TestInsertNestedColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO nest_test (date,x, values.name,values.value)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now, 1, new[] {"aaaa@bbb.com", "awdasdas"}, new[] {"dsdsds", "dsfdsds"}},
                        new object[] {DateTime.Now.AddHours(-1), 2, new string[0], new string[0]},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnBulk()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO default.`super+` (date,email)values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                {
                    DbType = DbType.Object,
                    ParameterName = "bulk",
                    Value = new[]
                    {
                        new object[] {DateTime.Now,"aaaa@bbb.com"},
                        new object[] {DateTime.Now.AddHours(-1),""},
                    }
                });
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnConst()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,['a','b','c'])");
                cmd.ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestInsertArrayColumnParam()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,@p)");
                cmd.AddParameter("p", new[] {"aaaa@bbb.com", "awdasdas"});
                cmd.ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestPerfromance()
        {
            using (var cnn = GetConnection())
            {
                var cmd = cnn.CreateCommand("SELECT date, time, recordId, parentId, rootId, relatedUser, successState, initiatorType, initiatorPersonSsoId, initiatorPersonToken, initiatorPersonIp, initiatorServiceServer, initiatorServiceService, initiatorServiceDN, reporterType, reporterPersonSsoId, reporterPersonToken, reporterPersonIp, reporterServiceServer, reporterServiceService, reporterServiceDN, type, parameters.name,parameters.value,objectType,objectServer,objectIdentity,objectDescription FROM dev_audit.audit_actions WHERE parentId = 0 AND date> '2017-05-22' ORDER BY time DESC LIMIT 0,10");
                cmd.AddParameter("p", new[] { "aaaa@bbb.com", "awdasdas" });
                var list = new List<List<object>>();
                var times = new List<TimeSpan>();
                var sw=new Stopwatch();
                sw.Start();
                using (var reader = cmd.ExecuteReader())
                {
                    times.Add(sw.Elapsed);
                    sw.Restart();
                    reader.ReadAll(x =>
                    {
                        var rowList = new List<Object>();
                        for (var i = 0; i < x.FieldCount; i++)
                            rowList.Add(x.GetValue(i));
                        list.Add(rowList);
                        times.Add(sw.Elapsed);
                        sw.Restart();
                    });
                }
                sw.Stop();
                
            }
        }
        [TestMethod]
        public void TestInsertFieldless()
        {
            using (var cnn = GetConnection())
            {
                var sql = "insert into vince_test values ('2017-05-17','CSA_CPTY1233',0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }
        [TestMethod]
        public void TestChecksumError()
        {
            using (var cnn = GetConnection())
            {
                var sql = "insert into vince_test(fakedate, csa, server) values('2017-05-17', 'CSA_CPTY1233', 0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }

        [TestMethod]
        public void TestBigRequest() {
            var sql = @"select _f1,_f2,sum(_f3)AS _f3,sum(_f4)AS _f4,groupUniqArray(_f5)[1]AS _f5,sum(_f6)AS _f6,sum(_f7)AS _f7,sum(_f8)AS _f8,sum(_f9)AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734827 AS _f2,countMerge(total_views)AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['755571'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734827 AS _f2,0 AS _f3,countMerge(total_views)AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['755571'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT _f1,734827 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,sum(pageViews)AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,uniqCombinedMerge(views)AS pageViews FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['755571'],publication_hid)GROUP BY publication_hid,page,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734827 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,sumMerge(total_time)/countMerge(avg_time)AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,sumMergeState(total_time)AS total_time,countMergeState(avg_time)AS avg_time FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['755571'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734827 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,sum(views)AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,publication_hid,uniqCombinedMerge(views)as views,page FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['755571'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7),page)GROUP BY publication_hid,_f1 UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734827 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,sumMerge(total_time)/countMerge(avg_time)AS _f9 FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['755571'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))group by _f2,_f1 ORDER BY _f2,_f1
 UNION ALL 
select _f1,_f2,sum(_f3)AS _f3,sum(_f4)AS _f4,groupUniqArray(_f5)[1]AS _f5,sum(_f6)AS _f6,sum(_f7)AS _f7,sum(_f8)AS _f8,sum(_f9)AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734825 AS _f2,countMerge(total_views)AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['958005'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734825 AS _f2,0 AS _f3,countMerge(total_views)AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['958005'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT _f1,734825 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,sum(pageViews)AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,uniqCombinedMerge(views)AS pageViews FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['958005'],publication_hid)GROUP BY publication_hid,page,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734825 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,sumMerge(total_time)/countMerge(avg_time)AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,sumMergeState(total_time)AS total_time,countMergeState(avg_time)AS avg_time FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['958005'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734825 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,sum(views)AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,publication_hid,uniqCombinedMerge(views)as views,page FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['958005'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7),page)GROUP BY publication_hid,_f1 UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734825 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,sumMerge(total_time)/countMerge(avg_time)AS _f9 FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['958005'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))group by _f2,_f1 ORDER BY _f2,_f1
 UNION ALL 
select _f1,_f2,sum(_f3)AS _f3,sum(_f4)AS _f4,groupUniqArray(_f5)[1]AS _f5,sum(_f6)AS _f6,sum(_f7)AS _f7,sum(_f8)AS _f8,sum(_f9)AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734823 AS _f2,countMerge(total_views)AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['82370'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734823 AS _f2,0 AS _f3,countMerge(total_views)AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['82370'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT _f1,734823 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,sum(pageViews)AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,uniqCombinedMerge(views)AS pageViews FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['82370'],publication_hid)GROUP BY publication_hid,page,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734823 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,sumMerge(total_time)/countMerge(avg_time)AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,sumMergeState(total_time)AS total_time,countMergeState(avg_time)AS avg_time FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['82370'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734823 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,sum(views)AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,publication_hid,uniqCombinedMerge(views)as views,page FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['82370'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7),page)GROUP BY publication_hid,_f1 UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734823 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,sumMerge(total_time)/countMerge(avg_time)AS _f9 FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['82370'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))group by _f2,_f1 ORDER BY _f2,_f1
 UNION ALL 
select _f1,_f2,sum(_f3)AS _f3,sum(_f4)AS _f4,groupUniqArray(_f5)[1]AS _f5,sum(_f6)AS _f6,sum(_f7)AS _f7,sum(_f8)AS _f8,sum(_f9)AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734822 AS _f2,countMerge(total_views)AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['518248'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734822 AS _f2,0 AS _f3,countMerge(total_views)AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['518248'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT _f1,734822 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,sum(pageViews)AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,uniqCombinedMerge(views)AS pageViews FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['518248'],publication_hid)GROUP BY publication_hid,page,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734822 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,sumMerge(total_time)/countMerge(avg_time)AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,sumMergeState(total_time)AS total_time,countMergeState(avg_time)AS avg_time FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['518248'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734822 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,sum(views)AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,publication_hid,uniqCombinedMerge(views)as views,page FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['518248'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7),page)GROUP BY publication_hid,_f1 UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734822 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,sumMerge(total_time)/countMerge(avg_time)AS _f9 FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['518248'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))group by _f2,_f1 ORDER BY _f2,_f1
 UNION ALL 
select _f1,_f2,sum(_f3)AS _f3,sum(_f4)AS _f4,groupUniqArray(_f5)[1]AS _f5,sum(_f6)AS _f6,sum(_f7)AS _f7,sum(_f8)AS _f8,sum(_f9)AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734820 AS _f2,countMerge(total_views)AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['268436'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734820 AS _f2,0 AS _f3,countMerge(total_views)AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['268436'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7)UNION ALL SELECT _f1,734820 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,sum(pageViews)AS _f6,0 AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,uniqCombinedMerge(views)AS pageViews FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['268436'],publication_hid)GROUP BY publication_hid,page,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734820 AS _f2,0 AS _f3,0 AS _f4,NULL AS _f5,0 AS _f6,sumMerge(total_time)/countMerge(avg_time)AS _f7,0 AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,sumMergeState(total_time)AS total_time,countMergeState(avg_time)AS avg_time FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND has(['268436'],publication_hid)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))GROUP BY _f1 UNION ALL SELECT _f1,734820 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,sum(views)AS _f8,0 AS _f9 FROM(SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,publication_hid,uniqCombinedMerge(views)as views,page FROM page_view_aggregated_page WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['268436'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7),page)GROUP BY publication_hid,_f1 UNION ALL SELECT intDiv((toDate('2019-03-22')-date)-1,7)AS _f1,734820 AS _f2,0 AS _f3,0 AS _f4,publication_hid AS _f5,0 AS _f6,0 AS _f7,0 AS _f8,sumMerge(total_time)/countMerge(avg_time)AS _f9 FROM page_view_aggregated_exit WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-14))AND publication_hid IN(SELECT publication_hid FROM page_view_aggregated_session WHERE(date<'2019-03-22')AND(date>=(toDate('2019-03-22')-7))AND has(['268436'],publication_hid)GROUP BY publication_hid ORDER BY countMerge(total_views)DESC limit 1)GROUP BY publication_hid,intDiv((toDate('2019-03-22')-date)-1,7))group by _f2,_f1 ORDER BY _f2,_f1";
            using (var cnn = GetConnection("Compress=True;BufferSize=32768;SocketTimeout=10000;CheckCompressedHash=False;Compressor=lz4;Host=ch-release.flippingbook.com;Port=9000;Database=release_fbo;User=andreya;Password=123")) {
                var cmd = cnn.CreateCommand();
                cmd.CommandText = sql;
                var reader = cmd.ExecuteReader();
                reader.ReadAll(rowReader => { });
            }
        }
    }
}