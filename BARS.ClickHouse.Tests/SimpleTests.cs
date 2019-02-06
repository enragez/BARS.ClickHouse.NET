namespace BARS.ClickHouse.Tests
{
    using System;
    using System.Data;
    using Ado;
    using NUnit.Framework;

    [TestFixture]
    public class SimpleTests
    {
        [Test]
        public void DecimalParam()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var cmd = cnn.CreateCommand("insert into decimal_test  (date, dec1, dec2, dec3) values('1970-01-01',@d,@d,@d)");
                cmd.AddParameter("d", DbType.Decimal, 666m);
                cmd.ExecuteNonQuery();
                
                cmd = cnn.CreateCommand("insert into decimal_test (date, dec1, dec2, dec3) values('1970-01-01',@d,@d,@d)");
                cmd.AddParameter("d", DbType.Decimal, -666m);
                cmd.ExecuteNonQuery();
            }
        }

        [Test]
        public void InsertAndSelectEquals()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var date = new DateTime(2018, 12, 2);

                var dateString = date.ToString("yyyy-MM-dd");

                cnn.CreateCommand($"INSERT INTO test_data (date, user_id) VALUES ('{dateString}', 228)")
                .ExecuteNonQuery();

                var dateResult = (DateTime) cnn.CreateCommand("SELECT date FROM test_data WHERE user_id = 228")
                                            .ExecuteScalar();

                Assert.AreEqual(date, dateResult);
            }
        }

        [Test]
        public void SelectDecimal()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                using (var cmd = cnn.CreateCommand("SELECT date, dec1, dec2 ,dec3 FROM decimal_test"))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        TestHelper.PrintData(reader);
                    }
                }
            }
        }

        [Test]
        public void SelectFromArray()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                using (var reader = cnn.CreateCommand("SELECT * FROM array_test").ExecuteReader())
                {
                    TestHelper.PrintData(reader);
                }
            }
        }

        [Test]
        public void SelectIn()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                using (var cmd = cnn.CreateCommand("SELECT * FROM `test_data` WHERE user_id IN (@values)"))
                {
                    cmd.Parameters.Add("values", DbType.UInt64, new[] {1L, 2L, 3L});
                    using (var reader = cmd.ExecuteReader())
                    {
                        TestHelper.PrintData(reader);
                    }
                }
            }
        }

        [Test]
        public void ShouldConvertIntoConnectionStringAndBack()
        {
            const string connectionString =
                "Compress=True;CheckCompressedHash=False;Compressor=lz4;Host=192.168.228.116;Port=9000;User=default;Password=123;SocketTimeout=600000;Database=tests;";
            var expectedSettings = new ClickHouseConnectionSettings(connectionString);
            var actualSettings = new ClickHouseConnectionSettings(expectedSettings.ToString());

            Assert.AreEqual(expectedSettings.BufferSize, actualSettings.BufferSize);
            Assert.AreEqual(expectedSettings.SocketTimeout, actualSettings.SocketTimeout);
            Assert.AreEqual(expectedSettings.Host, actualSettings.Host);
            Assert.AreEqual(expectedSettings.Port, actualSettings.Port);
            Assert.AreEqual(expectedSettings.Database, actualSettings.Database);
            Assert.AreEqual(expectedSettings.Compress, actualSettings.Compress);
            Assert.AreEqual(expectedSettings.Compressor, actualSettings.Compressor);
            Assert.AreEqual(expectedSettings.CheckCompressedHash, actualSettings.CheckCompressedHash);
            Assert.AreEqual(expectedSettings.User, actualSettings.User);
            Assert.AreEqual(expectedSettings.Password, actualSettings.Password);
        }

        [Test]
        public void TestChecksumError()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var sql = "insert into vince_test(date, csa, server) values('2017-05-17', 'CSA_CPTY1233', 0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertArrayColumnBulk()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO bulk_test (date,email) values @bulk;");
                cmd.Parameters.Add(new ClickHouseParameter
                                   {
                                       DbType = DbType.Object,
                                       ParameterName = "bulk",
                                       Value = new[]
                                               {
                                                   new object[] {DateTime.Now, "aaaa@bbb.com"},
                                                   new object[] {DateTime.Now.AddHours(-1), ""}
                                               }
                                   });
                cmd.ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertArrayColumnConst()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var cmd =
                    cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,['a','b','c'])");
                cmd.ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertArrayColumnParam()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var cmd = cnn.CreateCommand("INSERT INTO array_test (date,x, arr)values ('2017-01-01',1,@p)");
                cmd.AddParameter("p", new[] {"aaaa@bbb.com", "awdasdas"});
                cmd.ExecuteNonQuery();
            }
        }

        [Test]
        public void TestInsertFieldless()
        {
            using (var cnn = TestHelper.GetConnection())
            {
                var sql = "insert into vince_test values ('2017-05-17','CSA_CPTY1233',0)";
                cnn.CreateCommand(sql).ExecuteNonQuery();
            }
        }
    }
}