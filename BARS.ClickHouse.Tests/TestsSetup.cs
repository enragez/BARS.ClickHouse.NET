namespace BARS.ClickHouse.Tests
{
    using NUnit.Framework;

    [SetUpFixture]
    public class TestsSetup
    {
        private const string DefaultConnectionString = @"Compress=True;
                                                         Host=192.168.228.116;
                                                         Port=9000;
                                                         Database=default;
                                                         User=default;
                                                         Password=123";
        [OneTimeSetUp]
        public void RunBeforeAnyTests()
        {
            using (var conn = TestHelper.GetConnection(DefaultConnectionString))
            {
                conn.CreateCommand("CREATE DATABASE IF NOT EXISTS tests").ExecuteNonQuery();
                
                conn.ChangeDatabase("tests");
                
                conn.CreateCommand(@"CREATE TABLE IF NOT EXISTS array_test
                                     ( date Date, 
                                       x Int32,  
                                       arr Array(String) 
                                     ) ENGINE = MergeTree(date, x, 8192)")
                    .ExecuteNonQuery();
                
                conn.CreateCommand(@"CREATE TABLE IF NOT EXISTS test_data 
                                     ( date Date, 
                                       user_id UInt64
                                     ) ENGINE = MergeTree(date, user_id, 8192)")
                    .ExecuteNonQuery();
                
                conn.CreateCommand(@"CREATE TABLE IF NOT EXISTS vince_test 
                                     ( date Date, 
                                       csa String,
                                       server Int32
                                     ) ENGINE = MergeTree(date, server, 8192)")
                    .ExecuteNonQuery();
                
                conn.CreateCommand(@"CREATE TABLE IF NOT EXISTS bulk_test 
                                     ( date Date, 
                                       email String
                                     ) ENGINE = MergeTree(date, email, 8192)")
                    .ExecuteNonQuery();
            }
        }
    }
}