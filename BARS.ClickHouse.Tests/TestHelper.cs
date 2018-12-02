namespace BARS.ClickHouse.Tests
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Linq;
    using Ado;

    public static class TestHelper
    {
        private const string ConnectionString = @"Compress=True;
                                                  CheckCompressedHash=False;
                                                  Compressor=lz4;
                                                  Host=192.168.228.116;
                                                  Port=9000;
                                                  Database=tests;
                                                  User=default;
                                                  Password=123";
        
        public static ClickHouseConnection GetConnection(string connString = ConnectionString)
        {
            var settings = new ClickHouseConnectionSettings(connString);
            var cnn = new ClickHouseConnection(settings);
            cnn.Open();
            return cnn;
        }

        public static void PrintData(IDataReader reader)
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
    }
}