namespace BARS.ClickHouse.Ado
{
    using System;
    using System.Data;

    public static class AdoExtensions
    {
        public static void ReadAll(this IDataReader reader, Action<IDataReader> rowAction)
        {
            do
            {
                while (reader.Read())
                {
                    rowAction(reader);
                }
                
            } while (reader.NextResult());
        }
    }
}