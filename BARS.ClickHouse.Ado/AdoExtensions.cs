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
        
        public static T AddParameter<T>(this T cmd, string name, DbType type, object value) where T : IDbCommand
        {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.DbType = type;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }

        public static T AddParameter<T>(this T cmd, string name, object value) where T : IDbCommand
        {
            var par = cmd.CreateParameter();
            par.ParameterName = name;
            par.Value = value;
            cmd.Parameters.Add(par);
            return cmd;
        }
    }
}