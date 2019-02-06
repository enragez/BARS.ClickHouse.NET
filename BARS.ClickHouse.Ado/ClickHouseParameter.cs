namespace BARS.ClickHouse.Ado
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Linq;
    using Impl;

    public class ClickHouseParameter : IDbDataParameter
    {
        public DbType DbType { get; set; }

        public string ParameterName { get; set; }

        public object Value { get; set; }

        ParameterDirection IDataParameter.Direction { get; set; }

        bool IDataParameter.IsNullable => false;

        string IDataParameter.SourceColumn { get; set; }

        DataRowVersion IDataParameter.SourceVersion { get; set; }

        byte IDbDataParameter.Precision { get; set; }

        byte IDbDataParameter.Scale { get; set; }

        int IDbDataParameter.Size { get; set; }

        private string AsSubstitute(object val)
        {
            if (DbType == DbType.String || DbType == DbType.AnsiString ||
                DbType == DbType.StringFixedLength || DbType == DbType.AnsiStringFixedLength ||
                DbType == 0 && val is string)
            {
                if (!(val is string) && val is IEnumerable)
                {
                    return string.Join(",", ((IEnumerable) val).Cast<object>().Select(AsSubstitute));
                }
                
                return ProtocolFormatter.EscapeStringValue(val.ToString());
            }

            if (DbType == DbType.DateTime || DbType == DbType.DateTime2 ||
                DbType == DbType.DateTime2 || DbType == 0 && val is DateTime)
            {
                return $"'{(DateTime) val:yyyy-MM-dd HH:mm:ss}'";
            }

            if (DbType == DbType.Date)
            {
                return $"'{(DateTime) val:yyyy-MM-dd}'";
            }

            if (DbType != 0 && DbType != DbType.Object &&
                !(val is string) && val is IEnumerable enumerable1)
            {
                return string.Join(",", enumerable1.Cast<object>().Select(AsSubstitute));
            }

            if ((DbType == 0 || DbType == DbType.Object) && !(val is string) && val is IEnumerable enumerable)
            {
                return "[" + string.Join(",", enumerable.Cast<object>().Select(AsSubstitute)) + "]";
            }

            return val.ToString();
        }

        public string AsSubstitute()
        {
            return AsSubstitute(Value);
        }

        public override string ToString()
        {
            return $"{ParameterName}({DbType}): {Value}";
        }
    }
}