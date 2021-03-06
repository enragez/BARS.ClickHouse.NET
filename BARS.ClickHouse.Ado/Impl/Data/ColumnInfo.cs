﻿namespace BARS.ClickHouse.Ado.Impl.Data
{
    using ColumnTypes;

    internal class ColumnInfo
    {
        public string Name { get; set; }

        public ColumnType Type { get; set; }

        internal void Write(ProtocolFormatter formatter, int rows)
        {
            formatter.WriteString(Name);
            formatter.WriteString(Type.AsClickHouseType());

            if (rows > 0)
            {
                Type.Write(formatter, rows);
            }
        }

        public static ColumnInfo Read(ProtocolFormatter formatter, int rows)
        {
            var rv = new ColumnInfo();
            rv.Name = formatter.ReadString();
            rv.Type = ColumnType.Create(formatter.ReadString());
            if (rows > 0)
            {
                rv.Type.Read(formatter, rows);
            }

            return rv;
        }
    }
}