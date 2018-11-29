﻿namespace ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using ATG.Insert;
    using Buffer = System.Buffer;

    internal class DateColumnType : ColumnType
    {
        private static readonly DateTime UnixTimeBase = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateColumnType()
        {
        }

        public DateColumnType(DateTime[] data)
        {
            Data = data;
        }

        public DateTime[] Data { get; protected set; }

        public override int Rows => Data?.Length ?? 0;

        internal override Type CLRType => typeof(DateTime);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            var itemSize = sizeof(ushort);
            var bytes = formatter.ReadBytes(itemSize * rows);
            var xdata = new ushort[rows];
            Buffer.BlockCopy(bytes, 0, xdata, 0, itemSize * rows);
            Data = xdata.Select(x => UnixTimeBase.AddDays(x)).ToArray();
        }


        public override string AsClickHouseType()
        {
            return "Date";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
            {
                formatter.WriteBytes(BitConverter.GetBytes((ushort) (d - UnixTimeBase).TotalDays));
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                    Data = new[]
                           {
                               DateTime.ParseExact(ProtocolFormatter.UnescapeStringValue(val.StringValue),
                                                   "yyyy-MM-dd", null, DateTimeStyles.AssumeUniversal)
                           };
                    break;
                default:
                    throw new InvalidCastException("Cannot convert numeric value to Date.");
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            switch (parameter.DbType)
            {
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    Data = new[] {(DateTime) Convert.ChangeType(parameter.Value, typeof(DateTime))};
                    break;
                default:
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Date.");
            }
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            throw new InvalidCastException();
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<DateTime>().ToArray();
        }
    }
}