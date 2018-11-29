namespace ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using ATG.Insert;

    internal class EnumColumnType : ColumnType
    {
        public EnumColumnType(int baseSize, IEnumerable<Tuple<string, int>> values)
        {
            Values = values;
            BaseSize = baseSize;
        }

        public IEnumerable<Tuple<string, int>> Values { get; }

        public int BaseSize { get; }

        public int[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;

        internal override Type CLRType => typeof(int);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            switch (BaseSize)
            {
                case 8:
                {
                    var vals = new SimpleColumnType<byte>();
                    vals.Read(formatter, rows);
                    Data = vals.Data.Select(x => (int) x).ToArray();
                    break;
                }
                case 16:
                {
                    var vals = new SimpleColumnType<short>();
                    vals.Read(formatter, rows);
                    Data = vals.Data.Select(x => (int) x).ToArray();
                    break;
                }
                default:
                    throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
            }
        }

        public override string AsClickHouseType()
        {
            return $"Enum{BaseSize}({string.Join(",", Values.Select(x => $"{x.Item1}={x.Item2}"))})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            switch (BaseSize)
            {
                case 8:
                    new SimpleColumnType<byte>(Data.Select(x => (byte) x).ToArray()).Write(formatter, rows);
                    break;
                case 16:
                    new SimpleColumnType<short>(Data.Select(x => (short) x).ToArray()).Write(formatter, rows);
                    break;
                default:
                    throw new NotSupportedException($"Enums with base size {BaseSize} are not supported.");
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                {
                    var uvalue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
                    Data = new[] {Values.First(x => x.Item1 == uvalue).Item2};
                    break;
                }
                default:
                    Data = new[] {int.Parse(val.StringValue)};
                    break;
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            switch (parameter.DbType)
            {
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    Data = new[] {Values.First(x => x.Item1 == parameter.Value?.ToString()).Item2};
                    break;
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                    Data = new[] {(int) Convert.ChangeType(parameter.Value, typeof(int))};
                    break;
                default:
                    throw new InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to Enum.");
            }
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            return Data[currentRow];
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<int>().ToArray();
        }
    }
}