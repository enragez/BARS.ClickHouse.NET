namespace BARS.ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.InteropServices;
    using ATG.Insert;
    using Buffer = System.Buffer;

    internal class SimpleColumnType<T> : ColumnType
    {
        public SimpleColumnType()
        {
        }

        public SimpleColumnType(T[] data)
        {
            Data = data;
        }

        public T[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;

        internal override Type CLRType => typeof(T);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            var itemSize = Marshal.SizeOf(typeof(T));
            var bytes = formatter.ReadBytes(itemSize * rows);
            Data = new T[rows];
            Buffer.BlockCopy(bytes, 0, Data, 0, itemSize * rows);
        }

        public override string AsClickHouseType()
        {
            if (typeof(T) == typeof(double))
            {
                return "Float64";
            }

            if (typeof(T) == typeof(float))
            {
                return "Float32";
            }

            if (typeof(T) == typeof(byte))
            {
                return "UInt8";
            }

            if (typeof(T) == typeof(sbyte))
            {
                return "Int8";
            }

            return typeof(T).Name;
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            var itemSize = Marshal.SizeOf(typeof(T));
            var bytes = new byte[itemSize * rows];
            Buffer.BlockCopy(Data, 0, bytes, 0, itemSize * rows);
            formatter.WriteBytes(bytes);
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                    Data = new[]
                           {
                               (T) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue),
                                                      typeof(T))
                           };
                    break;
                case Parser.ConstType.Number:
                    Data = new[] {(T) Convert.ChangeType(val.StringValue, typeof(T))};
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            switch (parameter.DbType)
            {
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                case DbType.Single:
                case DbType.Decimal:
                case DbType.Double:
                    Data = new[] {(T) Convert.ChangeType(parameter.Value, typeof(T))};
                    break;
                default:
                    throw new
                        InvalidCastException($"Cannot convert parameter with type {parameter.DbType} to {typeof(T).Name}.");
            }
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            return Convert.ToInt64(Data[currentRow]);
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<T>().ToArray();
        }
    }
}