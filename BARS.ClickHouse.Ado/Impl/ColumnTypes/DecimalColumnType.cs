namespace BARS.ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using System.Linq;
    using BARS.ClickHouse.Ado;
    using BARS.ClickHouse.Ado.Impl;
    using BARS.ClickHouse.Ado.Impl.ATG.Insert;

    internal class DecimalColumnType : ColumnType
    {
        private readonly int _byteLength;
        private readonly uint _length;
        private readonly uint _precision;
        private readonly decimal _exponent;

        public DecimalColumnType(uint length, uint precision)
        {
            _length = length;
            _precision = precision;
            if (_length >= 30)
            {
                throw new
                    ClickHouseException("Decimals with length >= 30 are not supported (.NET framework decimal range limit)");
            }

            if (length <= 9)
            {
                _byteLength = 4;
            }
            else if (length <= 18)
            {
                _byteLength = 8;
            }
            else if (length <= 38)
            {
                _byteLength = 16;
            }
            else
            {
                throw new ClickHouseException($"Invalid Decimal length {length}");
            }

            _exponent = (decimal) Math.Pow(10, precision);
        }

        public decimal[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;
        
        internal override Type CLRType => typeof(decimal);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Data = new decimal[rows];
            var bytes = formatter.ReadBytes(rows * _byteLength);
            for (var i = 0; i < rows; i++)
            {
                if (_byteLength == 4)
                {
                    Data[i] = BitConverter.ToInt32(bytes, i * _byteLength) / _exponent;
                }
                else if (_byteLength == 4)
                {
                    Data[i] = BitConverter.ToInt64(bytes, i * _byteLength) / _exponent;
                }
                else
                {
                    var c = (byte) ((bytes[(1 + i) * _byteLength - 1] & 0x80) != 0 ? 0xff : 0);
                    decimal current = 0;
                    for (var k = 0; k < _byteLength; k++)
                    {
                        current *= 0x100;
                        current += c ^ bytes[(1 + i) * _byteLength - k - 1];
                    }

                    Data[i] = (c != 0 ? -(current + 1) : current) / _exponent;
                }
            }
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
            {
                var premultiplied = d * _exponent;
                switch (_byteLength)
                {
                    case 4:
                        formatter.WriteBytes(BitConverter.GetBytes((int) premultiplied));
                        break;
                    case 8:
                        formatter.WriteBytes(BitConverter.GetBytes((long) premultiplied));
                        break;
                    default:
                    {
                        var c = (byte) (premultiplied > 0 ? 0 : 0xff);
                        if (c != 0)
                        {
                            premultiplied = -premultiplied - 1;
                        }

                        for (var i = 0; i < _byteLength; i++)
                        {
                            var next = (byte) ((byte) (Math.Truncate(premultiplied) % 0xff) ^ c);
                            premultiplied = Math.Truncate(premultiplied / 0x100);
                            formatter.WriteByte(next);
                        }

                        break;
                    }
                }
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                    Data = new[]
                           {
                               (decimal) Convert.ChangeType(ProtocolFormatter.UnescapeStringValue(val.StringValue),
                                                            typeof(decimal))
                           };
                    break;
                case Parser.ConstType.Number:
                    Data = new[] {(decimal) Convert.ChangeType(val.StringValue, typeof(decimal))};
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        public override string AsClickHouseType()
        {
            return $"Decimal({_length}, {_precision})";
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            Data = new[] {(decimal) Convert.ChangeType(parameter.Value, typeof(decimal))};
        }

        public override object Value(int currentRow)
        {
            return Data[currentRow];
        }

        public override long IntValue(int currentRow)
        {
            return (long) Data[currentRow];
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            Data = objects.Cast<decimal>().ToArray();
        }
    }
}