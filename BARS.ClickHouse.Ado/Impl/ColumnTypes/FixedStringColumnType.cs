namespace BARS.ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using ATG.Insert;

    internal class FixedStringColumnType : ColumnType
    {
        public FixedStringColumnType(uint length)
        {
            Length = length;
        }

        public uint Length { get; }

        public string[] Data { get; private set; }

        public override int Rows => Data?.Length ?? 0;

        internal override Type CLRType => typeof(string);

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            Data = new string[rows];
            var bytes = formatter.ReadBytes((int) (rows * Length));
            for (var i = 0; i < rows; i++)
            {
                Data[i] = Encoding.UTF8.GetString(bytes, (int) (i * Length), (int) Length);
            }
        }

        public override string AsClickHouseType()
        {
            return $"FixedString({Length})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var d in Data)
            {
                var bytes = Encoding.UTF8.GetBytes(d);
                var left = Length - bytes.Length;
                if (left > 0)
                {
                    formatter.WriteBytes(bytes);
                    formatter.WriteBytes(new byte[left]);
                }
                else
                {
                    formatter.WriteBytes(bytes.Take((int) Length).ToArray());
                }
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            switch (val.TypeHint)
            {
                case Parser.ConstType.String:
                {
                    var uvalue = ProtocolFormatter.UnescapeStringValue(val.StringValue);
                    Data = new[] {uvalue};
                    break;
                }
                default:
                    Data = new[] {val.StringValue};
                    break;
            }
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            Data = new[] {parameter.Value?.ToString()};
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
            var data = new List<string>();

            foreach (var val in objects)
            {
                if (val is Guid)
                {
                    data.Add(val.ToString());
                }
                else
                {
                    data.Add((string)val);
                }
            }
            
            Data = data.ToArray();
        }
    }
}