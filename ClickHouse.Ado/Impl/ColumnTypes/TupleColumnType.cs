﻿namespace ClickHouse.Ado.Impl.ColumnTypes
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using ATG.Insert;

    internal class TupleColumnType : ColumnType
    {
        public TupleColumnType(IEnumerable<ColumnType> values)
        {
            Columns = values.ToArray();
        }

        public ColumnType[] Columns { get; }

        public override int Rows => Columns.First().Rows;

        internal override Type CLRType => typeof(Tuple<>).MakeGenericType(Columns.Select(x => x.CLRType).ToArray());

        internal override void Read(ProtocolFormatter formatter, int rows)
        {
            foreach (var column in Columns)
            {
                column.Read(formatter, rows);
            }
        }

        public override string AsClickHouseType()
        {
            return $"Tuple({string.Join(",", Columns.Select(x => x.AsClickHouseType()))})";
        }

        public override void Write(ProtocolFormatter formatter, int rows)
        {
            Debug.Assert(Rows == rows, "Row count mismatch!");
            foreach (var column in Columns)
            {
                column.Write(formatter, rows);
            }
        }

        public override void ValueFromConst(Parser.ValueType val)
        {
            throw new NotSupportedException();
        }

        public override void ValueFromParam(ClickHouseParameter parameter)
        {
            throw new NotImplementedException();
        }

        public override object Value(int currentRow)
        {
            return Columns.Select(x => x.Value(currentRow)).ToArray();
        }

        public override long IntValue(int currentRow)
        {
            throw new InvalidCastException();
        }

        public override void ValuesFromConst(IEnumerable objects)
        {
            throw new NotImplementedException();
        }
    }
}