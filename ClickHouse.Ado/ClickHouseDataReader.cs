namespace ClickHouse.Ado
{
    using System;
    using System.Data;
    using Impl.ColumnTypes;
    using Impl.Data;

    public class ClickHouseDataReader : IDataReader
    {
        private readonly CommandBehavior _behavior;

        private ClickHouseConnection _clickHouseConnection;

        internal Block Current { get; private set; }

        private int _currentRow;

        internal ClickHouseDataReader(ClickHouseConnection clickHouseConnection, CommandBehavior behavior)
        {
            _clickHouseConnection = clickHouseConnection;
            _behavior = behavior;
            NextResult();
        }

        public void Dispose()
        {
            Close();
        }

        public string GetName(int i)
        {
            return Current.Columns[i].Name;
        }

        public string GetDataTypeName(int i)
        {
            if (Current == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return Current.Columns[i].Type.AsClickHouseType();
        }

        public Type GetFieldType(int i)
        {
            if (Current == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return Current.Columns[i].Type.CLRType;
        }

        public object GetValue(int i)
        {
            if (Current == null || Current.Rows <= _currentRow || i < 0 || i >= FieldCount)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return Current.Columns[i].Type.Value(_currentRow);
        }

        public int GetValues(object[] values)
        {
            if (Current == null || Current.Rows <= _currentRow)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            var n = Math.Max(values.Length, Current.Columns.Count);
            for (var i = 0; i < n; i++)
            {
                values[i] = Current.Columns[i].Type.Value(_currentRow);
            }

            return n;
        }

        public int GetOrdinal(string name)
        {
            if (Current == null || Current.Rows <= _currentRow)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return Current.Columns.FindIndex(x => x.Name == name);
        }

        public bool GetBoolean(int i)
        {
            return GetInt64(i) != 0;
        }

        public byte GetByte(int i)
        {
            return (byte) GetInt64(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public char GetChar(int i)
        {
            return (char) GetInt64(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotSupportedException();
        }

        public short GetInt16(int i)
        {
            return (short) GetInt64(i);
        }

        public int GetInt32(int i)
        {
            return (int) GetInt64(i);
        }

        public long GetInt64(int i)
        {
            if (Current == null || Current.Rows <= _currentRow || i < 0 || i >= FieldCount)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return Current.Columns[i].Type.IntValue(_currentRow);
        }

        public float GetFloat(int i)
        {
            return Convert.ToSingle(GetValue(i));
        }

        public double GetDouble(int i)
        {
            return Convert.ToDouble(GetValue(i));
        }

        public string GetString(int i)
        {
            return GetValue(i).ToString();
        }

        public decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(GetValue(i));
        }

        public DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(GetValue(i));
        }

        public IDataReader GetData(int i)
        {
            throw new NotSupportedException();
        }

        object IDataRecord.this[int i] => GetValue(i);

        object IDataRecord.this[string name] => GetValue(GetOrdinal(name));

        public bool IsDBNull(int i)
        {
            if (Current == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            if (Current.Columns[i].Type is NullableColumnType type)
            {
                return type.IsNull(_currentRow);
            }

            return false;
        }

        public int FieldCount => Current.Columns.Count;


        public void Close()
        {
            if (Current != null)
            {
                _clickHouseConnection.Formatter.ReadResponse();
            }

            if ((_behavior & CommandBehavior.CloseConnection) != 0)
            {
                _clickHouseConnection.Close();
            }

            _clickHouseConnection = null;
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            _currentRow = -1;
            return (Current = _clickHouseConnection.Formatter.ReadBlock()) != null;
        }

        public bool Read()
        {
            if (Current == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            _currentRow++;
            return Current.Rows > _currentRow;
        }

        public int Depth { get; } = 1;

        public bool IsClosed => _clickHouseConnection == null;

        public int RecordsAffected => Current.Rows;
    }
}