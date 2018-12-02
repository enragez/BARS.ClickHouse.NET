namespace BARS.ClickHouse.Ado
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using Impl.ColumnTypes;
    using Impl.Data;

    /// <summary> DataReader with all data readed from ClickHouseDataReader </summary>
    /// <remarks> Sometimes need when other frameworks calls for IDataReader and doesn't call NextResult</remarks>
    public class ClickHouseInMemoryReader : IDataReader
    {
        private readonly List<List<ColumnInfo>> _data;

        private int _currentRowIndex;

        internal ClickHouseInMemoryReader(List<List<ColumnInfo>> data)
        {
            _data = data;

            _currentRowIndex = -1;
        }
        
        public void Dispose()
        {
        }

        public string GetName(int i)
        {
            return _data[_currentRowIndex][i].Name;
        }

        public string GetDataTypeName(int i)
        {
            return _data[_currentRowIndex][i].Type.AsClickHouseType();
        }

        public Type GetFieldType(int i)
        {
            return _data[_currentRowIndex][i].Type.CLRType;
        }

        public object GetValue(int i)
        {
            return _data[_currentRowIndex][i].Type.Value(_currentRowIndex);
        }

        public int GetValues(object[] values)
        {
            var n = Math.Max(values.Length, _data[_currentRowIndex].Count);
            for (var i = 0; i < n; i++)
            {
                values[i] = _data[_currentRowIndex][i].Type.Value(_currentRowIndex);
            }

            return n;
        }

        public int GetOrdinal(string name)
        {
            for (var i = 0; i < _data[_currentRowIndex].Count; i++)
            {
                if (_data[_currentRowIndex][i].Name == name)
                {
                    return i;
                }
            }

            return -1;
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
            return new Guid(GetString(i));
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
            return _data[_currentRowIndex][i].Type.IntValue(_currentRowIndex);
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

        public bool IsDBNull(int i)
        {
            if (_data[_currentRowIndex] == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            if (_data[_currentRowIndex][i].Type is NullableColumnType type)
            {
                return type.IsNull(_currentRowIndex);
            }

            return false;
        }

        public int FieldCount => _data[_currentRowIndex].Count;

        public object this[int i] => GetValue(i);

        public object this[string name] => GetValue(GetOrdinal(name));

        public void Close()
        {
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            _currentRowIndex++;

            return _currentRowIndex < _data.Count;
        }

        public int Depth { get; } = 1;

        public bool IsClosed => false;

        public int RecordsAffected => _data.Count;
    }
}