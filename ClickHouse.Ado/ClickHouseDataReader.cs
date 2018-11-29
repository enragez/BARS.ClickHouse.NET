﻿namespace ClickHouse.Ado
{
    using System;
    using System.Data;
    using Impl.ColumnTypes;
    using Impl.Data;

    public class ClickHouseDataReader : IDataReader
    {
        private readonly CommandBehavior _behavior;

        private ClickHouseConnection _clickHouseConnection;

        private Block _currentBlock;

        private int _currentRow;

        internal ClickHouseDataReader(ClickHouseConnection clickHouseConnection, CommandBehavior behavior
        )
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
            return _currentBlock.Columns[i].Name;
        }

        public string GetDataTypeName(int i)
        {
            if (_currentBlock == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return _currentBlock.Columns[i].Type.AsClickHouseType();
        }

        public Type GetFieldType(int i)
        {
            if (_currentBlock == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return _currentBlock.Columns[i].Type.CLRType;
        }

        public object GetValue(int i)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return _currentBlock.Columns[i].Type.Value(_currentRow);
        }

        public int GetValues(object[] values)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            var n = Math.Max(values.Length, _currentBlock.Columns.Count);
            for (var i = 0; i < n; i++)
            {
                values[i] = _currentBlock.Columns[i].Type.Value(_currentRow);
            }

            return n;
        }

        public int GetOrdinal(string name)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return _currentBlock.Columns.FindIndex(x => x.Name == name);
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
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            return _currentBlock.Columns[i].Type.IntValue(_currentRow);
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
            if (_currentBlock == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            if (_currentBlock.Columns[i].Type is NullableColumnType type)
            {
                return type.IsNull(_currentRow);
            }

            return false;
        }

        public int FieldCount => _currentBlock.Columns.Count;


        public void Close()
        {
            if (_currentBlock != null)
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
            return (_currentBlock = _clickHouseConnection.Formatter.ReadBlock()) != null;
        }

        public bool Read()
        {
            if (_currentBlock == null)
            {
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            }

            _currentRow++;
            return _currentBlock.Rows > _currentRow;
        }

        public int Depth { get; } = 1;

        public bool IsClosed => _clickHouseConnection == null;

        public int RecordsAffected => _currentBlock.Rows;
    }
}