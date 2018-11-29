namespace ClickHouse.Ado
{
    using System;
    using System.Data;
    using System.IO;
    using System.Net.Sockets;
    using Impl;
    using Impl.Data;

    public class ClickHouseConnection : IDbConnection
    {
        private NetworkStream _netStream;
        private Stream _stream;

        private TcpClient _tcpClient;

        public ClickHouseConnection()
        {
        }

        public ClickHouseConnection(ClickHouseConnectionSettings settings)
        {
            ConnectionSettings = settings;
        }

        public ClickHouseConnection(string connectionString)
        {
            ConnectionSettings = new ClickHouseConnectionSettings(connectionString);
        }

        public ClickHouseConnectionSettings ConnectionSettings { get; private set; }

        internal ProtocolFormatter Formatter { get; set; }

        public void Dispose()
        {
            if (_tcpClient != null)
            {
                Close();
            }
        }

        public void Close()
        {
            if (_stream != null)
            {
                _stream.Close();
                _stream.Dispose();
                _stream = null;
            }

            if (_netStream != null)
            {
                _netStream.Close();
                _netStream.Dispose();
                _netStream = null;
            }

            if (_tcpClient != null)
            {
                _tcpClient.Close();
                _tcpClient = null;
            }

            if (Formatter == null)
            {
                return;
            }

            Formatter.Close();
            Formatter = null;
        }

        public void Open()
        {
            if (_tcpClient != null)
            {
                throw new InvalidOperationException("Connection already open.");
            }

            _tcpClient = new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.Connect(ConnectionSettings.Host, ConnectionSettings.Port);
            _netStream = new NetworkStream(_tcpClient.Client);
            _stream = new UnclosableStream(_netStream);
            var ci = new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(_stream, ci,
                                              () => _tcpClient.Client.Poll(ConnectionSettings.SocketTimeout,
                                                                           SelectMode.SelectRead));
            Formatter.Handshake(ConnectionSettings);
        }

        public string ConnectionString
        {
            get => ConnectionSettings.ToString();
            set => ConnectionSettings = new ClickHouseConnectionSettings(value);
        }

        public int ConnectionTimeout { get; set; }
        public string Database { get; private set; }

        public void ChangeDatabase(string databaseName)
        {
            CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
            Database = databaseName;
        }

        public ConnectionState State => Formatter != null ? ConnectionState.Open : ConnectionState.Closed;

        public IDbTransaction BeginTransaction()
        {
            throw new NotSupportedException();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotSupportedException();
        }

        IDbCommand IDbConnection.CreateCommand()
        {
            return new ClickHouseCommand(this);
        }

        public ClickHouseCommand CreateCommand()
        {
            return new ClickHouseCommand(this);
        }

        public ClickHouseCommand CreateCommand(string text)
        {
            return new ClickHouseCommand(this, text);
        }
    }
}