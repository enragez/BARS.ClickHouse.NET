namespace BARS.ClickHouse.Ado
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    public class ClickHouseConnectionSettings
    {
        private static readonly Dictionary<string, PropertyInfo> Properties;

        static ClickHouseConnectionSettings()
        {
            Properties = typeof(ClickHouseConnectionSettings).GetProperties().ToDictionary(x => x.Name, x => x);
        }

        public ClickHouseConnectionSettings(string connectionString)
        {
            var varName = new StringBuilder();
            var varValue = new StringBuilder();

            char? valueEscape = null;
            var inEscape = false;
            var inValue = false;
            foreach (var c in connectionString)
            {
                if (inEscape)
                {
                    if (inValue)
                    {
                        varValue.Append(c);
                    }
                    else
                    {
                        varName.Append(c);
                    }

                    inEscape = false;
                }
                else if (valueEscape.HasValue)
                {
                    if (valueEscape.Value == c)
                    {
                        valueEscape = null;
                    }
                    else
                    {
                        if (inValue)
                        {
                            varValue.Append(c);
                        }
                        else
                        {
                            varName.Append(c);
                        }
                    }
                }
                else
                {
                    switch (c)
                    {
                        case '\\':
                            inEscape = true;
                            break;
                        case '"':
                        case '\'':
                            valueEscape = c;
                            break;
                        default:
                        {
                            if (char.IsWhiteSpace(c))
                            {
                            }
                            else
                            {
                                switch (c)
                                {
                                    case '=' when inValue:
                                        throw new
                                            FormatException($"Value for parameter {varName} in the connection string contains unescaped '='.");
                                    case '=':
                                        inValue = true;
                                        break;
                                    case ';' when !inValue:
                                        throw new
                                            FormatException($"No value for parameter {varName} in the connection string.");
                                    case ';':
                                        SetValue(varName.ToString(), varValue.ToString());
                                        inValue = false;
                                        varName.Clear();
                                        varValue.Clear();
                                        break;
                                    default:
                                    {
                                        if (inValue)
                                        {
                                            varValue.Append(c);
                                        }
                                        else
                                        {
                                            varName.Append(c);
                                        }

                                        break;
                                    }
                                }
                            }

                            break;
                        }
                    }
                }
            }

            if (inValue)
            {
                SetValue(varName.ToString(), varValue.ToString());
            }
        }

        public bool Async { get; private set; }

        public int BufferSize { get; private set; } = 4096;

        public int ApacheBufferSize { get; private set; }

        public int SocketTimeout { get; private set; } = 1000;

        public int ConnectionTimeout { get; private set; } = 1000;
 
        public int DataTransferTimeout { get; private set; } = 1000;

        public int KeepAliveTimeout { get; private set; } = 1000;

        public int TimeToLiveMillis { get; private set; }

        public int DefaultMaxPerRoute { get; private set; }

        public int MaxTotal { get; private set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        //additional
        public int MaxCompressBufferSize { get; private set; }

        // queries settings
        public int MaxParallelReplicas { get; private set; }

        public string TotalsMode { get; private set; }

        public string QuotaKey { get; private set; }

        public int Priority { get; private set; }

        public string Database { get; private set; }

        public bool Compress { get; private set; }

        public string Compressor { get; private set; }

        public bool CheckCompressedHash { get; private set; } = true;

        public bool Decompress { get; private set; }

        public bool Extremes { get; private set; }

        public int MaxThreads { get; private set; }

        public int MaxExecutionTime { get; private set; }

        public int MaxBlockSize { get; private set; }

        public int MaxRowsToGroupBy { get; private set; }

        public string Profile { get; private set; }

        public string User { get; private set; }

        public string Password { get; private set; }

        public bool DistributedAggregationMemoryEfficient { get; private set; }

        public int MaxBytesBeforeExternalGroupBy { get; private set; }

        public int MaxBytesBeforeExternalSort { get; private set; }

        private void SetValue(string name, string value)
        {
            Properties[name].GetSetMethod(true)
                            .Invoke(this, new[] {Convert.ChangeType(value, Properties[name].PropertyType)});
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            foreach (var prop in Properties)
            {
                var value = prop.Value.GetValue(this, null);
                if (value == null)
                {
                    continue;
                }

                builder.Append(prop.Key);
                builder.Append("=\"");
                builder.Append(value.ToString().Replace("\\", "\\\\").Replace("\"", "\\\""));
                builder.Append("\";");
            }

            return builder.ToString();
        }
    }
}