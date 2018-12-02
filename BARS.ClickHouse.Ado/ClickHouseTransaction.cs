namespace BARS.ClickHouse.Ado
{
    using System.Data;

    /// <summary>
    ///     ClickHouse doesn't support transactions
    ///     But sometimes u need fake object like this
    /// </summary>
    public class ClickHouseTransaction : IDbTransaction
    {
        public ClickHouseTransaction(IDbConnection connection)
        {
            Connection = connection;
        }

        public ClickHouseTransaction(IDbConnection connection, IsolationLevel level)
            : this(connection)
        {
            IsolationLevel = level;
        }

        public void Dispose()
        {
        }

        public void Commit()
        {
        }

        public void Rollback()
        {
        }

        public IDbConnection Connection { get; }

        public IsolationLevel IsolationLevel { get; }
    }
}