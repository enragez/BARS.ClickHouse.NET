namespace BARS.ClickHouse.Client
{
    using System;
    using System.IO;

    internal class TsvWithHeaderOutputter : TsvOutputter
    {
        public TsvWithHeaderOutputter(Stream s) : base(s)
        {
        }

        public override void HeaderCell(string name)
        {
            Console.Write(name);
            Console.Write('\t');
        }

        public override void DataStart()
        {
            Console.WriteLine("\n--------------------------------------------------------");
        }
    }
}