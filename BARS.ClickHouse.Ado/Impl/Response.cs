namespace BARS.ClickHouse.Ado.Impl
{
    using System.Collections.Generic;
    using Data;

    internal class Response
    {
        public List<Block> Blocks { get; } = new List<Block>();

        public void AddBlock(Block block)
        {
            Blocks.Add(block);
        }

        public void OnProgress(long rows, long total, long bytes)
        {
        }

        public void OnEnd()
        {
        }
    }
}