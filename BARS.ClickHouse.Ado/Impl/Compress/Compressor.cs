namespace BARS.ClickHouse.Ado.Impl.Compress
{
    using System.IO;
    using Data;

    internal abstract class Compressor
    {
        public abstract CompressionMethod Method { get; }
        
        public abstract Stream BeginCompression(Stream baseStream);
        
        public abstract void EndCompression();
        
        public abstract Stream BeginDecompression(Stream baseStream);
        
        public abstract void EndDecompression();

        public static Compressor Create(ClickHouseConnectionSettings settings)
        {
            return new Lz4Compressor(false, settings);
        }
    }
}