namespace Akka.Persistence.DynamoDb.Journal
{
    public static class Events
    {
        public sealed class Initialized
        {
            public static readonly Initialized Instance = new();

            private Initialized()
            {
            }
        }
    }
}