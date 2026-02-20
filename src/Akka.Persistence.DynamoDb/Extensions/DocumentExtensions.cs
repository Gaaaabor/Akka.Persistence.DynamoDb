using Amazon.DynamoDBv2.DocumentModel;

namespace Akka.Persistence.DynamoDb.Extensions
{
    public static class DocumentExtensions
    {
        public static bool TryGetLong(this Document document, string attributeName, out long longValue)
        {
            if (document.TryGetValue(attributeName, out var entry) && long.TryParse(entry, out longValue))
            {
                return true;
            }

            longValue = 0L;
            return false;
        }
    }
}