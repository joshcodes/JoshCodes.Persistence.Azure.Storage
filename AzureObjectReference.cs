using System.Runtime.Serialization;

namespace JoshCodes.Persistence.Azure.Storage
{
    [DataContract(Name = "r")]
    public class AzureObjectReference
    {
        public AzureObjectReference(string rowKey, string partitionKey, string tableName)
        {
            RowKey = rowKey;
            PartitionKey = partitionKey;
            TableName = tableName;
        }

        [DataMember(Name = "k")]
        public string RowKey { get; set; }

        [DataMember(Name = "p")]
        public string PartitionKey { get; set; }

        [DataMember(Name = "t")]
        public string TableName { get; set; }

        public static System.Collections.Generic.IEqualityComparer<AzureObjectReference> GetComparer()
        {
            return Core.Equality<AzureObjectReference>.CreateComparer(azureObjectReference => azureObjectReference.RowKey);
        }
    }
}
