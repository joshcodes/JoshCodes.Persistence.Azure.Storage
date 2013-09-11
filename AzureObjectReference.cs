using System;

using System.Runtime.Serialization;

namespace JoshCodes.Persistence.Azure.Storage
{
    [DataContract(Name = "r")]
    public class AzureObjectReference
    {
        public AzureObjectReference(string rowKey, string partitionKey, string tableName)
        {
            this.RowKey = rowKey;
            this.PartitionKey = partitionKey;
            this.TableName = tableName;
        }

        [DataMember(Name = "k")]
        public string RowKey { get; set; }

        [DataMember(Name = "p")]
        public string PartitionKey { get; set; }

        [DataMember(Name = "t")]
        public string TableName { get; set; }
    }
}
