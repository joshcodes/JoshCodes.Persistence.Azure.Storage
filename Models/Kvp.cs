using System;

using JoshCodes.Web.Models.Persistence;

using Microsoft.WindowsAzure.StorageClient;

namespace JoshCodes.Persistence.Azure.Storage
{
    public class KvpStore : AzureObjectStore<IDefineKvp, Kvp, KvpEntity>, IStoreKvp
    {
        public KvpStore(CloudTableClient tableClient)
            : base(tableClient, Kvp.EntityTableName)
        {
        }

        public string Get(string container, string key)
        {
            var kvp = base.Find(container, key);
            if (kvp == null)
            {
                return null;
            }
            return kvp.Value;
        }

        public void Create(string container, string key, string value)
        {
            var kvp = new KvpEntity()
            {
                RowKey = key,
                PartitionKey = container,
                Value = value,
                LastModified = DateTime.UtcNow,
            };
            base.Create(kvp);
        }

        protected override Kvp CreateObjectStore(KvpEntity entity)
        {
            return new Kvp(entity, _tableClient);
        }
    }

    public class Kvp : AzureObjectWrapper<KvpEntity>, IDefineKvp
    {
        internal const string EntityTableName = "KvpTable";

        public Kvp(Guid id, CloudTableClient tableClient)
            : base(id, tableClient, EntityTableName)
        {
        }

        public Kvp(KvpEntity entity, CloudTableClient tableClient)
            : base(entity, tableClient, EntityTableName)
        {
        }

        public string Container
        {
            get { return Storage.PartitionKey; }
        }

        public string LookupKey
        {
            get { return Storage.RowKey; }
        }

        public string Value
        {
            get { return Storage.Value; }
        }
    }

    public class KvpEntity : Entity
    {
        public string Value { get; set; }
    }
}
