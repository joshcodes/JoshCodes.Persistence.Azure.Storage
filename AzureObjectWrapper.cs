using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;
using System.Net;
using System.Data.Services.Client;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Persistence.Azure.Sql.Extensions;

namespace JoshCodes.Persistence.Azure.Sql
{
    public class AzureObjectWrapper<TEntity> where TEntity : TableServiceEntity
    {
        protected TEntity _storage;
        protected CloudTableClient _tableClient;
        protected string _rowKey;
        protected string _partitionKey;
        private string _entityTableName;

        #region Constructors

        public AzureObjectWrapper(Uri urn, CloudTableClient tableClient, string entityTableName)
        {
            _rowKey = urn.ParseRowKey(tableClient, out _partitionKey);
            _tableClient = tableClient;
            _entityTableName = entityTableName;
        }

        public AzureObjectWrapper(string key, string partitionKey, CloudTableClient tableClient, string entityTableName)
        {
            this._rowKey = key;
            this._partitionKey = partitionKey;
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;
        }

        public AzureObjectWrapper(TEntity storage, CloudTableClient tableClient, string entityTableName)
        {
            this._storage = storage;
            this._rowKey = storage.RowKey;
            this._partitionKey = storage.PartitionKey;
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;
        }

        public delegate TEntity CreateEntity(out string partitionKey, out string rowKey);

        public AzureObjectWrapper(CloudTableClient tableClient, string entityTableName, CreateEntity createEntity)
        {
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;

            tableClient.CreateTableIfNotExist(entityTableName);
            var tableServiceContext = tableClient.GetDataServiceContext();

            string rowKey, partitionKey;
            this._storage = createEntity.Invoke(out partitionKey, out rowKey);
            this._rowKey = rowKey;
            this._partitionKey = partitionKey;
            this._storage.PartitionKey = partitionKey;
            this._storage.RowKey = rowKey;

            tableServiceContext.AddObject(entityTableName, this._storage);
            tableServiceContext.SaveChanges();
        }

        #endregion

        protected TEntity storage
        {
            get
            {
                if (_storage == null)
                {
                    this.EditableStorage((s) => { _storage = s; return true; });
                }
                return _storage;
            }
        }

        #region Utility methods

        protected static string GetBaseUriString(CloudTableClient tableClient, string entityTableName)
        {
            return tableClient.BaseUri.AbsoluteUri + "/" + entityTableName + "/";
        }

        protected static string ParseRowKey(Uri id, CloudTableClient tableClient, string entityTableName)
        {
            var rowKey = id.AbsoluteUri.Substring(GetBaseUriString(tableClient, entityTableName).Length);
            return rowKey;
        }

        protected static T Decode<T>(string value)
        {
            if (String.IsNullOrWhiteSpace(value))
            {
                return default(T);
            }
            var reader = XmlReader.Create(new StringReader(value));
            var serializer = new DataContractSerializer(typeof(T));
            T result = (T)serializer.ReadObject(reader);
            return result;
        }

        protected static string Encode<T>(T value)
        {
            if (EqualityComparer<T>.Default.Equals(value))
            {
                return String.Empty;
            }
            string serializedString;
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(typeof(T));
                serializer.WriteObject(memoryStream, value);
                memoryStream.Position = 0;
                serializedString = reader.ReadToEnd();
            }
            return serializedString;
        }

        #endregion

        #region Properties

        public Uri IdUrn
        {
            get
            {
                return this.BuildUrn(_rowKey, _partitionKey, _tableClient);
            }
        }

        public Guid IdGuid
        {
            get
            {
                return Guid.Parse(_rowKey);
            }
        }

        public string Key
        {
            get { return _rowKey; }
        }

        public string Partition
        {
            get { return _partitionKey; }
        }

        #endregion

        protected bool AtomicModification<T>(T requiredValue, T newValue, out T currentValue, Expression<Func<TEntity, T>> propertySelector)
            where T : IComparable<T>
        {
            var tableServiceContext = _tableClient.GetDataServiceContext();
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var results = from entity in query
                          where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                          select entity;
            var storage = results.First();

            var success = storage.AtomicModification(requiredValue, newValue, out currentValue, tableServiceContext, propertySelector);
            if (success)
            {
                this._storage = storage;
            }
            return success;
        }

        protected void EditableStorage(Func<TEntity, bool> callback)
        {
            this.EditableStorage((entity, isRetry) =>
                {
                    return callback.Invoke(entity);
                });
        }

        protected void EditableStorage(Func<TEntity, bool, bool> callback)
        {
            bool isPreconditionFailedResponse = false;
            var tableServiceContext = _tableClient.GetDataServiceContext();
            do
            {
                var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
                var results = from entity in query
                              where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                              select entity;
                var storage = results.First();

                try
                {
                    if (callback.Invoke(storage, isPreconditionFailedResponse))
                    {
                        tableServiceContext.UpdateObject(storage);
                        tableServiceContext.SaveChanges();
                    }
                    return;
                }
                catch (Exception ex)
                {
                    isPreconditionFailedResponse = ex.IsProblemPreconditionFailed();
                    if (!isPreconditionFailedResponse)
                    {
                        throw;
                    }
                }
            } while (!isPreconditionFailedResponse);
        }

        public void Delete()
        {
            var tableServiceContext = _tableClient.GetDataServiceContext();
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var results = from entity in query
                          where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                          select entity;
            
            var storage = results.First();
            tableServiceContext.DeleteObject(storage);
            tableServiceContext.SaveChanges();
        }

    }
}
