using System;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;
using System.Collections.Generic;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Web.Models.Domain;

namespace JoshCodes.Persistence.Azure.Storage
{
    public class Entity : TableServiceEntity
    {
        private const string GuidFormat = "N";

        public Entity()
        {
        }

        public Entity(Guid key, DateTime lastModified)
        {
            if (key == default(Guid) || key == Guid.Empty) // Likely the same thing
            {
                throw new ArgumentException("key value must be set", "key");
            }
            if (lastModified == default(DateTime))
            {
                throw new ArgumentException("Last Modified value must be set", "lastModified");
            }

            this.RowKey = Entity.BuildRowKey(key);
            this.PartitionKey = Entity.BuildPartitionKey(this.RowKey);
            this.LastModified = lastModified;
        }

        public Entity(string key, DateTime lastModified)
        {
            if (String.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("key value must be set", "key");
            }
            if (lastModified == default(DateTime))
            {
                throw new ArgumentException("Last Modified value must be set", "lastModified");
            }

            this.RowKey = key;
            this.PartitionKey = Entity.BuildPartitionKey(this.RowKey);
            this.LastModified = lastModified;
        }

        public Guid GetKey()
        {
            //TODO: return Guid.ParseExact(this.RowKey, GuidFormat);
            return Guid.Parse(this.RowKey);
        }

        public DateTime LastModified { get; set; }

        internal static string BuildRowKey(Guid rowKey)
        {
            return rowKey.ToString(GuidFormat);
        }

        internal static string BuildPartitionKey(string rowKey)
        {
            return (rowKey.GetHashCode() % 13).ToString();
        }

        #region Encoding / Decoding

        public static T Decode<T>(string value)
        {
            if (String.IsNullOrWhiteSpace(value))
            {
                return default(T);
            }
            var reader = XmlReader.Create(new StringReader(value));
            var serializer = new DataContractSerializer(typeof(T));
            try
            {
                T result = (T)serializer.ReadObject(reader);
                return result;
            }
            catch (SerializationException)
            {
                return default(T);
            }
        }

        public static string Encode<T>(T value)
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
    }
}
