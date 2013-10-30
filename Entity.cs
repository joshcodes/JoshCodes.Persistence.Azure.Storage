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
        public Entity()
        {
        }

        public Entity(DomainId id)
        {
            this.IdKey = id.Key;
            this.IdGuid = id.Guid.ToString();
            this.IdUrn = id.Urn != null ?
                id.Urn.AbsoluteUri : null;
        }

        public string IdGuid { get; set; }

        public string IdKey { get; set; }

        public string IdUrn { get; set; }

        public DateTime UpdatedAt { get; set; }

        public DateTime CreatedAt { get; set; }

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
