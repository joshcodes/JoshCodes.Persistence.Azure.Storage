using System;

using Microsoft.WindowsAzure.StorageClient;

namespace JoshCodes.Persistence.Azure.Storage
{
    public class Entity : TableServiceEntity
    {
        public string IdGuid { get; set; }

        public string IdUrn { get; set; }

        public DateTime UpdatedAt { get; set; }

        public DateTime CreatedAt { get; set; }
    }
}
