using System;

using Microsoft.WindowsAzure;

namespace JoshCodes.Persistence.Azure.Storage
{
    public static class Settings
    {
        private const string connectionStringName = "StorageConnectionString";

        private static string GetStorageSetting()
        {
            var storageSetting = System.Configuration.ConfigurationManager.AppSettings["azure.cloud_storage-storage_setting"];
            return storageSetting;
        }

        public static Microsoft.WindowsAzure.CloudStorageAccount StorageAccount()
        {
            var storageSetting = GetStorageSetting();
            var storageAccount = Microsoft.WindowsAzure.CloudStorageAccount.Parse(storageSetting);
            return storageAccount;
        }

        public static Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient BlobClient()
        {
            var storageSetting = GetStorageSetting();
            var storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(storageSetting);
            return storageAccount.CreateCloudBlobClient();
        }
    }
}
