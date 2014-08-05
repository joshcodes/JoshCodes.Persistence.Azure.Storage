using System;

using Microsoft.WindowsAzure;

namespace JoshCodes.Persistence.Azure.Storage
{
    public static class Settings
    {
        private const string connectionStringName = "StorageConnectionString";

        private static string GetStorageSetting()
        {
            return GetStorageSetting("azure.cloud_storage-storage_setting");
        }

        private static string GetStorageSetting(string appSettingsKey)
        {
            if(String.IsNullOrWhiteSpace(appSettingsKey))
            {
                return GetStorageSetting();
            }
            var storageSetting = System.Configuration.ConfigurationManager.AppSettings[appSettingsKey];
            return storageSetting;
        }

        public static Microsoft.WindowsAzure.CloudStorageAccount StorageAccount()
        {
            return StorageAccount(GetStorageSetting());
        }

        public static Microsoft.WindowsAzure.CloudStorageAccount StorageAccount(string appSettingsKey)
        {
            var storageSetting = GetStorageSetting(appSettingsKey);
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
