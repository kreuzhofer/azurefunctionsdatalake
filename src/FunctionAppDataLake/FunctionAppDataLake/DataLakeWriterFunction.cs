using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Threading;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.Management.DataLake.Store;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;

namespace FunctionAppDataLake
{
    public static class DataLakeWriterFunction
    {
        private static DataLakeStoreAccountManagementClient _adlsClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;
        private static string _subId = "";
        private static string _adlsAccountName = "";

        [FunctionName("DataLakeWriterFunction")]
        public static void Run([TimerTrigger("*/5 * * * * *")]TimerInfo myTimer, [EventHub("eventhub", Connection = "eventhubconnection")]out string eventHubMessage, TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            // login to azure ad
            // Service principal / appplication authentication with client secret / key
            // Use the client ID of an existing AAD "Web App" application.
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var domain = "";
            var webApp_clientId = "";
            var clientSecret = "";
            var clientCredential = new ClientCredential(webApp_clientId, clientSecret);
            var credsTask = ApplicationTokenProvider.LoginSilentAsync(domain, clientCredential);
            Task.WaitAll(credsTask);
            var creds = credsTask.Result;

            // Create client objects and set the subscription ID
            _adlsClient = new DataLakeStoreAccountManagementClient(creds) { SubscriptionId = _subId };
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);

            var directory = "testDir";
            CreateDirectory(directory);
            var randomGuid = Guid.NewGuid();
            var filePath = directory + "\\" + randomGuid + ".txt";
            CreateFile(filePath);

            // notify eventhub queue about the new file;
            var message = new
            {
                Timestamp = DateTime.UtcNow,
                Filename = filePath
            };
            eventHubMessage = JsonConvert.SerializeObject(message, Formatting.Indented);
        }

        // Create a directory
        public static void CreateDirectory(string path)
        {
            _adlsFileSystemClient.FileSystem.Mkdirs(_adlsAccountName, path);
        }

        public static void CreateFile(string name)
        {
            _adlsFileSystemClient.FileSystem.Create(_adlsAccountName, name);
        }
    }
}