using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace PushBlobsToEventhub
{
    class Program
    {
        private static string ImportContainer = "";
        private const string EndpointUrl = "";
        private const string PrimaryKey = "";

        static void Main(string[] args)
        {
            // Parse the connection string and return a reference to the storage account.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                ConfigurationManager.ConnectionStrings["AzureStorage"].ConnectionString);

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(ImportContainer);

            var eventHubClient = EventHubClient.CreateFromConnectionString(ConfigurationManager.ConnectionStrings["EventHub"].ConnectionString);

            int i = 0;
            var token = container.ListBlobsSegmented(null);
            while (token.ContinuationToken != null)
            {
                var results = token.Results.ToList();
                var count = results.Count;
                Console.WriteLine("Processing " + count + " results...");
                foreach (var blob in results)
                {
                    WebClient webClient = new WebClient();
                    var blobContent = webClient.DownloadString(blob.Uri);
                    var blobObject = JsonConvert.DeserializeObject(blobContent);

                    using (var client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey))
                    {
                        Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'db1'").AsEnumerable().First();
                        DocumentCollection collection = client.CreateDocumentCollectionQuery(database.CollectionsLink,
                            "SELECT * FROM c WHERE c.id = 'news'").AsEnumerable().First();

                        var docTask = client.CreateDocumentAsync(collection.AltLink, blobObject);
                        Task.WaitAll(docTask);
                        var doc = docTask.Result;
                        var json = doc.Resource.ToString();
                        Console.WriteLine(json);
                    }


                    //var message = new
                    //{
                    //    BlobUrl = blob.Uri
                    //};
                    //var messageJson = JsonConvert.SerializeObject(message, Formatting.Indented);
                    //eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(messageJson)));
                    //return;
                    i++;
                    if (i > 100)
                        return;
                }
                token = container.ListBlobsSegmented(token.ContinuationToken);
            }
        }
    }
}
