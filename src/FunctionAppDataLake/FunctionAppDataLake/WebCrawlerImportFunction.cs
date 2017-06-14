using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;


namespace FunctionAppDataLake
{
    public static class WebCrawlerImportFunction
    {
        private const string EndpointUrl = "";
        private const string PrimaryKey = "";

        [FunctionName("WebCrawlerImportFunction")]
        public static async void Run([EventHubTrigger("blobimport", Connection = "blobimporteventhub")]string myEventHubMessage, TraceWriter log)
        {
            log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");

            dynamic message = JsonConvert.DeserializeObject(myEventHubMessage);
            WebClient webClient = new WebClient();
            var blobContent = webClient.DownloadString((string)message.BlobUrl);
            var blobObject = JsonConvert.DeserializeObject(blobContent);

            using (var client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey))
            {
                Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'db1'").AsEnumerable().First();
                DocumentCollection collection = client.CreateDocumentCollectionQuery(database.CollectionsLink,
                    "SELECT * FROM c WHERE c.id = 'news'").AsEnumerable().First();

                var doc = await client.CreateDocumentAsync(collection.AltLink, blobObject);
                var json = doc.Resource.ToString();
                log.Info(json);
            }
        }
    }
}