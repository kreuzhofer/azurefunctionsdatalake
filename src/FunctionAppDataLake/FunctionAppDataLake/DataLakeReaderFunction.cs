using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;


namespace FunctionAppDataLake
{
    public static class DataLakeReaderFunction
    {
        [FunctionName("DataLakeReaderFunction")]
        public static void Run([EventHubTrigger("eventhub", Connection = "eventhubconnection")]string myEventHubMessage, TraceWriter log)
        {
            log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");
        }
    }
}