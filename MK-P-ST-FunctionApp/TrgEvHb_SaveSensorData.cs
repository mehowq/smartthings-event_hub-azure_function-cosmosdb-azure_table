using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace MK_P_ST_FunctionApp
{

    public class SensorDataAzTbl : TableEntity
    {
        //inherits PartitionKey and RowKey which we need for Azure Table storage
        public string SensorId { get; set; }
        public DateTime ReadAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public string SensorName { get; set; }
        public string SensorType { get; set; }
        public string SensorValue { get; set; }

    }

    public class SensorData
    {
        public string SensorId { get; set; }
        public DateTime ReadAt { get; set; }
        public DateTime CreatedAt { get; set; }
        public string SensorName { get; set; }
        public string SensorType { get; set; }
        public string SensorValue { get; set; }
    }


    public static class TrgEvHb_SaveSensorData
    {
        private const string _APPSET_CONNSTRING_EVENTHUB = "SENSORDATA_CONNSTRING_EVENTHUB";
        private const string _APPSET_CONNSTRING_STORAGEACC = "SENSORDATA_CONNSTRING_STORAGEACCOUNT";
        private const string _APPSET_TABLE = "SENSORDATA_TABLE";
        private const string _APPSET_CONNSTRING_COSMOSDB = "SENSORDATA_CONNSTRING_COSMOSDB";
        private const string _APPSET_COSMOSDB_DATABASE = "%SENSORDATA_COSMOSDB_DATABASE%";
        private const string _APPSET_COSMOSDB_COLLECTION = "%SENSORDATA_COSMOSDB_COLLECTION%";


        // Please bare in mind that only one trigger/app/handler can listen to event hub with the basic plan
        [FunctionName("TrgEvHb_SaveSensorData")]
        public static void Run(
            [EventHubTrigger("", Connection = _APPSET_CONNSTRING_EVENTHUB)]
            string myEventHubMessage,
            [CosmosDB(
                databaseName: _APPSET_COSMOSDB_DATABASE,
                collectionName: _APPSET_COSMOSDB_COLLECTION,
                ConnectionStringSetting = _APPSET_CONNSTRING_COSMOSDB)]out dynamic document,
            TraceWriter log,
            ExecutionContext context)
        {
            IConfigurationRoot config = GetApplicationSettings(context);

            log.Info($"C# Event Hub trigger function started processing message {context.InvocationId}: {myEventHubMessage}");

            //Not using JObject.Parse as it converted datetime by loosing miliseconds 
            SensorData oSensorData = JsonConvert.DeserializeObject<SensorData>(myEventHubMessage);

            //CosmosDB
            document = BuildCosmosDbDocument(oSensorData);

            //Azure Table
            SaveDataToAzureTable(config, oSensorData);


            log.Info($"C# Event Hub trigger function processed message {context.InvocationId}");
        }

        private static dynamic BuildCosmosDbDocument(SensorData oSensorData)
        {
            DateTime creationDt = DateTime.Now;

            dynamic document = new
            {
                SensorId = oSensorData.SensorId,
                ReadAt = oSensorData.ReadAt,
                CreatedAt = creationDt,
                SensorName = oSensorData.SensorName,
                SensorType = oSensorData.SensorType,
                SensorValue = oSensorData.SensorValue
            };
            return document;
        }
        private static SensorDataAzTbl BuildAzureTableDocument(SensorData oSensorData)
        {
            DateTime creationDt = DateTime.Now;

            SensorDataAzTbl sensorData = new SensorDataAzTbl()
            {
                PartitionKey = oSensorData.SensorId,
                RowKey = creationDt.Ticks.ToString(),

                SensorId = oSensorData.SensorId,
                ReadAt = oSensorData.ReadAt,
                CreatedAt = creationDt,
                SensorName = oSensorData.SensorName,
                SensorType = oSensorData.SensorType,
                SensorValue = oSensorData.SensorValue
            };
            return sensorData;
        }

        private static async System.Threading.Tasks.Task SaveDataToAzureTable(IConfigurationRoot config, SensorData oSensorData)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(config[_APPSET_CONNSTRING_STORAGEACC]);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(config[_APPSET_TABLE]);
            await table.CreateIfNotExistsAsync();

            SensorDataAzTbl sensorData = BuildAzureTableDocument(oSensorData);

            TableOperation insertOperation = TableOperation.Insert(sensorData);
            await table.ExecuteAsync(insertOperation);
        }

        

        private static IConfigurationRoot GetApplicationSettings(ExecutionContext context)
        {
            //Get Application Settings from file for local testing purposes
            return new ConfigurationBuilder()
            .SetBasePath(context.FunctionAppDirectory)
            //You need to create an entry for each of the application settings in the Azure Portal
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
        }
    }

}
