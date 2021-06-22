using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Host;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using Newtonsoft.Json.Linq;
using System.Text;

namespace HelsingborgsStad.AzureTriggerYggio
{
    public static class AzureTriggerYggio
    {
        private static ILogger logger;
        private static string DA_STORAGE_SAS_URL => Environment.GetEnvironmentVariable("DA_STORAGE_SAS_URL");

        [FunctionName("AzureTriggerYggio")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log)
        {
            logger=log;
            log.LogInformation("AzureTriggerYggio was started.");

            string name = req.Query["name"];

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            JObject deviceData = JObject.Parse(requestBody);
            await SendDataToDataLake(deviceData);
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;

            /*string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";*/

            log.LogInformation("AzureTriggerYggio finished.");
            return new OkObjectResult("");
        }
        
        static DataLakeFileSystemClient GetDataLakeFileSystemClient()
        {
            var dfsUri = new Uri(DA_STORAGE_SAS_URL);
            var client = new DataLakeFileSystemClient(dfsUri);
            return client;
        }

        static async Task SendDataToDataLake(JObject deviceData, bool error = false)
        {
            var dataLakeFileSystemClient = GetDataLakeFileSystemClient();
            
            var dataLakeDirectoryClient = await CreateDirectory(dataLakeFileSystemClient, deviceData["payload"]["iotnode"]["name"].ToString(), deviceData["payload"]["iotnode"]["reportedAt"].ToString(), error);

            await UploadFile(dataLakeDirectoryClient, deviceData);
        }

        static async Task<DataLakeDirectoryClient> CreateDirectory(DataLakeFileSystemClient fileSystemClient, string devEUI, string deviceTime, bool error)
        {
            DateTime payloadTime = ConvertDevicePayloadTime(deviceTime);
            string year = payloadTime.ToString("yyyy");
            string month = payloadTime.ToString("MM");
            string day = payloadTime.ToString("dd");
            string deliveryFolderName = $"yggio/{year}/{month}/{day}/{devEUI}";

            if (error)
            {
                deliveryFolderName = $"yggio/{year}/{month}/{day}/{devEUI}/error";
            }
            
            DataLakeDirectoryClient directoryClient =
                await fileSystemClient.CreateDirectoryAsync(deliveryFolderName);

            return directoryClient;
        }

        static async Task UploadFile(DataLakeDirectoryClient directoryClient, JObject deviceData)
        {
            DateTime payloadTime = ConvertDevicePayloadTime(deviceData["payload"]["iotnode"]["reportedAt"].ToString());
            string filename = $"{payloadTime.ToString("yyyy-MM-dd HH:mm:ss")}.json";
            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(filename);
            string data = deviceData.ToString();
            long fileSize = data.Length;

            Stream file = await fileClient.OpenWriteAsync(false);
            byte[] bytes = Encoding.ASCII.GetBytes(data);
            await file.WriteAsync(bytes, 0, bytes.Length);
            file.Close();
        }

        static DateTime ConvertDevicePayloadTime(string time)
        {
            if (DateTime.TryParse(time, out var deviceTime))
                return deviceTime;
            else
            {
                //If an error occurs while converting device time return the local time now and log warning
                logger.LogWarning($"An error occured while converting device payload time. {time}");
                return DateTime.Now;
            }
        }
    }
}
