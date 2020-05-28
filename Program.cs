using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using Google.Apis.Util.Store;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Threading;

namespace SizeDB
{
    /// <summary>
    /// ClientSecret - файл с Sheets API
    /// ScopesSheets - права на использование нашего Google Drive
    /// AppName - наше приложение
    /// SpreadsheetId - Id таблицы на Google Drive
    /// server_name - имя нашего сервера базы данных PostgreSQL
    /// db_name - имя базы данных на сервере PostgreSQL
    /// </summary>
    class Program
    {
        private static readonly string ClientSecret = "client_secret.json";
        private static readonly string[] ScopesSheets = { SheetsService.Scope.Spreadsheets };
        private static readonly string AppName = "SizeDB";
        private static readonly string SpreadsheetId = "1h5Q0ufdVYustYIoHlWmRqQjmuJ5mILj8bN8HDTWuHKE";
        private static readonly string server_name = "localhost";
        private static readonly string db_name = "postgres";
        static void Main(string[] args)
        {
            //Создание credential
            Console.WriteLine("Get Credentials");
            var credential = GetSheetCredentials();

            //Создание service
            Console.WriteLine("Get service");
            var service = GetService(credential);

            //Узнаем размер базы данных PostgreSQL на диске
            string db_size = GetDBsize(db_name);
            Console.WriteLine($"Get size DataBase: {db_size} Gb");

            //Определяем свободное пространство на диске 
            var freeDiskSpace = GetFreeDiskSpace(db_size);
            Console.WriteLine($"Free Disk space: {freeDiskSpace} Gb");

            //Проверка наличия листа с именем сервера,
            //если такого нет, создание листа
            //+ создание шапки таблицы
            CheckSheet(service);

            //Заполняем таблицу на Google Drive
            Console.WriteLine("Fill data to sheet");
            FillSheet(service, SpreadsheetId, db_name, db_size);

            //Выводим информацию о свободном месте на диске
            FillFreeDiskSize(service, SpreadsheetId, freeDiskSpace);

            Console.WriteLine("Done.");
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey(); //Delay
        }

        private static UserCredential GetSheetCredentials()
        {
            using (var stream = new FileStream(ClientSecret, FileMode.Open, FileAccess.Read))
            {
                var credPath = Path.Combine(Directory.GetCurrentDirectory(), "sheetsCreds.json");
                return GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    ScopesSheets,
                    "user",
                    CancellationToken.None,
                    new FileDataStore(credPath, true)).Result;
            }
        }

        private static SheetsService GetService(UserCredential credential)
        {
            return new SheetsService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
                ApplicationName = AppName
            });
        }

        private static string GetDBsize(string db_name)
        {
            //Подключение к БД PostgreSQL
            var connectionString = ConfigurationManager.ConnectionStrings["ConnectToDB"].ConnectionString;
            //Создаем соединение с Npgsql provider
            NpgsqlConnection conn = new NpgsqlConnection(connectionString);
            //Открываем соединение к БД
            conn.Open();
            //запрос к базе данных, чтобы узнать размер:
            //string sql = "SELECT pg_size_pretty( pg_database_size( '" + db_name + "' ))";
            string sql = "SELECT pg_database_size( '" + db_name + "' )";
            var da = new NpgsqlCommand(sql, conn);

            double result = double.Parse(da.ExecuteScalar().ToString());
            double db_size = Math.Round((result / 1024 / 1024 / 1024), 2);
            //Закрываем соединение
            conn.Close();

            return db_size.ToString();
        }

        private static string GetFreeDiskSpace(string db_size)
        {
            //Подключение к строке о размере диска
            var connectionString_ds = ConfigurationManager.ConnectionStrings["DiskSpace"].ConnectionString;

            double DiskSpace = double.Parse(connectionString_ds);//Размер диска
            double DBsize = double.Parse(db_size);//Размер БД

            double freeDiskSpace = DiskSpace - DBsize; //Свободное место на диске в Гб.
            return freeDiskSpace.ToString();
        }

        private static void CheckSheet(SheetsService service)
        {
            var ssRequest = service.Spreadsheets.Get(SpreadsheetId);
            Spreadsheet ss = ssRequest.Execute();
            List<string> sheetList = new List<string>();
            //Заполняем массив названиями листов
            foreach (Sheet sheet in ss.Sheets)
            {
                sheetList.Add(sheet.Properties.Title);
            }

            string sn = null;
            //Проверяем наличие уже существующего листа
            foreach (string item in sheetList)
            {
                if (item.Contains(server_name))
                {
                    sn = item;
                }
            }

            if (sn == null) //Создаем новый лист
            {
                Console.WriteLine("Create sheet");
                CreateSheet(service, SpreadsheetId);
            }
            else //Будем заполнять уже существующий лист данными
            {
                Console.WriteLine("Go on");
                Thread.Sleep(3000); //Ожидание заданного промежутка времени (3 сек.)

                //Узнаем номер последней строки в листе
                var lastRow = GetLastRow(service);

                //Удаляем последнюю строку с данными о свободном месте на диске(если имеется)
                DeleteLastRow(service, lastRow);
            }
        }

        private static void CreateSheet(SheetsService service, string spreadsheetId)
        {
            string sheet_name = server_name;

            var addSheetRequest = new AddSheetRequest();
            addSheetRequest.Properties = new SheetProperties();

            addSheetRequest.Properties.Title = sheet_name;
            BatchUpdateSpreadsheetRequest batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest();
            batchUpdateSpreadsheetRequest.Requests = new List<Request>();
            batchUpdateSpreadsheetRequest.Requests.Add(new Request
            {
                AddSheet = addSheetRequest
            });

            var batchUpdateRequest =
                service.Spreadsheets.BatchUpdate(batchUpdateSpreadsheetRequest, spreadsheetId);

            batchUpdateRequest.Execute();

            //Добавление шапки таблицы
            var range = $"{sheet_name}!A:D";
            var valueRange = new ValueRange();

            var oblist = new List<object>() { "Сервер", "База данных", "Размер в ГБ", "Дата обновления" };
            valueRange.Values = new List<IList<object>> { oblist };

            var appendRequest = service.Spreadsheets.Values.Append(valueRange, spreadsheetId, range);
            appendRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.USERENTERED;
            appendRequest.Execute();
        }

        private static void FillSheet(SheetsService service, string spreadsheetId, string db_name, string db_size)
        {
            DateTime update_date = DateTime.Now; //дата обновления

            var range = $"{server_name}!A:D"; //диапазон ячеек для заполнения
            var valueRange = new ValueRange();

            var oblist = new List<object>() { server_name, db_name, db_size, update_date.ToShortDateString() };
            valueRange.Values = new List<IList<object>> { oblist };

            var appendRequest = service.Spreadsheets.Values.Append(valueRange, spreadsheetId, range);
            appendRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.USERENTERED;
            appendRequest.Execute();

        }

        private static void FillFreeDiskSize(SheetsService service, string spreadsheetId, string freeDiskSpace)
        {
            DateTime update_date = DateTime.Now; //дата обновления

            var range = $"{server_name}!A:D";
            var valueRange = new ValueRange();

            var oblist = new List<object>() { server_name, "Свободно", freeDiskSpace, update_date.ToShortDateString() };
            valueRange.Values = new List<IList<object>> { oblist };

            var appendRequest = service.Spreadsheets.Values.Append(valueRange, spreadsheetId, range);
            appendRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.USERENTERED;
            appendRequest.Execute();
        }

        private static int GetLastRow(SheetsService service)
        {
            //Узнаем номер последней заполненной строки
            int lastRow = 0;
            var range = $"{server_name}!A:D";
            SpreadsheetsResource.ValuesResource.GetRequest request =
                service.Spreadsheets.Values.Get(SpreadsheetId, range);

            var response = request.Execute();
            IList<IList<object>> values = response.Values;
            if (values != null && values.Count > 0)
            {
                foreach (var row in values)
                {
                    lastRow++;
                }
            }
            else
            {
                Console.WriteLine("No data found.");
            }
            return lastRow;
        }

        private static void DeleteLastRow(SheetsService service, int lastRow)
        {
            //Удаляем строку со старыми данными о размере свободного места на диске.
            var range = $"{server_name}!A{lastRow}:D";
            var requestBody = new ClearValuesRequest();

            var deleteRequest = service.Spreadsheets.Values.Clear(requestBody, SpreadsheetId, range);
            var deleteResponse = deleteRequest.Execute();
        }
    }
}
