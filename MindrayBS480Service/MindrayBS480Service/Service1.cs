using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;

namespace MindrayBS480Service
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        #region settings
        public static string IPadress = "172.18.95.31"; // cgm-app12, подсетка приборов
        public static int port = 8018;                  // порт

        public static string AnalyzerCode = "905";                  // код из аналайзер конфигурейшн, который связывает прибор в PSMV2
        public static string AnalyzerConfigurationCode = "MINDRAY"; // код прибора из аналайзер конфигурейшн

        public static string user = "PSMExchangeUser"; // логин для базы обмена файлами и для базы CGM Analytix
        public static string password = "PSM_123456";  // пароль для базы обмена файлами и для базы CGM Analytix   

        public static bool ServiceIsActive;            // флаг для запуска и остановки потока
        public static string AnalyzerResultPath = AppDomain.CurrentDomain.BaseDirectory + "\\AnalyzerResults"; // папка для файлов с результатами

        static object ExchangeLogLocker = new object();    // локер для логов обмена
        static object FileResultLogLocker = new object();  // локер для логов обмена
        static object ServiceLogLocker = new object();     // локер для логов драйвера

        // управляющие биты
        static byte[] VT = { 0x0B }; // <SB>
        static byte[] FS = { 0x1C }; // <EB>
        static byte[] CR = { 0x0D }; // <CR>

        #endregion

        #region Функции логов

        // лог обмена с анализатором
        static void ExchangeLog(string Message)
        {
            lock (ExchangeLogLocker)
            {
                string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Exchange";
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                string filename = path + "\\ExchangeThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                if (!System.IO.File.Exists(filename))
                {
                    using (StreamWriter sw = System.IO.File.CreateText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }
                else
                {
                    using (StreamWriter sw = System.IO.File.AppendText(filename))
                    {
                        sw.WriteLine(DateTime.Now + ": " + Message);
                    }
                }

            }
        }

        // Лог записи результатов в CGM
        static void FileResultLog(string Message)
        {
            try
            {
                lock (FileResultLogLocker)
                {
                    //string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult" + "\\" + DateTime.Now.Year + "\\" + DateTime.Now.Month;
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\FileResult";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    //string filename = path + $"\\{FileName}" + ".txt";
                    string filename = path + $"\\ResultLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";

                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
            }
            catch
            {

            }
        }

        // Лог драйвера
        static void ServiceLog(string Message)
        {
            lock (ServiceLogLocker)
            {
                try
                {
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\Log\\Service";
                    if (!Directory.Exists(path))
                    {
                        Directory.CreateDirectory(path);
                    }

                    string filename = path + "\\ServiceThread_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
                    if (!System.IO.File.Exists(filename))
                    {
                        using (StreamWriter sw = System.IO.File.CreateText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = System.IO.File.AppendText(filename))
                        {
                            sw.WriteLine(DateTime.Now + ": " + Message);
                        }
                    }
                }
                catch
                {

                }
            }
        }

        #endregion

        #region  Вспомогательные функции

        //дописываем к номеру месяца ноль если нужно
        public static string CheckZero(int CheckPar)
        {
            string BackPar = "";
            if (CheckPar < 10)
            {
                BackPar = $"0{CheckPar}";
            }
            else
            {
                BackPar = $"{CheckPar}";
            }
            return BackPar;
        }

        //собираем несколько массивов в один
        static Byte[] ConcatByteArray(params Byte[][] ArraysPar)
        {
            Byte[] FinallArray = { };
            for (int i = 0; i < ArraysPar.Length; i++)
            {
                int EndOfGeneralArray = FinallArray.Length;
                Array.Resize(ref FinallArray, FinallArray.Length + ArraysPar[i].Length);
                Array.Copy(ArraysPar[i], 0, FinallArray, EndOfGeneralArray, ArraysPar[i].Length);
            }
            return FinallArray;
        }

        // Создаем файл с результатом, отправленным анализатором
        static void MakeAnalyzerResultFile(string AllMessagePar)
        {
            if (!Directory.Exists(AnalyzerResultPath))
            {
                Directory.CreateDirectory(AnalyzerResultPath);
            }
            DateTime now = DateTime.Now;
            string filename = AnalyzerResultPath + "\\Results_" + now.Year + CheckZero(now.Month) + CheckZero(now.Day) + CheckZero(now.Hour) + CheckZero(now.Minute) + CheckZero(now.Second) + CheckZero(now.Millisecond) + ".res";
            using (System.IO.FileStream fs = new System.IO.FileStream(filename, FileMode.OpenOrCreate))
            {
                foreach (string res in AllMessagePar.Split('\r'))
                {
                    byte[] ResByte = Encoding.GetEncoding(1251).GetBytes(res + "\r\n");
                    fs.Write(ResByte, 0, ResByte.Length);
                }
            }
        }

        // преобразование кода теста в код теста, понятный прибору, для отправки задания
        public static string TranslateToAnalyzerCodes(string CGMTestCodesPar)
        {
            string BackTestCode = "";
            try
            {
                string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
                CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");
                using (SqlConnection Connection = new SqlConnection(CGMConnectionString))
                {
                    Connection.Open();
                    //ищем код теста в analyzer configuration
                    SqlCommand TestCodeCommand = new SqlCommand(
                       "SELECT TOP 1 k.amt_analyskod  FROM konvana k " +
                       $"WHERE k.ins_maskin = '{AnalyzerConfigurationCode}' AND k.met_kod = '{CGMTestCodesPar}' ", Connection);
                    SqlDataReader Reader = TestCodeCommand.ExecuteReader();

                    if (Reader.HasRows) // если есть данные
                    {
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { BackTestCode = Reader.GetString(0); };
                        }
                    }
                    Reader.Close();
                    Connection.Close();
                }
            }
            catch (Exception error)
            {
                FileResultLog($"Error: {error}");
            }
            return BackTestCode;
        }

        // Функция преобразования кода теста прибора в код теста PSMV2 в CGM
        public static string TranslateToPSMCodes(string AnalyzerTestCodesPar)
        {
            string BackTestCode = "";
            try
            {
                string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
                //string CGMConnectionString = @"Data Source=CGM-APP11\SQLCGMAPP11;Initial Catalog=KDLPROD; Integrated Security=True;";
                CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");
                using (SqlConnection Connection = new SqlConnection(CGMConnectionString))
                {
                    Connection.Open();
                    // Ищем только тесты, которые настроены для прибора exias и настроены для PSMV2
                    SqlCommand TestCodeCommand = new SqlCommand(
                       "SELECT k1.amt_analyskod  FROM konvana k " +
                       "LEFT JOIN konvana k1 ON k1.met_kod = k.met_kod AND k1.ins_maskin = 'PSMV2' " +
                       $"WHERE k.ins_maskin = '{AnalyzerConfigurationCode}' AND k.amt_analyskod = '{AnalyzerTestCodesPar}' ", Connection);
                    SqlDataReader Reader = TestCodeCommand.ExecuteReader();

                    if (Reader.HasRows) // если есть данные
                    {
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { BackTestCode = Reader.GetString(0); };
                        }
                    }
                    Reader.Close();
                    Connection.Close();
                }
            }
            catch (Exception error)
            {
                FileResultLog($"Error: {error}");
            }

            return BackTestCode;
        }
        #endregion

        #region Функции обмена сообщениями с анализатором

        #region Отправка ACK
        static void ACKSending(Socket client_, Encoding utf8, string id)
        {
            // ACK sending
            DateTime now = DateTime.Now;
            string ackDate = now.ToString("yyyyMMddHHmmss");

            // шаблон ответа ACK в формате HL7 (по мануалу)
            string ackMSH = $@"MSH|^~\&|||||{ackDate}||ACK^R01|{id}|P|2.3.1||||0||ASCII|||";
            string ackMSA = $@"MSA|AA|{id}|Message accepted|||0|";

            string ackResponse = "";

            ackResponse = ackMSH + '\r' + ackMSA;

            // строка ответа с результатом
            byte[] SendingMessageBytes;
            //SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ackResponse), FS, CR);
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ackResponse), CR, FS, CR);

            if (client_.Poll(1, SelectMode.SelectWrite))
            {
                client_.Send(SendingMessageBytes);
                ExchangeLog($"Sending ACK to analyzer");
                ExchangeLog("LIS:" + "\n" + utf8.GetString(SendingMessageBytes));
                ExchangeLog($"");
            }
        }
        #endregion

        #region Отправка QCK - подтверждения на запрос задания (если данные есть в ЛИС))

        static void QCKSending(Socket client_, Encoding utf8, string id)
        {
            DateTime now = DateTime.Now;
            string qckDate = now.ToString("yyyyMMddHHmmss");

            // шаблон ответа QCK в формате HL7 (по мануалу)
            string qckMSH = $@"MSH|^~\&|||||{qckDate}||QCK^Q02|{id}|P|2.3.1||||0||ASCII|||";
            string qckMSA = $@"MSA|AA|{id}|Message accepted|||0|";
            string qckERR = $@"ERR|0|";
            string qckQAK = $@"QAK|SR|OK|";
            string qckResponse = "";

            qckResponse = qckMSH + '\r' + qckMSA + '\r' + qckERR + '\r' + qckQAK;
            // строка ответа с результатом
            byte[] SendingMessageBytes;
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(qckResponse), CR, FS, CR);

            client_.Send(SendingMessageBytes);

            ExchangeLog($"RID exists. Sending QCK^Q02 to analyzer.");
            ExchangeLog("LIS (SRV):" + "\n" + utf8.GetString(SendingMessageBytes));
            ExchangeLog($"");
        }

        #endregion

        #region Отправка QCK, если текущего RID не существует
        // If the sample of the bar code does not exist,
        static void QCKNFSending(Socket client_, Encoding utf8, string id)
        {
            DateTime now = DateTime.Now;
            string qckDate = now.ToString("yyyyMMddHHmmss");

            // шаблон ответа QCK в формате HL7 (по мануалу)
            string qckMSH = $@"MSH|^~\&|||||{qckDate}||QCK^Q02|{id}|P|2.3.1||||0||ASCII|||";
            string qckMSA = $@"MSA|AA|{id}|Message accepted|||0|";
            string qckERR = $@"ERR|0|";
            string qckQAK = $@"QAK|SR|NF|";
            string qckResponse = "";

            qckResponse = qckMSH + '\r' + qckMSA + '\r' + qckERR + '\r' + qckQAK;
            // строка ответа с результатом
            byte[] SendingMessageBytes;
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(qckResponse), CR, FS, CR);

            client_.Send(SendingMessageBytes);

            ExchangeLog($"RID does NOT exist. Sending QCK^Q02 to analyzer.");
            ExchangeLog("LIS:" + "\n" + utf8.GetString(SendingMessageBytes));
            ExchangeLog($"");
        }

        #endregion

        #region Отправка DSR - задания анализатору, демографии пациента, тест
        static void DSRSending(Socket client_, Encoding utf8, string id, string rid, string pid, string FullName, string birthday, string sex, string sampledate, string dsrDSP29)
        {
            DateTime now = DateTime.Now;
            string dsrDate = now.ToString("yyyyMMddHHmmss");

            #region шаблон задания HL7
            // шаблон ответа DSR в формате HL7 (по мануалу)
            // вместо dsrdate в msh скорее всего нужно дату которая в запросе от прибора, так же и в QCK 
            string dsrMSH = $@"MSH|^~\&|||||{dsrDate}||DSR^Q03|{id}|P|2.3.1||||0||ASCII|||";
            string dsrMSA = $@"MSA|AA|{id}|Message accepted|||0|";
            string dsrERR = $@"ERR|0|";
            string dsrQAK = $@"QAK|SR|OK|";

            string dsrQRD = $@"QRD|{dsrDate}|R|D|{id}|||RD|{rid}|OTH|||T|";
            string dsrQRF = $@"QRF||{dsrDate}|{dsrDate}|||RCT|COR|ALL||";

            string dsrDSP1 = $@"DSP|1||{pid}|||";
            string dsrDSP2 = $@"DSP|2||1|||";
            string dsrDSP3 = $@"DSP|3||{FullName}|||";
            string dsrDSP4 = $@"DSP|4||{birthday}|||";
            string dsrDSP5 = $@"DSP|5||{sex}|||";
            string dsrDSP6 = $@"DSP|6||O|||";

            string dsrDSP7 = $@"DSP|7|||||";
            string dsrDSP8 = $@"DSP|8|||||";
            string dsrDSP9 = $@"DSP|9|||||";
            string dsrDSP10 = $@"DSP|10|||||";
            string dsrDSP11 = $@"DSP|11|||||";
            string dsrDSP12 = $@"DSP|12|||||";
            string dsrDSP13 = $@"DSP|13|||||";
            string dsrDSP14 = $@"DSP|14|||||";

            string dsrDSP15 = $@"DSP|15||outpatient|||";
            string dsrDSP16 = $@"DSP|16|||||";
            string dsrDSP17 = $@"DSP|17||own|||";
            string dsrDSP18 = $@"DSP|18|||||";
            string dsrDSP19 = $@"DSP|19|||||";
            string dsrDSP20 = $@"DSP|20|||||";

            string dsrDSP21 = $@"DSP|21||{rid}|||";
            string dsrDSP22 = $@"DSP|22||3|||";
            string dsrDSP23 = $@"DSP|23||{sampledate}|||";
            string dsrDSP24 = $@"DSP|24||N|||";
            string dsrDSP25 = $@"DSP|25|||||";
            string dsrDSP26 = $@"DSP|26||serum|||";
            string dsrDSP27 = $@"DSP|27||КДЛ|||";
            string dsrDSP28 = $@"DSP|28||КДЛ|||";

            //string dsrDSP29 = $@"DSP|29||443^^^|||";


            string dsrDSPall = dsrDSP1 + '\r' + dsrDSP2 + '\r' + dsrDSP3 + '\r' + dsrDSP4 + '\r' + dsrDSP5 + '\r' + dsrDSP6 + '\r' + dsrDSP7 + '\r' +
                               dsrDSP8 + '\r' + dsrDSP9 + '\r' + dsrDSP10 + '\r' + dsrDSP11 + '\r' + dsrDSP12 + '\r' + dsrDSP13 + '\r' + dsrDSP14 + '\r' +
                               dsrDSP15 + '\r' + dsrDSP16 + '\r' + dsrDSP17 + '\r' + dsrDSP18 + '\r' + dsrDSP19 + '\r' + dsrDSP20 + '\r' + dsrDSP21 + '\r' +
                               dsrDSP22 + '\r' + dsrDSP23 + '\r' + dsrDSP24 + '\r' + dsrDSP25 + '\r' + dsrDSP26 + '\r' + dsrDSP27 + '\r' + dsrDSP28 + '\r' + dsrDSP29;


            string dsrDSC = $@"DSC||";

            #endregion

            // полное сообщение с заданием и демографией
            string dsrResponse = "";
            dsrResponse = dsrMSH + '\r' + dsrMSA + '\r' + dsrERR + '\r' + dsrQAK + '\r' + dsrQRD + '\r' + dsrQRF + '\r' + dsrDSPall + '\r' + dsrDSC;

            // строка ответа с результатом
            byte[] SendingMessageBytes;
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(dsrResponse), CR, FS, CR);

            client_.Send(SendingMessageBytes);

            ExchangeLog($"Sending DSR^Q03 to analyzer.");
            ExchangeLog("LIS:" + "\n" + utf8.GetString(SendingMessageBytes));
            ExchangeLog($"");

        }

        #endregion

        #endregion

        #region получение данных по заявке и отправка прибору задания
        public static void GetRequestFromCGMDB(Socket client_, Encoding utf8, string id, string RIDPar)
        {
            // переменные для данных из CGM
            string PID = "";
            string PatientSurname = "";
            string PatientName = "";
            string FullName = "";
            string PatientSex = "";
            string PatientBirthDay = "";
            string LISTestCode = "";
            DateTime PatientBirthDayDate = new DateTime();
            DateTime RegistrationDateDate = DateTime.Now;
            DateTime SampleDateDate = DateTime.Now;
            string SampleDate = "";
            bool RIDExists = false;
            int dspCounter = 28;
            string DSPTests = "";

            string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
            CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");

            try
            {
                using (SqlConnection CGMconnection = new SqlConnection(CGMConnectionString))
                {
                    CGMconnection.Open();

                    //ищем RID в базе
                    SqlCommand RequetDataCommand = new SqlCommand(
                       "SELECT TOP 1" +
                         "p.pop_pid AS PID, p.pop_enamn AS PatientSurname, p.pop_fnamn AS PatientName, p.pop_fdatum AS PatientBirthday, " +
                         "CASE WHEN p.pop_kon = 'K' THEN 'Female' ELSE 'Male' END AS PatientSex, " +
                         "r.rem_ank_dttm AS RegistrationDate " +
                       "FROM dbo.remiss (NOLOCK) r " +
                         "INNER JOIN dbo.pop (NOLOCK) p ON p.pop_pid = r.pop_pid " +
                       "WHERE r.rem_deaktiv = 'O' " +
                         $"AND r.rem_rid IN ('{RIDPar}') " +
                         "AND r.rem_ank_dttm IS NOT NULL ", CGMconnection);
                    SqlDataReader Reader = RequetDataCommand.ExecuteReader();

                    // если такой ШК есть
                    if (Reader.HasRows)
                    {
                        RIDExists = true;
                        // получаем данные по заявке
                        while (Reader.Read())
                        {
                            if (!Reader.IsDBNull(0)) { PID = Reader.GetString(0); };
                            if (!Reader.IsDBNull(1)) { PatientSurname = Reader.GetString(1); };
                            if (!Reader.IsDBNull(2)) { PatientName = Reader.GetString(2); };
                            if (!Reader.IsDBNull(3))
                            {
                                PatientBirthDayDate = Reader.GetDateTime(3);
                                //PatientBirthDay = PatientBirthDayDate.Year + CheckZero(PatientBirthDayDate.Month) + CheckZero(PatientBirthDayDate.Day);
                                PatientBirthDay = PatientBirthDayDate.Year + CheckZero(PatientBirthDayDate.Month) + CheckZero(PatientBirthDayDate.Day) + CheckZero(PatientBirthDayDate.Hour)
                                                  + CheckZero(PatientBirthDayDate.Minute) + CheckZero(PatientBirthDayDate.Second); ;
                            }
                            if (!Reader.IsDBNull(4)) { PatientSex = Reader.GetString(4); };

                            if (!Reader.IsDBNull(5))
                            {
                                RegistrationDateDate = Reader.GetDateTime(5);
                            };
                        }
                    }
                    Reader.Close();

                    if (RIDExists)
                    {
                        SqlCommand TestCodeCommand = new SqlCommand(
                            "SELECT b.ana_analyskod, prov.pro_provdat " +
                            "FROM dbo.remiss (NOLOCK) r " +
                              "INNER JOIN dbo.bestall (NOLOCK) b ON b.rem_id = r.rem_id " +
                              "INNER JOIN dbo.prov (NOLOCK) prov ON prov.pro_id = b.pro_id " +
                            "WHERE r.rem_deaktiv = 'O' " +
                            $"AND r.rem_rid IN('{RIDPar}') " +
                            "AND r.rem_ank_dttm IS NOT NULL", CGMconnection);
                        SqlDataReader TestsReader = TestCodeCommand.ExecuteReader();
                        // Если задания есть
                        if (TestsReader.HasRows)
                        {
                            while (TestsReader.Read())
                            {
                                //test = TestsReader.GetString(0);
                                if (!TestsReader.IsDBNull(0))
                                {
                                    LISTestCode = TestsReader.GetString(0);
                                    string AnalyzerTestCode = TranslateToAnalyzerCodes(LISTestCode);
                                    dspCounter++;
                                    if (dspCounter == 29)
                                    {
                                        DSPTests = $@"DSP|{dspCounter}||{AnalyzerTestCode}^^^|||";
                                    }
                                    else
                                    {
                                        DSPTests = DSPTests + '\r' + $@"DSP|{dspCounter}||{AnalyzerTestCode}^^^|||";
                                    }
                                }
                                // Sample date from prov table
                                SampleDateDate = TestsReader.GetDateTime(1);
                                SampleDate = SampleDateDate.Year + CheckZero(SampleDateDate.Month) + CheckZero(SampleDateDate.Day) + CheckZero(SampleDateDate.Hour)
                                            + CheckZero(SampleDateDate.Minute) + CheckZero(SampleDateDate.Second);
                            }
                        }
                        TestsReader.Close();
                    }
                    CGMconnection.Close();
                }

                // Если ШК существует, то отправляем подтверждение
                if (RIDExists)
                {
                    // отправляем сообщение с подтверждением
                    QCKSending(client_, utf8, id);

                    Thread.Sleep(100);

                    FullName = PatientSurname + ' ' + PatientName;
                    // отправляем прибору задание и демографию пациента
                    DSRSending(client_, utf8, id, RIDPar, PID, FullName, PatientBirthDay, PatientSex, SampleDate, DSPTests);
                }
                // Если не удалось найти ШК в ЛИС
                else
                {
                    // отправляем сообщение с подтверждением
                    QCKNFSending(client_, utf8, id);
                }
            }
            catch (Exception ex)
            {
               // Console.WriteLine(ex);
            }
        }

        #endregion

        #region Функция обработки файлов с результатами и создания файлов для службы, которая разберет файл и запишет данные в CGM
        static void ResultsProcessing()
        {
            while (ServiceIsActive)
            {
                try
                {
                    #region папки архива, результатов и ошибок

                    string OutFolder = ConfigurationManager.AppSettings["FolderOut"];
                    // архивная папка
                    string ArchivePath = AnalyzerResultPath + @"\Archive";
                    // папка для ошибок
                    string ErrorPath = AnalyzerResultPath + @"\Error";
                    // папка для файлов с результатами для CGM
                    string CGMPath = AnalyzerResultPath + @"\CGM";

                    if (!Directory.Exists(ArchivePath))
                    {
                        Directory.CreateDirectory(ArchivePath);
                    }

                    if (!Directory.Exists(ErrorPath))
                    {
                        Directory.CreateDirectory(ErrorPath);
                    }

                    if (!Directory.Exists(CGMPath))
                    {
                        Directory.CreateDirectory(CGMPath);
                    }
                    #endregion

                    // строки для формирования файла (psm файла) с результатами для службы,
                    // которая разбирает файлы и записывает результаты в CGM
                    string MessageHead = "";
                    string MessageTest = "";
                    string AllMessage = "";

                    // поолучаем список всех файлов в текущей папке
                    string[] Files = Directory.GetFiles(AnalyzerResultPath, "*.res");

                    // шаблоны регулярных выражений для поиска данных
                    string RIDPattern = @"OBR[|]\d+[|](?<RID>\d+)[|]\S*";
                    string TestPattern = @"OBX[|]\d+[|]NM[|](?<Test>\d+)[|]\S*";
                    // Предполагаем, что в названии теста нет цифр
                    string ResultPattern = @"OBX[|]\d+[|]NM[|]\d+[|]\D+[|](?<Result>\d+[.]?\d*)[|]\S+";

                    Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex TestRegex = new Regex(TestPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex ResultRegex = new Regex(ResultPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));

                    // пробегаем по файлам
                    foreach (string file in Files)
                    {
                        FileResultLog(file);
                        string[] lines = System.IO.File.ReadAllLines(file);
                        string RID = "";
                        string Test = "";
                        //string Result = "";

                        // обрезаем только имя текущего файла
                        string FileName = file.Substring(AnalyzerResultPath.Length + 1);
                        // название файла .ок, который должен создаваться вместе с результирующим для обработки службой FileGetterService
                        string OkFileName = "";

                        // проходим по строкам в файле
                        foreach (string line in lines)
                        {
                            string line_ = line.Replace("^", "@");
                            Match RIDMatch = RIDRegex.Match(line_);
                            Match TestMatch = TestRegex.Match(line_);
                            Match ResultMatch = ResultRegex.Match(line_);

                            // поиск RID в строке
                            if (RIDMatch.Success)
                            {
                                RID = RIDMatch.Result("${RID}");
                                FileResultLog($"Заявка № {RID}");
                                MessageHead = $"O|1|{RID}||ALL|R|20230101000100|||||X||||ALL||||||||||F";
                            }
                            else
                            {

                            }

                            // поиск теста в строке
                            if (TestMatch.Success)
                            {
                                Test = TestMatch.Result("${Test}");
                                // преобразуем тест в код теста PSM
                                string PSMTestCode = TranslateToPSMCodes(Test);
                                string Result = "";
                                if (ResultMatch.Success)
                                {
                                    Result = ResultMatch.Result("${Result}");
                                    FileResultLog($"PSMV2 код: {PSMTestCode}");
                                    FileResultLog($"{Test} - результат: {Result}");

                                    // нужно округлять значение результата до 2 цифр после запятой
                                    IFormatProvider formatter = new NumberFormatInfo { NumberDecimalSeparator = "." };
                                    double res = double.Parse(Result, formatter);
                                    res = Math.Round(res, 3);

                                    // обработка результатов ниже предела чувствительности методики

                                    // Если тест IgA (ИМО130) - 0.2 г/л
                                    if (Test == "441" && res < 0.2)
                                    {
                                        FileResultLog($"Результат {res} ниже предела чувствительности методики. Замена результата на <0.2");
                                        Result = "<0.2";
                                    }
                                    // Если тест IgG (ИМ0135) - 0.3 г/л
                                    else if (Test == "443" && res < 0.3)
                                    {
                                        FileResultLog($"Результат {res} ниже предела чувствительности методики. Замена результата на <0.3");
                                        Result = "<0.3";
                                    }
                                    // Если тест IgM (ИМ0140) - 0.05 г/л
                                    else if (Test == "442" && res < 0.05)
                                    {
                                        FileResultLog($"Результат {res} ниже предела чувствительности методики. Замена результата на <0.05");
                                        Result = "<0.05";
                                    }
                                    else
                                    {
                                        FileResultLog($"Результат округлен: {res}");
                                        Result = res.ToString();
                                    }

                                    //FileResultLog($"Результат округлен: {res}");
                                    //Result = res.ToString();

                                    if ((PSMTestCode != "") && (Result != ""))
                                    {
                                        // формируем строку с ответом для результирующего файла
                                        MessageTest = MessageTest + $"R|1|^^^{PSMTestCode}^^^^{AnalyzerCode}|{Result}|||N||F||MINDRAY^||20230101000001|{AnalyzerCode}" + "\r";
                                    }
                                }
                            }
                        }

                        // получаем название файла .ок на основании файла с результатом
                        if (FileName.IndexOf(".") != -1)
                        {
                            OkFileName = FileName.Split('.')[0] + ".ok";
                        }

                        // если строки с результатами и с ШК не пустые, значит формируем результирующий файл
                        if (MessageHead != "" && MessageTest != "")
                        {
                            try
                            {
                                // собираем полное сообщение с результатом
                                AllMessage = MessageHead + "\r" + MessageTest;
                                FileResultLog(AllMessage);

                                // создаем файл для записи результата в папке для рез-тов
                                //if (!File.Exists(CGMPath + @"\" + FileName))
                                if (!File.Exists(OutFolder + @"\" + FileName))
                                {
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }
                                else
                                {
                                    //File.Delete(CGMPath + @"\" + FileName);
                                    //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + FileName))
                                    File.Delete(OutFolder + @"\" + FileName);
                                    using (StreamWriter sw = File.CreateText(OutFolder + @"\" + FileName))
                                    {
                                        foreach (string msg in AllMessage.Split('\r'))
                                        {
                                            sw.WriteLine(msg);
                                        }
                                    }
                                }

                                // создаем .ok файл в папке для рез-тов
                                if (OkFileName != "")
                                {
                                    //if (!File.Exists(CGMPath + @"\" + OkFileName))
                                    if (!File.Exists(OutFolder + @"\" + OkFileName))
                                    {
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName))
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                    else
                                    {
                                        //File.Delete(CGMPath + OkFileName);
                                        //using (StreamWriter sw = File.CreateText(CGMPath + @"\" + OkFileName))
                                        File.Delete(OutFolder + OkFileName);
                                        using (StreamWriter sw = File.CreateText(OutFolder + @"\" + OkFileName))
                                        {
                                            sw.WriteLine("ok");
                                        }
                                    }
                                }

                                // помещение файла в архивную папку
                                if (File.Exists(ArchivePath + @"\" + FileName))
                                {
                                    File.Delete(ArchivePath + @"\" + FileName);
                                }
                                File.Move(file, ArchivePath + @"\" + FileName);

                                FileResultLog("Файл обработан и перемещен в папку Archive");
                                FileResultLog("");
                            }
                            catch (Exception e)
                            {
                                FileResultLog(e.ToString());
                                // помещение файла в папку с ошибками
                                if (File.Exists(ErrorPath + @"\" + FileName))
                                {
                                    File.Delete(ErrorPath + @"\" + FileName);
                                }
                                File.Move(file, ErrorPath + @"\" + FileName);

                                FileResultLog("Ошибка обработки файла. Файл перемещен в папку Error");
                                FileResultLog("");
                            }
                        }
                        else
                        {
                            // помещение файла в папку с ошибками
                            if (File.Exists(ErrorPath + @"\" + FileName))
                            {
                                File.Delete(ErrorPath + @"\" + FileName);
                            }
                            File.Move(file, ErrorPath + @"\" + FileName);

                            FileResultLog("Ошибка обработки файла. Файл перемещен в папку Error");
                            FileResultLog("");
                        }
                    }
                }
                catch (Exception ex)
                {
                    FileResultLog(ex.ToString());
                }
                Thread.Sleep(1000);
            }
        }

        #endregion

        // TCP server
        static void TCPServer()
        {
            try
            {
                while (ServiceIsActive)
                {
                    IPAddress ip = IPAddress.Parse(IPadress);
                    // локальная точка EndPoint, на которой сокет будет принимать подключения от клиентов
                    EndPoint endpoint = new IPEndPoint(ip, port);
                    // создаем сокет
                    Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    // связываем сокет с локальной точкой endpoint 
                    socket.Bind(endpoint);

                    // получаем конечную точку, с которой связан сокет
                    ServiceLog(socket.LocalEndPoint.ToString());

                    // запуск прослушивания подключений
                    socket.Listen(1000);
                    ServiceLog("TCP Сервер запущен. Ожидание подключений...");
                    // После начала прослушивания сокет готов принимать подключения
                    // получаем входящее подключение
                    Socket client = socket.Accept();

                    // получаем адрес клиента, который подключился к нашему tcp серверу
                    ServiceLog($"Адрес подключенного клиента: {client.RemoteEndPoint}");

                    int ServerCount = 0; // счетчик

                    while (ServiceIsActive)
                    {
                        #region состояние сокета
                        // состояние сокета
                        // client.Poll(1, SelectMode.SelectRead) - true, если:
                        // если был вызван метод Listen(Int32) и подключение отложено
                        // если данные доступны для чтения
                        // если подключение закрыто, сброшено или завершено
                        // Console.WriteLine($"handler.Available {client.Available}; " +
                        //   $"SelectRead: {client.Poll(1, SelectMode.SelectRead)};" +
                        //   $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)};" +
                        //   $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                        #endregion

                        // нет данных для чтения и соединение не активно
                        if (client.Poll(1, SelectMode.SelectRead) && client.Available == 0)
                        {
                            client = socket.Accept();
                            ServiceLog("Ожидание переподключения");
                        }

                        // если клиент ничего не посылает
                        if (client.Available == 0)
                        {
                            ServerCount++;
                            if (ServerCount == 100)
                            {
                                ServerCount = 0;
                                //ServiceLog("Текущее время: " + DateTime.Now.ToString("hh:mm:ss") + " / " + DateTime.Now.TimeOfDay.ToString() + " / " + DateTime.Now.ToString("HH:mm:ss"));
                                //ServiceLog("Текущее время UTC: " + DateTime.UtcNow.ToString("hh:mm:ss") + " / " + DateTime.UtcNow.ToString());
                                ServiceLog("Свойства сокета: " +
                                            $"Blocking: {client.Blocking}; " +
                                            $"Connected: {client.Connected}; " +
                                            $"RemoteEndPoint: {client.RemoteEndPoint}; " +
                                            $"LocalEndPoint: {client.LocalEndPoint}; ");
                                ServiceLog("Состояние сокета: " +
                                           $"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)}; " +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)}; " +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                                ServiceLog("Прослушивание сокета...");
                                ServiceLog("");

                                // Accept синхронно извлекает первый ожидающий запрос на подключение из очереди запросов на подключение прослушиваемого сокета, а затем создает и возвращает новый Socket.
                                ServiceLog("Ожидание инициализации соединения от прибора...");
                                client = socket.Accept();
                                ServiceLog($"Адрес подключенного клиента: {client.RemoteEndPoint}");
                            }
                        }

                        // есть данные на сокете, получаем сообщение от анализатора
                        else
                        {
                            // UTF8 encoder
                            Encoding utf8 = Encoding.UTF8;
                            // количество полученных байтов
                            int received_bytes = 0;
                            // буфер для получения данных
                            byte[] received_data = new byte[1024];
                            // StringBuilder для склеивания полученных данных в одну строку
                            var messageFromMindray = new StringBuilder();

                            ServiceLog("Свойства сокета: " + $"Blocking: {client.Blocking}; " + $"Connected: {client.Connected}; " + $"RemoteEndPoint: {client.RemoteEndPoint}; ");

                            // состояние сокета
                            ServiceLog("Состояние сокета: " +
                                           $"handler.Available {client.Available}; " +
                                           $"SelectRead: {client.Poll(1, SelectMode.SelectRead)}; " +
                                           $"SelectWrite: {client.Poll(1, SelectMode.SelectWrite)}; " +
                                           $"SelectError: {client.Poll(1, SelectMode.SelectError)};");
                            ServiceLog("Есть данные на сокете. Получение сообщения от анализатора.");
                            ServiceLog("");

                            // считываем, пока есть данные на сокете
                            do
                            {
                                received_bytes = client.Receive(received_data);
                                // GetString - декодирует последовательность байтов из указанного массива байтов в строку.
                                // преобразуем полученный набор байтов в строку
                                string ResponseMsg = Encoding.UTF8.GetString(received_data, 0, received_bytes);

                                // добавляем в StringBuilder
                                messageFromMindray.Append(ResponseMsg);
                                //ExchangeLog(messageFromMindray.ToString());
                                ExchangeLog("Analyzer:" + "\n" + messageFromMindray.ToString());
                            }
                            while (client.Available > 0);

                            // нужно заменить птички, иначе рег.выражение не работает
                            string messageMindray = messageFromMindray.ToString().Replace("^", "@");

                            #region Определение типа сообщения от прибора
                            // Тип сообщения QRY - запрос задания
                            string QRYPattern = @"MSH[|]\S+[|](?<type>\w+)@Q02[|]\S+[|]";
                            Regex QRYRegex = new Regex(QRYPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string QRY = "";

                            Match QRYMatch = QRYRegex.Match(messageMindray);

                            if (QRYMatch.Success)
                            {
                                QRY = QRYMatch.Result("${type}");
                                //ExchangeLog($"Message type: {QRY}");
                            }

                            // Тип сообщения ORU - сообщение с результатом
                            string ORUPattern = @"MSH[|]\S+[|](?<type>\w+)@R01[|]\S+[|]";
                            Regex ORURegex = new Regex(ORUPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string ORU = "";

                            Match ORUMatch = ORURegex.Match(messageMindray);

                            if (ORUMatch.Success)
                            {
                                ORU = ORUMatch.Result("${type}");
                                //ExchangeLog($"Message type: {ORU}");
                            }

                            #endregion

                            #region нахождение MessageId  в сообщении анализатора
                            // шаблона для поиска Message Id в сообщении от прибора
                            string MessageIdPattern = @"\S+[|](?<MessageId>\d+)[|]P[|]2.3.1[|]";
                            Regex MessageIdRegex = new Regex(MessageIdPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string MessageId = "";

                            Match MessageIdMatch = MessageIdRegex.Match(messageMindray);
                            if (MessageIdMatch.Success)
                            {
                                MessageId = MessageIdMatch.Result("${MessageId}");
                                ExchangeLog($"Message ID : {MessageId}");
                            }

                            #endregion

                            // если сообщение с результатами - ORU
                            if (ORU == "ORU")
                            {
                                // формируем файл с результатом
                                MakeAnalyzerResultFile(messageFromMindray.ToString());
                                // отправляем прибору подтверждение получения - ACK
                                ACKSending(client, utf8, MessageId);
                            }

                            // если прибор запрашивает задание - QRY
                            if (QRY == "QRY")
                            {
                                // шаблона для поиска RID в сообщении от прибора QRY
                                string RIDPattern = @"QRD[|]\S+RD[|](?<RID>\d+)[|]OTH[|]\S+";
                                Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                                string RID = "";

                                Match RIDMatch = RIDRegex.Match(messageMindray);

                                if (RIDMatch.Success)
                                {
                                    RID = RIDMatch.Result("${RID}");
                                    ExchangeLog($"RID: {RID}");
                                }

                                // Првоеряем, есть ли RID, получаем задание из базы CGM и  отправляем подтверждение прибору
                                GetRequestFromCGMDB(client, utf8, MessageId, RID);
                            }
                        }

                        /*
                        #region Перезагрузка сокета
                        // Т.к. соединение отваливаетя, когда прибор уходит в режим сна, принудительно перезагружаем соединение каждый день в 8:00
                        DateTime now = DateTime.Now;
                        // перезагрузка в 08:00 и в 20:00
                        if (now.ToString("hh:mm:ss") == "08:00:00")
                        // перезагрузка в 08:00
                        //if (now.ToString("HH:mm:ss") == "08:00:00")
                        {
                            // Socket.Shutdown - блокирует передачу и получение данных для объекта Socket.
                            // Close - Закрывает подключение Socket и освобождает все связанные ресурсы.
                            // Accept - синхронно извлекает первый ожидающий запрос на подключение из очереди запросов на подключение прослушиваемого сокета, а затем создает и возвращает новый Socket.

                            //client.Shutdown(SocketShutdown.Both);
                            //client.Close();

                            ServiceLog("Свойства сокета: " + $"Blocking: {client.Blocking}; " + $"Connected: {client.Connected}; " + $"RemoteEndPoint: {client.RemoteEndPoint}; ");
                            ServiceLog("Перезагрузка соединения");
                            ServiceLog("Ожидание инициализации соединения от прибора...");
                            client = socket.Accept();
                            ServiceLog($"Адрес подключенного клиента: {client.RemoteEndPoint}");
                            //ServiceLog("Свойства сокета: " + $"Blocking: {client.Blocking}; " + $"Connected: {client.Connected}; " + $"RemoteEndPoint: {client.RemoteEndPoint}; ");

                        }
                        #endregion
                        */


                        Thread.Sleep(1000);
                    }
                }
            }
            catch (Exception error)
            {
                ServiceLog($"Exception: {error}");
            }
        }

        protected override void OnStart(string[] args)
        {
            ServiceIsActive = true;
            ServiceLog("Сервис начал работу.");


            //TCP сервер для прибора
            Thread TCPServerThread = new Thread(new ThreadStart(TCPServer));
            TCPServerThread.Name = "TCPServer";
            TCPServerThread.Start();

            // Поток обработки результатов
            Thread ResultProcessingThread = new Thread(ResultsProcessing);
            ResultProcessingThread.Name = "ResultsProcessing";
            ResultProcessingThread.Start();
        }

        protected override void OnStop()
        {
            ServiceIsActive = false;
            ServiceLog("Сервис остановлен");
        }
    }
}
