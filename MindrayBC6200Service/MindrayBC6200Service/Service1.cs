using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;

namespace MindrayBC6200Service
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        #region settings
        //public static string IPadress = "172.18.95.31"; // cgm-app12, подсетка приборов
        public static string IPadress = "10.128.131.112"; // cgm-app12, подсетка приборов
        public static int port = 8017;                  // порт

        public static string AnalyzerCode = "906";                  // код из аналайзер конфигурейшн, который связывает прибор в PSMV2
        public static string AnalyzerConfigurationCode = "MIN6200"; // код прибора из аналайзер конфигурейшн

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

        #region вспомогательные функции

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
        // each analysis result message need a response message which contains two segments: MSH and MSA.
        #region Отправка ACK
        static void ACKSending(Socket client_, Encoding utf8, string id)
        {
            // ACK sending
            DateTime now = DateTime.Now;
            string ackDate = now.ToString("yyyyMMddHHmmss");

            // шаблон ответа ACK в формате HL7 (по мануалу)
            string ackMSH = $@"MSH|^~\&|BC-6800|Mindray|||{ackDate}||ACK^R01|{id}|P|2.3.1||||||UNICODE";
            string ackMSA = $@"MSA|AA|{id}";

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

        #region отправка сообщения ORR^O02 - send back a request response message
        static void ORRSending(Socket client_, Encoding utf8, string id, string rid, string pid, string FullName, string birthday, string sex, string sampledate, string TestMethod)
        {
            DateTime now = DateTime.Now;
            string ORRMsgDate = now.ToString("yyyyMMddHHmmss");

            #region шаблон задания HL7
            string ORRMSH = $@"MSH|^~\&|LIS||||{ORRMsgDate}||ORR^O02|{id}|P|2.3.1||||||UNICODE";
            string ORRMSA = $@"MSA|AA|{id}";

            //string ORRPID = $@"PID|1||{PID}^^^^MR||{PatientSurname}^{PatientName}||{PatientBirthDay}000000|{PatientSex}";
            string ORRPID = $@"PID|1||{pid}^^^^MR||{FullName}||{birthday}000000|{sex}";
            string ORRPV1 = $@"PV1|1|Outpatient|RP^^1|||||||||||||||||Public";

            string ORRORC = $@"ORC|AF|{rid}|||";
            string ORROBR = $@"OBR|1|{rid}||01001^Automated Count^99MRC||{sampledate}||||||||{sampledate}||||||||||HM||||||||admin";

            string ORROBX = $@"OBX|1|IS|08001^Take Mode^99MRC||A||||||F" + '\r' +
                            $@"OBX|2|IS|08002^Blood Mode^99MRC||W||||||F" + '\r' +
                            $@"OBX|3|IS|08003^Test Mode^99MRC||{TestMethod}||||||F" +
                            $@"OBX|4|IS|01002^Ref Group^99MRC||Child||||||F";
            //$@"OBX|1|IS|02001^Take Mode^99MRC||O||||||" + '\r' +
            //                $@"OBX|2|IS|02002^Blood Mode^99MRC||W||||||" + '\r' +
            //              $@"OBX|3|IS|02003^Test Mode^99MRC||{TestMethod}||||||";

            #endregion

            // полное сообщение с заданием и демографией
            string ORRResponse = "";
            ORRResponse = ORRMSH + '\r' + ORRMSA + '\r' + ORRPID + '\r' + ORRPV1 + '\r' + ORRORC + '\r' + ORROBR + '\r' + ORROBX;

            // строка ответа с результатом
            byte[] SendingMessageBytes;
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ORRResponse), CR, FS, CR);

            client_.Send(SendingMessageBytes);

            ExchangeLog($"Sending ORR^O02 to analyzer.");
            ExchangeLog("LIS:" + "\n" + utf8.GetString(SendingMessageBytes));
            ExchangeLog($"");
        }
        #endregion

        #region отправка ORR^O02, когда ШК не найден
        static void ORRNFSending(Socket client_, Encoding utf8, string id)
        {
            DateTime now = DateTime.Now;
            string ORRNFDate = now.ToString("yyyyMMddHHmmss");

            // шаблон ответа ORR в формате HL7 (по мануалу)
            string ORRNFMSH = $@"MSH|^~\&|LIS||||{ORRNFDate}||ORR^O02|{id}|P|2.3.1||||||UNICODE";
            string ORRNFMSA = $@"MSA|AR|{id}";

            string ORRNFResponse = "";

            ORRNFResponse = ORRNFMSH + '\r' + ORRNFMSA;
            // строка ответа с результатом
            byte[] SendingMessageBytes;
            SendingMessageBytes = ConcatByteArray(VT, utf8.GetBytes(ORRNFResponse), CR, FS, CR);

            client_.Send(SendingMessageBytes);

            ExchangeLog($"RID does NOT exist. Sending ORR^O02 to analyzer.");
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

            string TestMode = "CBC"; // метод по умолчанию, CBC выполняется всегда
            bool RetExists = false;
            bool DiffExists = false;
            bool NrbcExists = false;

            string CGMConnectionString = ConfigurationManager.ConnectionStrings["CGMConnection"].ConnectionString;
            CGMConnectionString = String.Concat(CGMConnectionString, $"User Id = {user}; Password = {password}");

            try
            {
                using (SqlConnection CGMconnection = new SqlConnection(CGMConnectionString))
                {
                    CGMconnection.Open();

                    #region ищем RID и получаем данные по нему из БД
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
                    #endregion

                    #region есть ли тесты в задании и определяем методы, исходя из тестов
                    // если шк есть, получаем тесты
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
                                if (!TestsReader.IsDBNull(0))
                                {
                                    LISTestCode = TestsReader.GetString(0);

                                    //ExchangeLog($"Test {LISTestCode} exist in request");

                                    // если тест NEUT# или NEUT%, то есть задание на DIFF
                                    if ((LISTestCode == "Г0075") || (LISTestCode == "Г0080"))
                                    {
                                        DiffExists = true;
                                        ExchangeLog($"Test {LISTestCode} (NEUT#/%) exist in request");
                                    }

                                    // если тест RET# или RET%, то есть задание на RET
                                    if ((LISTestCode == "Г0145") || (LISTestCode == "Г0150"))
                                    {
                                        RetExists = true;
                                        ExchangeLog($"Test {LISTestCode} (RET#/%) exist in request");
                                    }

                                    // если тест NRBC# или NRBC%, то есть задание на NRBC
                                    if ((LISTestCode == "Г0180") || (LISTestCode == "Г0185"))
                                    {
                                        NrbcExists = true;
                                        ExchangeLog($"Test {LISTestCode} (NRBC#/%) exist in request");
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
                    #endregion

                    CGMconnection.Close();
                }

                // определяем Test method
                #region определяем Test method
                if (DiffExists && !RetExists && !NrbcExists)
                {
                    TestMode = "CBC+DIFF";
                    //ExchangeLog("TestMode: " + TestMode);
                }

                if (DiffExists && RetExists && !NrbcExists)
                {
                    TestMode = "CBC+DIFF+RET";
                    //ExchangeLog("TestMode: " + TestMode);
                }

                if (DiffExists && !RetExists && NrbcExists)
                {
                    TestMode = "CBC+DIFF+NRBC";
                    //ExchangeLog("TestMode: " + TestMode);
                }

                if (DiffExists && RetExists && NrbcExists)
                {
                    TestMode = "CBC+DIFF+RET+NRBC";
                    //ExchangeLog("TestMode: " + TestMode);
                }

                ExchangeLog("TestMode for execution: " + TestMode);

                #endregion

                // Если ШК существует, то отправляем задание прибору
                if (RIDExists)
                {
                    FullName = PatientSurname + '^' + PatientName;
                    // отправляем прибору задание и демографию пациента
                    ORRSending(client_, utf8, id, RIDPar, PID, FullName, PatientBirthDay, PatientSex, SampleDate, TestMode);
                }
                else
                {
                    ORRNFSending(client_, utf8, id);
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
                    string MessageComment = "";
                    string AllMessage = "";

                    // поолучаем список всех файлов в текущей папке
                    string[] Files = Directory.GetFiles(AnalyzerResultPath, "*.res");

                    // шаблоны регулярных выражений для поиска данных
                    string RIDPattern = @"OBR[|]\d+[|][|](?<RID>\d+)[|]\S*";
                    string TestPattern = @"OBX[|]\d+[|]NM[|]\S+[@](?<Test>\S+)[@]\S*";
                    string ResultPattern = @"OBX[|]\d+[|]NM[|]\S+[|](?<Result>\d+[,.]?\d*)[|]\S+";
                    string CommentPattern = @"OBX[|]\d+[|]IS[|]\S+[@](?<Comment>\D+)[@]\S*[|]{2}[T]\S+";

                    Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex TestRegex = new Regex(TestPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex ResultRegex = new Regex(ResultPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                    Regex CommentRegex = new Regex(CommentPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));

                    // пробегаем по файлам
                    foreach (string file in Files)
                    {
                        FileResultLog(file);
                        string[] lines = System.IO.File.ReadAllLines(file);
                        string RID = "";
                        string Test = "";
                        //string Result = "";
                        string Comment = "";

                        // обрезаем только имя текущего файла
                        string FileName = file.Substring(AnalyzerResultPath.Length + 1);
                        // название файла .ок, который должен создаваться вместе с результирующим для обработки службой FileGetterService
                        string OkFileName = "";

                        // проходим по строкам в файле
                        foreach (string line in lines)
                        {
                            // заменяем птички ^ на @, иначе регулярное врыажение некорректно работает
                            string line_ = line.Replace("^", "@");
                            Match RIDMatch = RIDRegex.Match(line_);
                            Match TestMatch = TestRegex.Match(line_);
                            Match ResultMatch = ResultRegex.Match(line_);
                            Match CommentMatch = CommentRegex.Match(line_);

                            // поиск RID в строке
                            if (RIDMatch.Success)
                            {
                                RID = RIDMatch.Result("${RID}");
                                FileResultLog($"Заявка № {RID}");
                                MessageHead = $"O|1|{RID}||ALL|R|20230101000100|||||X||||ALL||||||||||F";
                            }
                            else
                            {
                                //Console.WriteLine("RID не найден в строке");
                                //FileResultLog("RID не найден");
                                //FileToErrorPath = true;
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
                                    //FileResultLog($"PSMV2 код: {PSMTestCode}");
                                    //FileResultLog($"{Test} - результат: {Result}");

                                    if (PSMTestCode == "")
                                    {
                                        FileResultLog($"Код анализатора {Test} не интерпретирован в PSMV2 код.");
                                        FileResultLog($"{Test} - результат: {Result}");
                                    }
                                    else
                                    {
                                        FileResultLog($"PSMV2 код: {PSMTestCode}");
                                        FileResultLog($"{Test} - результат: {Result}");
                                    }
                                }


                                // если код тест был интерпретирован
                                if ((PSMTestCode != "") && (Result != ""))
                                {
                                    // формируем строку с ответом для результирующего файла
                                    MessageTest = MessageTest + $"R|1|^^^{PSMTestCode}^^^^{AnalyzerCode}|{Result}|||N||F||MIN6200^||20230101000001|{AnalyzerCode}" + "\r";
                                }
                            }

                            // если строка с комментарием
                            if (CommentMatch.Success)
                            {
                                Comment = CommentMatch.Result("${Comment}");
                                MessageComment = MessageComment + $"{Comment}; ";
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

                                if (MessageComment != "")
                                {
                                    AllMessage = AllMessage + "\r"
                                        + $"R|37|^^^0000^^^^{AnalyzerCode}|C|||N||F||^||20230101000001|{AnalyzerCode}"
                                        + "\r" + $"C|1|L|{MessageComment}|G";
                                }

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
                            catch (Exception ex)
                            {
                                FileResultLog(ex.ToString());
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

        #region TCP server
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
                                //client = socket.Accept();
                            }
                        }

                        else
                        {
                            // UTF8 encoder
                            Encoding utf8 = Encoding.UTF8;
                            // количество полученных байтов
                            int received_bytes = 0;
                            // буфер для получения данных
                            //byte[] received_data = new byte[1024];
                            //byte[] received_data = new byte[4096];
                            byte[] received_data = new byte[4608];
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

                            // Определяем тип сообщения, которое отправил прибор
                            #region Определение типа сообщения от прибора

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

                            // Тип сообщения ORM - запрос задания прибором
                            string ORMPattern = @"\S+[|](?<type>\w+)@O01[|]\w+[|]";
                            Regex ORMRegex = new Regex(ORMPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                            string ORM = "";

                            Match ORMMatch = ORMRegex.Match(messageMindray);

                            if (ORMMatch.Success)
                            {
                                ORM = ORMMatch.Result("${type}");
                                //ExchangeLog($"Message type: {ORM}");
                                //Console.WriteLine($"Message type: {ORM}");
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

                            // если прибор запрашивает задание - ORM
                            if (ORM == "ORM")
                            {
                                // шаблона для поиска RID в сообщении от прибора QRY
                                string RIDPattern = @"ORC[|]RF[|][|](?<RID>\d+)[|]*";
                                Regex RIDRegex = new Regex(RIDPattern, RegexOptions.None, TimeSpan.FromMilliseconds(150));
                                string RID = "";

                                Match RIDMatch = RIDRegex.Match(messageMindray);

                                if (RIDMatch.Success)
                                {
                                    RID = RIDMatch.Result("${RID}");
                                    ExchangeLog($"Request from LIS");
                                    ExchangeLog($"RID: {RID}");
                                }

                                // Првоеряем, есть ли RID, получаем задание из базы CGM и отправляем подтверждение прибору
                                GetRequestFromCGMDB(client, utf8, MessageId, RID);
                            }
                        }

                        Thread.Sleep(1000);
                    }
                }
            }
            catch (Exception error)
            {
                ServiceLog($"Exception: {error}");
            }
        }
        #endregion

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
