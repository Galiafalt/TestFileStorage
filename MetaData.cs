using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.IO;
using System.Collections.Concurrent;


namespace SUBD
{
    class FileSUBD
    {
        private Socket SUBDServer = null;
        private Thread ServerThread = null;
        private Thread DataWriter = null;
        private MetaData Meta = null;
        private ConcurrentQueue<DataWrite> WriteQueue = new ConcurrentQueue<DataWrite>();
        private String CurrentDBFile;

        private Int32 port = 45054;

        public FileSUBD()
        {
            Start();
        }

        public FileSUBD(Int32 StartPort)
        {
            this.port = StartPort;
            Start();
        }

        private void Start()
        {
            DirectoryInfo NeedDirectory = new DirectoryInfo("Database\\DataFiles");
            if (!NeedDirectory.Exists)
                NeedDirectory.Create();
            FileInfo[] DBFiles = NeedDirectory.GetFiles("Db*.fsf");
            if (DBFiles.Length == 0)
            {
                using (FileStream NewFile = File.Create(NeedDirectory.FullName + "\\Db0.fsf")) { }
                CurrentDBFile = NeedDirectory.FullName + "\\Db0.fsf";
            }
            else
            {
                CurrentDBFile = NeedDirectory.FullName + "\\" + DBFiles.Max(r => r.Name);
            }

            Meta = new MetaData();
            ServerThread = new Thread(ServerStart);
            ServerThread.Start();
            DataWriter = new Thread(WriteDataToFile);
            DataWriter.Start(); 
        }

        private void ServerStart()
        {
            IPEndPoint ServerIPEnd = new IPEndPoint(IPAddress.Any, port);
            SUBDServer = new Socket(IPAddress.Any.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            SUBDServer.LingerState = new LingerOption(true, 10);
            SUBDServer.ReceiveBufferSize = 1048576;
            SUBDServer.ExclusiveAddressUse = true;
            try
            {
                SUBDServer.Bind(ServerIPEnd);
                SUBDServer.Listen(100000);
                Console.WriteLine("TCP server is runing");
            }
            catch (ProtocolViolationException e)
            {
                throw new Exception("Сервер не запущен. Ошибка сетевого протокола: " + e.Message);
            }
            catch (SocketException e)
            {
                throw new Exception("Сервер не запущен. Ошибка сокета: " + e.Message);
            }
            catch (Exception e)
            {
                throw new Exception("Сервер не запущен. Ошибка: " + e.Message);
            }
            while (true)
            {
                Socket Client = SUBDServer.Accept();
                ThreadPool.QueueUserWorkItem(new WaitCallback(NewClient), Client);
                
            }
        }

        private void NewClient(Object Client)
        {
            Socket SUBDClient = (Socket)Client;
            SUBDClient.ReceiveBufferSize = 1048900;
            SUBDClient.SendBufferSize = 1048900;

            
            //Console.WriteLine("Client connected");
            while (SUBDClient.Connected)
            {
                //NetworkStream ClientData = SUBDClient.GetStream();
                if (SUBDClient.Available != 0 )
                {
                    byte[] OperationByte = new byte[1];
                    SUBDClient.Receive(OperationByte, 0, 1, SocketFlags.Partial);
                    int Operation = (int)OperationByte[0];
                    switch (Operation)
                    {
                        case 0:
                            
                            byte[] SearchIdentyByte = new byte[10];
                            byte[] SearchTypeByte = new byte[10];
                            byte[] SearchFromByte = new byte[8];
                            byte[] SearchTillByte = new byte[8];
                            bool isIdenty = false;
                            bool isType = false;
                            bool isFromTill = false;                            
                            while (SUBDClient.Available != 0)
                            {
                                byte[] ParamertByte = new byte[1];
                                SUBDClient.Receive(ParamertByte, 0, 1, SocketFlags.Partial);
                                int Paramert = (int)ParamertByte[0];
                                switch (Paramert)
                                {
                                    case 0:
                                        SUBDClient.Receive(SearchIdentyByte, 0, 10, SocketFlags.Partial);
                                        isIdenty = true;
                                        break;
                                    case 1:
                                        SUBDClient.Receive(SearchTypeByte, 0, 10, SocketFlags.Partial);
                                        isType = true;
                                        break;
                                    case 2:
                                        SUBDClient.Receive(SearchFromByte, 0, 8, SocketFlags.Partial);
                                        SUBDClient.Receive(SearchTillByte, 0, 8, SocketFlags.Partial);
                                        isFromTill = true;
                                        break;
                                    default:
                                        SUBDClient.Send(Encoding.UTF8.GetBytes("Неверный маркет параметра поиска"));
                                        break;
                                }
                            }
                            var SearchResult = this.Meta.Select(r => r);
                            if (isIdenty)
                            {
                                String SearchIdenty = Encoding.UTF8.GetString(SearchIdentyByte).Trim('\0');
                                SearchResult = SearchResult.Where(r => r.Identy == SearchIdenty);
                            }
                            if (isType)
                            {
                                String SearchType = Encoding.UTF8.GetString(SearchTypeByte).Trim('\0');
                                SearchResult = SearchResult.Where(r => r.Type == SearchType);
                            }
                            if (isFromTill)
                            {
                                DateTime SearchFrom = new DateTime(BitConverter.ToInt64(SearchFromByte, 0));
                                DateTime SearchTill = new DateTime(BitConverter.ToInt64(SearchTillByte, 0));
                                SearchResult = SearchResult.Where(r => r.ReceiveTime >= SearchFrom && r.ReceiveTime <= SearchTill);
                            }
                            int i = 1;
                            foreach (SingleMetaData OneMeta in SearchResult)
                            {                                
                                int LengthData = OneMeta.EndOffset - OneMeta.StartOffset-1;
                                byte[] SendRec = new byte[32 + LengthData];                                
                                Array.Copy(BitConverter.GetBytes(OneMeta.ReceiveTime.Ticks), 0, SendRec, 0, 8);
                                Array.Copy(Encoding.UTF8.GetBytes(OneMeta.Identy), 0, SendRec, 8, Encoding.UTF8.GetByteCount(OneMeta.Identy));
                                Array.Copy(Encoding.UTF8.GetBytes(OneMeta.Type), 0, SendRec, 18, Encoding.UTF8.GetByteCount(OneMeta.Type));
                                Array.Copy(BitConverter.GetBytes(LengthData), 0, SendRec, 28, 4);
                                FileInfo MetaFile = new FileInfo(OneMeta.StoreFile);
                                using (FileStream ReadFile = MetaFile.Open(FileMode.OpenOrCreate, FileAccess.Read))
                                {
                                    ReadFile.Seek(OneMeta.StartOffset, SeekOrigin.Begin);
                                    ReadFile.Read(SendRec, 32, LengthData);
                                }
                                
                                SUBDClient.Send(SendRec);
                            }             
                            break;
                        case 1:
                            byte[] IdentyByte = new byte[10];
                            SUBDClient.Receive(IdentyByte, 0, 10, SocketFlags.Partial);

                            byte[] TypeByte = new byte[10];
                            SUBDClient.Receive(TypeByte, 0, 10, SocketFlags.Partial);

                            byte[] DataLength = new byte[4];
                            SUBDClient.Receive(DataLength, 0, 4, SocketFlags.Partial);
                            int ClientData = BitConverter.ToInt32(DataLength, 0);
                            while (SUBDClient.Available < ClientData) { };
                            byte[] Data = new byte[ClientData];
                            SUBDClient.Receive(Data, 0, ClientData, SocketFlags.Partial);

                            DataWrite NewData = new DataWrite();
                            NewData.ReceiveTime = DateTime.Now;
                            NewData.Identy = Encoding.UTF8.GetString(IdentyByte);
                            NewData.Type = Encoding.UTF8.GetString(TypeByte);
                            NewData.Data = new byte[ClientData];
                            Data.CopyTo(NewData.Data, 0);
                            WriteQueue.Enqueue(NewData);
                            break;
                        default:
                            SUBDClient.Send(Encoding.UTF8.GetBytes("Неверный маркер операции (запись/поиск) с БД"));
                            break;
                    }
                }
                SUBDClient.Shutdown(SocketShutdown.Both);
                SUBDClient.Close(10);
            }            
        }

        public void WriteDataToFile()
        {
            while (true)
            {
                if (!WriteQueue.IsEmpty)
                {
                    DataWrite OneData = null;
                    if (WriteQueue.TryDequeue(out OneData))
                    {
                        SingleMetaData NewMeta = new SingleMetaData();
                        FileInfo DBFile = new FileInfo(CurrentDBFile);
                        while (DBFile.Length + OneData.Data.Length > int.MaxValue)
                        {
                            CreateNewDBFile();
                            DBFile = new FileInfo(CurrentDBFile);
                        }
                        using (FileStream WriteFile = DBFile.Open(FileMode.Append, FileAccess.Write, FileShare.Read))
                        {
                            NewMeta.ReceiveTime = OneData.ReceiveTime;
                            NewMeta.Identy = OneData.Identy;
                            NewMeta.Type = OneData.Type;
                            NewMeta.StoreFile = CurrentDBFile;
                            NewMeta.StartOffset = (int)DBFile.Length;
                            NewMeta.EndOffset = NewMeta.StartOffset + OneData.Data.Length - 1;
                            WriteFile.Write(OneData.Data, 0, OneData.Data.Length);
                            this.Meta.AddMeta(NewMeta);
                            WriteFile.Flush();
                            WriteFile.Close();
                        }
                    }
                }
                Thread.Sleep(1);
            }
        }

        public Boolean isRuning()
        {
            return SUBDServer.Connected;
        }

        private void CreateNewDBFile()
        {
            DirectoryInfo CurrentDirectory = new DirectoryInfo(Path.GetDirectoryName(CurrentDBFile));
            FileInfo[] DBFileCount = CurrentDirectory.GetFiles("Db*.fsf");
            using (FileStream NewFile = File.Create(CurrentDirectory.FullName + "\\" + String.Format("Db{0}.fsf", DBFileCount.Length))) {}
            CurrentDBFile = CurrentDirectory.FullName + "\\" + String.Format("Db{0}.fsf", DBFileCount.Length);
        }

        class DataWrite
        {
            internal DateTime ReceiveTime;
            internal String Identy;
            internal String Type;
            internal byte[] Data;
        }

        class SingleMetaData
        {
            internal DateTime ReceiveTime;
            internal String Identy;
            internal String Type;
            internal String StoreFile;
            internal int StartOffset;
            internal int EndOffset;

            internal byte[] ToBytes()
            {
                int StoreLength = Encoding.UTF8.GetByteCount(this.StoreFile);
                byte[] Result = new byte[40+StoreLength];
                Array.Copy(BitConverter.GetBytes(this.ReceiveTime.Ticks), 0, Result, 0, 8);
                Array.Copy(Encoding.UTF8.GetBytes(this.Identy), 0, Result, 8, 10);
                Array.Copy(Encoding.UTF8.GetBytes(this.Type), 0, Result, 18, 10);
                Array.Copy(BitConverter.GetBytes(StoreLength), 0, Result, 28, 4);
                Array.Copy(Encoding.UTF8.GetBytes(this.StoreFile), 0, Result, 32, StoreLength);
                Array.Copy(BitConverter.GetBytes(this.StartOffset), 0, Result, 32 + StoreLength, 4);
                Array.Copy(BitConverter.GetBytes(this.EndOffset), 0, Result, 36 + StoreLength, 4);
                return Result;
            }
        }

        class MetaData : HashSet<SingleMetaData>
        {

            public MetaData()
            {
                FileInfo MetaFile = new FileInfo("Database\\meta.info");
                using (FileStream ReadFile = MetaFile.Open(FileMode.OpenOrCreate, FileAccess.Read))
                {
                    while (ReadFile.Position != MetaFile.Length)
                    {
                        byte[] DateByte = new byte[8];  
                        ReadFile.Read(DateByte, 0, 8);

                        byte[] IdentyByte = new byte[10];
                        ReadFile.Read(IdentyByte, 0, 10);

                        byte[] TypeByte = new byte[10];
                        ReadFile.Read(TypeByte, 0, 10);

                        byte[] FileLenByte = new byte[4];
                        ReadFile.Read(FileLenByte, 0, 4);

                        byte[] FileNameByte = new byte[BitConverter.ToInt32(FileLenByte, 0)];
                        ReadFile.Read(FileNameByte, 0, FileNameByte.Length);

                        byte[] StartByte = new byte[4];
                        ReadFile.Read(StartByte, 0, 4);

                        byte[] EndByte = new byte[4];
                        ReadFile.Read(EndByte, 0, 4);

                        SingleMetaData OneMeta = new SingleMetaData();
                        OneMeta.ReceiveTime = new DateTime(BitConverter.ToInt64(DateByte, 0));
                        OneMeta.Identy = Encoding.UTF8.GetString(IdentyByte).Trim('\0');
                        OneMeta.Type = Encoding.UTF8.GetString(TypeByte).Trim('\0');
                        OneMeta.StoreFile = Encoding.UTF8.GetString(FileNameByte).Trim('\0');
                        OneMeta.StartOffset = BitConverter.ToInt32(StartByte, 0);
                        OneMeta.EndOffset = BitConverter.ToInt32(EndByte, 0);
                        this.Add(OneMeta);
                    }
                }
            }

            public void Find()
            {

            }

            public void AddMeta(SingleMetaData SingleMeta)
            {
                this.Add(SingleMeta);

                FileInfo MetaFile = new FileInfo("Database\\meta.info");
                using (FileStream WriteFile = MetaFile.Open(FileMode.Append, FileAccess.Write, FileShare.Read))
                {
                    WriteFile.Write(SingleMeta.ToBytes(), 0, SingleMeta.ToBytes().Length);
                    WriteFile.Flush();
                    WriteFile.Close();
                }
            }
        }
    }
}
