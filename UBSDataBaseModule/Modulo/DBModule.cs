using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Windows.Forms;
using UBSDataBaseModule.Clases;

namespace UBSDataBaseModule
{
    public class DBData
    {
        public DBData(string connection, string command, string variable)
        {
            Connection = connection;
            Command = command;
            Variable = variable;
            Data = "";
            Readed = false;
			Readed = true;
        }

        public string Connection { get; set; }

        public string Command { get; set; }

        public string Variable { get; set; }

        public string Data { get; set; }

        public bool Readed { get; set; }
    }

    public class DBModule : UBSLib.UBSBackgroundModule
    {
        private DB db;
        private Dictionary<string, DBData> results;

        private object lockDB;

        //////////////////////////////////////////////////////////////////////////////////
        public DBModule(string _id)
            : base(_id)
        { }


        //////////////////////////////////////////////////////////////////////////////////
        public override bool Init()
        {
            base.Init();
            db = new DB("Dummy", this);
            results = new Dictionary<string, DBData>();

            lockDB = new object();

            WriteConsole("Módulo cargado correctamente.");
            return true;
        }

        //////////////////////////////////////////////////////////////////////////////////
        public override bool Destroy()
        {
            db.Destroy();
            WriteConsole("Apagado de la base de datos: " + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.FFF"), true);
            return true;
        }

        //////////////////////////////////////////////////////////////////////////////////
        public override void HandleMessages(UBSLib.UBSMessage message)
        {
            string strMessage = message.ToString();

            //Mensajes
            string[] strParams = strMessage.Split('#');

            if (strMessage.StartsWith("insert#"))
            {
                try
                {
                    string Tabla = strParams[1];
                    string vars = strParams[2];
                    string vals = strParams[3];

                    if (strParams.Length == 4)
                    {
                        if (!db.insert(Tabla, vars, vals))
                            Error("No se ha podido llevar a cabo el proceso Insert en la base de datos.", true, false);
                    }
                    else
                    {
                        string DB = strParams[4];
                        if (!db.insert(Tabla, vars, vals, DB))
                            Error("No se ha podido llevar a cabo el proceso Insert en la base de datos.", true, false);
                    }
                }
                catch (Exception e)
                {
                    Error("La estructura de la llamada para el proceso Insert está mal estructurada. " + e.Message, true, false);
                }
            }

            if (strMessage.StartsWith("update#"))
            {
                try
                {
                    string Tabla = strParams[1];
                    string sets = strParams[2];
                    string cond = strParams[3];

                    if (!db.update(Tabla, sets, cond))
                        Error("No se ha podido llevar a cabo el proceso Update en la base de datos.", true, false);
                }
                catch (Exception e)
                {
                    Error("La estructura de la llamada para el proceso Update está mal estructurada. " + e.Message, true, false);
                }
            }

            if (strMessage.StartsWith("delete#"))
            {
                try
                {
                    string Tabla = strParams[1];
                    string cond = strParams[2];

                    if (strParams.Length == 3)
                    {
                        if (!db.delete(Tabla, cond))
                            Error("No se ha podido llevar a cabo el proceso Delete en la base de datos.", true, false);
                    }
                    else
                    {
                        string DB = strParams[3];
                        if (!db.delete(Tabla, cond, DB))
                            Error("No se ha podido llevar a cabo el proceso Delete en la base de datos.", true, false);
                    }
                }
                catch (Exception e)
                {
                    Error("La estructura de la llamada para el proceso Delete está mal estructurada. " + e.Message, true, false);
                }
            }

            if (strMessage.StartsWith("select#"))
            {
                try
                {
                    string Variable = strMessage;
                    string Tabla = strParams[1];
                    string vars = strParams[2];
                    string where = strParams[3];

                    List<string> listVal = new List<string>();

                    if (strParams.Length == 4)
                    {
                        if (!db.select(Tabla, vars, where, Variable))
                            Error("No se ha podido llevar a cabo el proceso Select en la base de datos.", true, false);
                    }
                    else
                    {
                        string DB = strParams[4];
                        if (!db.select(Tabla, vars, where, DB, Variable))
                            Error("No se ha podido llevar a cabo el proceso Select en la base de datos.", true, false);
                    }
                }
                catch (Exception e)
                {
                    Error("La estructura de la llamada para el proceso Select está mal estructurada. " + e.Message, true, false);
                }
            }

            if (strMessage.StartsWith("free#"))
            {
                try
                {
                    List<string> listVal = new List<string>();
                    string Variable = strMessage;
                    string text = strParams[1];

                    if (strParams.Length == 3)
                    {
                        string DB = strParams[2];
                        if (!db.free(text, DB, Variable))
                            Error("No se ha podido llevar a cabo el texto libre en la base de datos.", true, false);
                    }
                    else if (strParams.Length == 2)
                    {
                        if (!db.free(text, Variable))
                            Error("No se ha podido llevar a cabo el texto libre en la base de datos.", true, false);
                    }
                    else
                    {
                        Error("Los parametros de la llamada a la base de datos no concuerdan.", true, false);
                    }
                }
                catch (Exception e)
                {
                    Error("La estructura de la llamada a la base de datos de forma libre está mal estructurada. " + e.Message, true, false);
                }
            }

        }

        //////////////////////////////////////////////////////////////////////////////////


        public string getData(string Variable)
        {
            try
            {
                string result = "";
                int intentos = 0;
                bool found = false;
                DateTime t1 = DateTime.Now;

                if (results.ContainsKey(Variable))
                {
                    while (!found)
                    {
                        Stopwatch sw = new Stopwatch();

                        sw.Start();
                        DBData dataDB = results[Variable];
                        bool auxReaded = false;
                        while (!auxReaded && sw.Elapsed.Seconds < 2)
                        {
                            auxReaded = dataDB.Readed;

                            if (auxReaded)
                                break;
                            Application.DoEvents();
                            Thread.Sleep(1);
                        }
                        Log("Conseguir el dato " + Variable + " de la BD: " + sw.Elapsed.TotalMilliseconds);
                        sw.Stop();

                        result = results[Variable].Data;

                        results.Remove(Variable);

                        if (!auxReaded && intentos < 3)
                        {
                            db.añadirPila(dataDB);
                            intentos++;
                            WriteConsole("No se ha podido leer el dato " + Variable + " de la base de datos, reenviando la orden " + dataDB.Command + " (" + intentos + "/3)", true);
                        }
                        else
                            found = true;
                    }
                }
                else
                    Error("No se encuentra el query " + Variable, true, false);
                return result;
            }
            catch (Exception e)
            {
                Error("Error al recibir el resultado de la base de datos. " + e.Message, true, false);
                return null;
            }
        }

        public Dictionary<string, DBData> DBResults
        {
            get
            {
                lock (lockDB)
                {
                    return results;
                }
            }
        }
    }
}

