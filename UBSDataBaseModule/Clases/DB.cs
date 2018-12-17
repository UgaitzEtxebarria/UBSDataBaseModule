using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;
using System.Windows.Forms;

namespace UBSDataBaseModule.Clases
{
    public abstract class DB
    {
        private UBSLib.UBSBackgroundModule parent;
        private ConcurrentQueue<DBData> pilaComandos;
        private string connectionString;
        private string defaultConnectionString = "Data Source=.\\DB\\";

        Thread thrCambioProd;
        private System.Timers.Timer tmr;
        private bool cambiarDB;
        private bool cerrarDB;
        private ConcurrentQueue<SQLiteTransaction> pilaTransacciones;
        private SQLiteTransaction actualTransaction = null;

        /////////////////////////////////////////////////////////////////////////////

        public DB(string dbname, UBSLib.UBSBackgroundModule parent)
        {
            pilaComandos = new ConcurrentQueue<DBData>();
            this.parent = parent;
            cambiarDB = false;
            cerrarDB = false;

            string fecha = DateTime.Now.ToString().Replace('/', '-').Replace(':', '.');

            connectionString = "Data Source=.\\BD\\" + dbname + " (" + fecha + ").db;Version=3; PRAGMA synchronous=OFF;";

            SQLiteConnection.CreateFile(".\\BD\\" + dbname + " (" + fecha + ").db");

            SQLiteConnection connection = new SQLiteConnection(connectionString);

            pilaTransacciones = new ConcurrentQueue<SQLiteTransaction>();

            try
            {
                connection.Open();

                DateTime t1 = DateTime.Now;
                parent.Log("Creacion de las tablas de la base de datos iniciada : " + t1);

				List <string> SQLQueries = new List<string>();
				CreateTables(out SQLQueries);

				SQLiteCommand command = new SQLiteCommand();
				foreach (string query in SQLQueries)
				{
					command = new SQLiteCommand(query, connection);
					command.ExecuteNonQuery();
				}

				parent.Log("Creacion de las tablas finalizada : +" + (DateTime.Now - t1).TotalMilliseconds);
                connection.Close();

                tmr = new System.Timers.Timer();
                tmr.Interval = 5000;
                tmr.Elapsed += ActivarGuardado;
                tmr.Enabled = true;
                tmr.Start();
            }
            catch (Exception e)
            {
                parent.Error("Error al crear la base de datos. " + e.Message, true, false);
            }
            finally
            {
                connection.Dispose();
            }

            thrCambioProd = new Thread(() => EjecutorComandos());
            thrCambioProd.Name = "DB";
            thrCambioProd.IsBackground = true;
            thrCambioProd.Start();
        }

		//////////////////////////////////////////////////////////////////////////////////

		protected abstract void CreateTables(out List<string> SQLQueries);

		//////////////////////////////////////////////////////////////////////////////////

		private void EjecutorComandos()
        {
            SQLiteConnection connection;
            DBData dataDB = null;
            while (!cerrarDB || (cerrarDB && (pilaComandos.Count > 0)))
            {
                cambiarDB = false;
                //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Intento de leer pila, objetos en la pila: " + pilaComandos.Count, false);
                if (pilaComandos.Count > 0)
                {
                    if (pilaComandos.TryPeek(out dataDB))
                    {
                        //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Nueva orden recibida, objetos en la pila: " + pilaComandos.Count, false);
                        connection = new SQLiteConnection(dataDB.Connection);
                        try
                        {
                            connection.Open();

                            actualTransaction = connection.BeginTransaction();
                            while (!cambiarDB)
                            {
                                DateTime t1 = DateTime.Now;
                                if (pilaComandos.TryPeek(out dataDB))
                                {
                                    if (dataDB.Connection != connection.ConnectionString)
                                    {
                                        cambiarDB = true;
                                        break;
                                    }
                                    //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Nueva orden valida, objetos en la pila: " + pilaComandos.Count, false);

                                    if (pilaComandos.TryDequeue(out dataDB))
                                    {
                                        using (var command = new SQLiteCommand(connection))
                                        {
                                            //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden " + dataDB.Command + " recibida en " + t1.ToString("HH:mm:ss.FFF") + ", objetos en la pila: " + pilaComandos.Count, false);
                                            command.CommandText = dataDB.Command;
                                            if (!dataDB.Command.StartsWith("INSERT") && !dataDB.Command.StartsWith("UPDATE") && !dataDB.Command.StartsWith("DELETE"))
                                            {
                                                List<string> Selected = new List<string>();
                                                DbDataReader reader = command.ExecuteReader();

                                                //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden " + dataDB.Command + " enviada en " + (DateTime.Now - t1).TotalMilliseconds + " ms, objetos en la pila: " + pilaComandos.Count, false);

                                                while (reader.Read())
                                                {
													object[] objs = new object[reader.FieldCount];
                                                    reader.GetValues(objs);

                                                    foreach (object obj in objs)
                                                        Selected.Add(obj.ToString());
                                                }
                                                //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden " + dataDB.Command + " datos conseguidos en " + (DateTime.Now - t1).TotalMilliseconds + " ms, objetos en la pila: " + pilaComandos.Count, false);
                                                dataDB.Data = string.Join("#", Selected.ToArray());
                                                ((DBModule)parent).DBResults[dataDB.Variable] = dataDB;
                                                dataDB.Readed = true;
                                                //parent.WriteConsole("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Dato guardado: " + dataDB.Command + " en " + (DateTime.Now - t1).TotalMilliseconds + " ms, objetos en la pila: " + pilaComandos.Count, true);
                                                //parent.SetGlobalParameter(dataDB.Variable, dataDB);
                                                //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden " + dataDB.Command + " datos globales guardados en " + (DateTime.Now - t1).TotalMilliseconds + " ms, objetos en la pila: " + pilaComandos.Count, false);
                                            }
                                            else
                                                command.ExecuteNonQuery();

                                            /*if (dataDB != null)
                                                parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden " + dataDB.Command + " completada en " + (DateTime.Now - t1).TotalMilliseconds + " ms, objetos en la pila: " + pilaComandos.Count, false);*/
                                        }
                                    }
                                }
                            }
                            //connection.Close();
                        }
                        catch (Exception e)
                        {
                            parent.WriteConsole("Alarma 10002 activada" + ((dataDB.Command != null) ? (" con la orden " + dataDB.Command) : "") + " por: " + e.Message, true);
                        }
                        finally
                        {
                            //connection.Dispose();
                        }
                    }
                }
                if (cambiarDB && actualTransaction.Connection != null)
                {
                    //Cerrar BD en otro hilo ya que tarda mas de lo esperado.
                    Thread thrCerrarDB = new Thread(() => CerrarBaseDeDatos(actualTransaction));
                    thrCerrarDB.Name = "closeDB";
                    thrCerrarDB.IsBackground = true;
                    thrCerrarDB.Start();
                    //parent.Log("(" + DateTime.Now.ToString("HH:mm:ss.FFF") + ") Orden mandada a commitear en " + DateTime.Now.ToString("HH:mm:ss.FFF") + ", objetos en la pila: " + pilaComandos.Count, false);
                }
                Thread.Sleep(1);
            }
        }

        //////////////////////////////////////////////////////////////////////////////////

        private void ActivarGuardado(object sender, EventArgs e)
        {
            if (Thread.CurrentThread.Name == null)
                Thread.CurrentThread.Name = "Guardador de BD";
            cambiarDB = true;
            tmr.Stop();
            tmr.Enabled = true;
            tmr.Start();
        }

        //////////////////////////////////////////////////////////////////////////////////

        public void Destroy()
        {
            cerrarDB = true;
            thrCambioProd.Join();
        }

        //////////////////////////////////////////////////////////////////////////////////

        private void CerrarBaseDeDatos(SQLiteTransaction transaction)
        {
            SQLiteTransaction dequeued = null;
            try
            {
                //parent.WriteConsole("Cantidad en la fila de transacciones: " + pilaTransacciones.Count, true);
                if (transaction != null)
                    pilaTransacciones.Enqueue(transaction);
                else
                    pilaTransacciones.Enqueue(actualTransaction);

                while (pilaTransacciones.Count > 0)
                {
                    if (pilaTransacciones.TryDequeue(out dequeued))
                        transaction.Commit();
                }
            }
            catch (Exception e)
            {
                parent.Error("Error al cerrar la base de datos. " + e.Message, true, false);
            }
        }

        //////////////////////////////////////////////////////////////////////////////////
        private void añadirPila(string command, string nameDB, string variable)
        {
            try
            {
                string connection = "";

                if (nameDB == "")
                    connection = connectionString;
                else
                    connection = defaultConnectionString + nameDB + ".db;Version=3;";

                DBData result = new DBData(connection, command, variable);

                añadirPila(result);
            }
            catch (Exception e)
            {
                parent.Error("Error al lanzar una llamada select en la base de datos. " + e.Message, true, false);
            }
        }

        //////////////////////////////////////////////////////////////////////////////////
        public void añadirPila(DBData dataDB)
        {
            if (!cerrarDB)
            {
                try
                {
                    if (dataDB.Variable != "")
                    {
                        Stopwatch sw = new Stopwatch();

                        sw.Start();
                        while (((DBModule)parent).DBResults.ContainsKey(dataDB.Variable) && sw.Elapsed.TotalMilliseconds < 100)
                        {
                            parent.Log("DB => la variable " + dataDB.Variable + " ya existe, en espera...");
                            Application.DoEvents();
                            Thread.Sleep(1);
                        }
                        sw.Stop();

                        if (((DBModule)parent).DBResults.ContainsKey(dataDB.Variable))
                            parent.Error("Una peticion se ha quedado residual.", true, false);
                        else
                            //parent.WriteConsole("Añadiendo a la pila de BD: " + dataDB.Command, true);
                            ((DBModule)parent).DBResults.Add(dataDB.Variable, dataDB);
                    }
                }
                catch (Exception e)
                {
                    parent.Error("Error al crear el pedido en la base de datos. " + e.Message, true, false);
                }

                try
                {
                    pilaComandos.Enqueue(dataDB);
                }
                catch (Exception e)
                {
                    parent.Error("Error al añadir el pedido en base de datos. " + e.Message, true, false);
                }
            }
        }

        //////////////////////////////////////////////////////////////////////////////////
        public bool insert(string Tabla, string Variables, string Valores)
        {
            return insert(Tabla, Variables, Valores, "");
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool insert(string Tabla, string Variables, string Valores, string nameDB)
        {
            if (Variables.Split(',').Length != Valores.Split(',').Length) return false;
            string str = "INSERT INTO " + Tabla + " (" + Variables + ") VALUES (" + Valores + ")";

            añadirPila(str, nameDB, "");

            return true;
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool update(string Tabla, string Set, string Condition)
        {
            return update(Tabla, Set, Condition, "");
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool update(string Tabla, string Set, string Condition, string nameDB)
        {
            string str = "UPDATE " + Tabla + " SET " + Set + " WHERE " + Condition;

            añadirPila(str, nameDB, "");

            return true;
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool delete(string Tabla, string Condition)
        {
            return delete(Tabla, Condition, "");
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool delete(string Tabla, string Condition, string nameDB)
        {
            string str = "DELETE FROM " + Tabla + " WHERE " + Condition;
            añadirPila(str, nameDB, "");
            return true;
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool select(string Tabla, string Variables, string where, string returnVar)
        {
            return select(Tabla, Variables, where, "", returnVar);
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool select(string Tabla, string Variables, string where, string nameDB, string returnVar)
        {
            try
            {
                string str = "select " + Variables + " from " + Tabla + ((where != "") ? " where " + where : "");

                añadirPila(str, nameDB, returnVar);
                return true;
            }
            catch (Exception e)
            {
                parent.Error("Error al lanzar una llamada select en la base de datos. " + e.Message, true, false);
                return false;
            }

        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool free(string text, string returnVar)
        {
            return free(text, "", returnVar);
        }

        //////////////////////////////////////////////////////////////////////////////////

        public bool free(string text, string nameDB, string returnVar)
        {
            añadirPila(text, nameDB, returnVar);
            return true;
        }
    }
}
