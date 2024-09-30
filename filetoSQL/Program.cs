using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Net.Configuration;
using System.Text.RegularExpressions;


namespace filetoSQL
{
    internal class Program
    {
        const int versionMajor = 1;
        const int versionMinor = 0;
        const int versionRevision = 4;
        static string logFilePath = @"fileToSQL_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + ".txt";
        static bool RUN_VERBOSE = false;
        static bool FORCE_RUN = false;

        public static void WriteLog(string logMessage)
        {
            try
            {
                // Create a new file if it doesn't exist, or append to the existing file
                using (StreamWriter writer = File.AppendText(logFilePath))
                {
                    // Write the log entry with a timestamp
                    writer.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {logMessage}");
                }
            }
            catch {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write("ERROR: ");
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.Write("\'WriteLog\' ");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("(Error creating or writing to Log file)");
                Console.ForegroundColor = ConsoleColor.White;
            }
        }

        static void PrintError(string name, string error, string detail)
        {
            WriteLog("E: " + error + ": " + "\'" + name + "\' " + "(" + detail + ")");

            Console.ForegroundColor = ConsoleColor.Red;
            Console.Write(error + ": ");
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write("\'" + name + "\' ");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("(" + detail + ")");
            Console.ForegroundColor = ConsoleColor.White;
        }

        static void PrintInfo(string str1, string str2)
        {
            WriteLog("I: " + str1 + " " + str2);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write(str1);
            Console.Write(" ");
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(str2);
            Console.ForegroundColor = ConsoleColor.White;
        }

        static void PrintWarning(string str1, string str2)
        {
            WriteLog("W: " + str1 + " " + str2);

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.Write(str1);
            Console.Write(" ");
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine(str2);
            Console.ForegroundColor = ConsoleColor.White;
        }

        static void PrintHeader(string status)
        {
            WriteLog("H: " + status);

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(status);
            Console.ForegroundColor = ConsoleColor.White;
        }

        static void PrintOkStatus(string hdr)
        {
            WriteLog("      ... " + hdr + " OK");

            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write("[");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write(hdr + " OK");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine("]");
        }

        static void PrintErrorStatus(string hdr, string msg = null)
        {
            WriteLog("      ... " + hdr + " ERROR (" + msg + ")");

            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write("[");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Write(hdr + " ERROR");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write("]");
            if (msg != null)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write(" (");
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.Write(msg);
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Write(")");
            }
            Console.WriteLine();
        }

        static string GetOperation(string query)
        {
            string op = string.Empty;
            if (query.Contains("ALTER"))
            {
                op = "ALTER";
            }
            else if (query.Contains("UPDATE"))
            {
                op = "UPDATE";
            }
            else if (query.Contains("INSERT"))
            {
                op = "INSERT";
            }
            else if (query.Contains("CREATE"))
            {
                op = "CREATE";
            }
            else if (query.Contains("DROP"))
            {
                op = "DROP";
            }
            else
            {
                op = "UNKNOWN";
            }
            return op;
        }
         
        static void Usage()
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"Run SQL from file to a database.");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"Usage: ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("\tFileToDB");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" [-v] ");
            Console.Write(" [-f] ");
            Console.WriteLine();
            Console.Write(" -s ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("server");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" -d ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("database");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" -u ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("user");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" -p ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("password");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(" filename");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("\tFileToDB");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" [--verbose] ");
            Console.Write(" [--force] ");
            Console.Write(" --server ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("server");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" --database ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("database");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" --user ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("user");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" --password ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("password");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(" filename");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("\tFileToDB");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" -h");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("\tFileToDB");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write(" --help");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Options:");
            Console.WriteLine("\t-v | --verbose\tBe more verbose about it (optional).");
            Console.WriteLine("\t-f | --force\tForce update (optional). Otherwise only update dealer changes.");
            Console.WriteLine("\t-s | --server\tHostname (or IP) of the database server.");
            Console.WriteLine("\t-d | --database\tName of the database to connect to.");
            Console.WriteLine("\t-u | --user\tUssername with permissions to run the query.");
            Console.WriteLine("\t-p | --password\tPassword of the user.");
            Console.WriteLine("\tfilename\tFilename that contains the SQL commands, also accepts wildcards (*.sql, ...).");
            Console.WriteLine();
            Console.WriteLine("\t-h | --help\tShow this help message.");

            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.Write("Third ");
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.Write("3");
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine("ye Software Inc. (\u00A9) 2024");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("Version: {0}.{1}.{2}. ", versionMajor, versionMinor, versionRevision);
        }

        static void Main(string[] args)
        {
            string server = String.Empty;
            string database = String.Empty;
            string user = String.Empty;
            string password = String.Empty;
            string filename = String.Empty;

            if (args.Length < 9 || args.Length > 10)
            {
                Usage();
                return;
            }

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-s":
                    case "--server":
                        if (i + 1 < args.Length)
                            server = args[++i];
                        else
                            PrintError("Fatal Error", "Missing value for -s | --server option.", "Please read usage.");
                        break;

                    case "-d":
                    case "--database":
                        if (i + 1 < args.Length)
                            database = args[++i];
                        else
                            PrintError("Fatal Error", "Missing value for -d | --database option.", "Please read usage.");
                        break;

                    case "-u":
                    case "--user":
                        if (i + 1 < args.Length)
                            user = args[++i];
                        else
                            PrintError("Fatal Error", "Missing value for -u | --user option.", "Please read usage.");
                        break;

                    case "-p":
                    case "--password":
                        if (i + 1 < args.Length)
                            password = args[++i];
                        else
                            PrintError("Fatal Error", "Missing value for -p | --password option.", "Please read usage.");
                        break;
                    case "-v":
                    case "--verbose":
                        RUN_VERBOSE = true;
                        break;
                    case "-f":
                    case "--force":
                        FORCE_RUN = true;
                        break;
                    default:
                        filename = args[i];
                        break;
                }
            }
            PrintHeader("Start of process: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
            PrintInfo("  Server:        ", server);
            PrintInfo("  Database:      ", database);
            PrintInfo("  User:          ", user);
            PrintInfo("  Password:      ", "********"); // password);
            PrintInfo("  Force update:  ", (FORCE_RUN?"True":"False"));

            string directoryPath = Path.GetDirectoryName(filename);
            string searchPattern = Path.GetFileName(filename);
            if (string.IsNullOrEmpty(directoryPath) || string.IsNullOrWhiteSpace(directoryPath))
            {
                directoryPath = Path.GetDirectoryName("./");
            }
            PrintInfo("  Path:          ", directoryPath);

            if (searchPattern.Contains("*") || searchPattern.Contains("?"))
                PrintInfo("  SearchPattern: ", searchPattern);
            else
                PrintInfo("  Filename:      ", searchPattern);

            string[] files;
            try
            {
                files = Directory.GetFiles(directoryPath, searchPattern);
            }
            catch {
                PrintWarning("Warning! ", "Cant open directory '" + directoryPath + "' ... (Ignoring)");
                return;
            }

            string connectionString = "Server=" + server + ";Database=" + database + ";User Id=" + user + ";Password=" + password + ";";
            string operation = String.Empty;
            if (RUN_VERBOSE == true)
            {
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine($"Connection String: {connectionString}");
            }

            

            if (files.Length == 0)
            {
                PrintWarning("WARNING:", "No files found to proccess.");
            }
            else
            {
                PrintHeader("Files to process:");
                foreach (string file in files)
                {
                    WriteLog("  => " + file + ": ");
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write("  => ");
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.Write("{0} ", file);
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.Write("... ");
                    if (!RUN_VERBOSE && (
                         file.Contains("TOC_InsTarTOC_Backup") ||
                         file.Contains("PA_Taller_AbrirOT")
                        )
                       )
                    {
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.Write("[");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write("IGNORING FILE");
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.WriteLine("]");
                        continue;
                    }
                    string fileContent = File.ReadAllText(file);
                    operation = GetOperation(fileContent);
                    

                    if (RUN_VERBOSE == true)
                    {
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.WriteLine(fileContent);
                    }

                    if (!RUN_VERBOSE && (
                            file.Contains("co_InsClientsPV") ||
                            file.Contains("co_InsProveedoresPV") ||
                            file.Contains("co_InsProvVille") ||
                            file.Contains("co_InsTabCorrespProv") ||
                            file.Contains("ProveedoresBancos") ||
                            file.Contains("Proveedoresfac") ||
                            file.Contains("stoopciones") ||
                            file.Contains("toy_InsStoopc") ||
                            file.Contains("TOY_TAL_RELAX_Activar_Backup211014") ||
                            file.Contains("EPM01AS400HINM05TXT") ||
                            file.Contains("TOY_TAL_RELAX_Activar_Backup") ||
                            file.Contains("toy_InsStoopc") ||
                            file.Contains("stoopciones") ||
                            file.Contains("ProveedoresBancos") ||
                            file.Contains("Proveedoresfac") ||
                            file.Contains("PA_Taller_CrearPresupuesto") ||
                            file.Contains("PA_Taller_AnadirFormularioWAC") 
                       ))
                    {
                        // Awful hack to ingore certain files (only if not in verbose mode).
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.Write("[");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write("IGNORING (BAD QUERY)");
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.WriteLine("]");
                        continue;
                    }
                    if (!RUN_VERBOSE && (fileContent.Length < 10)) // no query can be les than, say, 10 char long
                    {
                        // Ignore empty queries (some are encrypted and you cant read them!
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.Write("[");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write("IGNORING (EMPTY QUERY)");
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.WriteLine("]");
                        continue;
                    }
                    if (true == FORCE_RUN 
                        || (fileContent.Contains("_FMT].") 
                        || fileContent.Contains("_FMT.")
                        || fileContent.Contains("_FAL].")
                        || fileContent.Contains("_FAL.")
                        || fileContent.Contains("_ENM].")
                        || fileContent.Contains("_ENM.")
                        || fileContent.Contains("_ING].")
                        || fileContent.Contains("_ING.")
                        || fileContent.Contains("_FRO].")
                        || fileContent.Contains("_FRO.")
                        || fileContent.Contains("_NAR].")
                        || fileContent.Contains("_NAR.")
                        || fileContent.Contains("_TYN].")
                        || fileContent.Contains("_TYN.")
                        || fileContent.Contains("_TYM].")
                        || fileContent.Contains("_TYM.")
                        || fileContent.Contains("_SON].")
                        || fileContent.Contains("_SON.")

                        ))
                    {
                        using (SqlConnection connection = new SqlConnection(connectionString))
                        {
                            try
                            {
                                connection.Open();
                                SqlCommand command = new SqlCommand(fileContent, connection);
                                try
                                {
                                    command.ExecuteNonQuery();
                                    PrintOkStatus(operation);
                                }
                                catch (SqlException ex)
                                {
                                    PrintErrorStatus(operation, ex.Message);
                                }
                                catch
                                {
                                    PrintErrorStatus(operation);
                                }
                                connection.Close();
                            }
                            catch (SqlException ex)
                            {
                                PrintErrorStatus(operation, ex.Message);
                            }
                            catch
                            {
                                PrintErrorStatus(operation);
                            }
                        }
                    }
                    else {
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.Write("[");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write("IGNORING (No changes detected)");
                        Console.ForegroundColor = ConsoleColor.DarkGray;
                        Console.WriteLine("]");
                    }
                }
                Console.ForegroundColor = ConsoleColor.White;
            }

            PrintInfo("Se log file for details: ", logFilePath);
            PrintHeader("End of process: " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffff"));

        }
    }
}


