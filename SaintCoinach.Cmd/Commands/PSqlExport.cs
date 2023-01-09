using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SaintCoinach.Xiv;
using Tharga.Toolkit.Console.Command.Base;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using SaintCoinach.Ex;
using SaintCoinach.Ex.Relational;

namespace SaintCoinach.Cmd.Commands
{
    public class PSqlExport : ActionCommandBase
    {
        private ARealmReversed _Realm;
        
        public PSqlExport(ARealmReversed realm) : 
            base("psql", "Exports the EXD data as Postgresql compatible schema and associated imports.")
        {
            _Realm = realm;
        }

        private string GetSqlType(Type type)
        {
            switch (type.Name)
            {
                case "UInt32":
                    return "bigint";
                case "Int32":
                    return "integer";
                case "SByte":
                    return "smallint";
                case "Byte":
                    return "smallint";
                case "XivString":
                    return "TEXT";
                case "Boolean":
                    return "boolean";
                case "UInt16":
                    return "integer";
                case "Int16":
                    return "smallint";
                case "Single":
                    return "float";
                case "Double":
                    return "double";
                case "Quad":
                    return "bigint";
                
                default:
                    throw new NotImplementedException( $"The type {type.Name} doesn't have an SQL type mapping");
            }
        }
        
        static bool IsUnescaped(object self) {
            return (self is Boolean
                    || self is Byte
                    || self is SByte
                    || self is Int16
                    || self is Int32
                    || self is Int64
                    || self is UInt16
                    || self is UInt32
                    || self is UInt64
                    || self is Single
                    || self is Double);
        }

        private string GetTableName(ISheet sheet)
        {
            return $"{sheet.Name.ToLower().Replace("/", "_")}";
        }

        private string FormatColumnName(string input)
        {
            if (input == "_key" || input == "_subkey")
            {
                return input;
            }
            input = Regex.Replace(input, "([A-Z])", "_$1");
            input = Regex.Replace(input, "([A-Z])", "_$1");
            input = input.Trim(new char[] {' ', '_', ']'});
            input = input.ToLower();
            input = input.Replace('[', '_');
            input = Regex.Replace(input, "{([0-9+])}", "_$i");
            input = Regex.Replace(input, "{|}|]", "");
            if (input == "order")
            {
                input = "\"order\"";
            }
            Console.WriteLine(input);
            return input;
        }

        private void DoRowData(ISheet sheet, XivRow row, List<string> data, StringBuilder sb)
        {
            for (int i = 0; i < sheet.Header.ColumnCount; i++)
            {
                var o = row.GetRaw(i);

                if (o is Quad)
                {
                    var q = (Quad)o;
                    data.Add(q.ToInt64().ToString());
                    continue;
                }

                if (IsUnescaped(o))
                {
                    data.Add(o.ToString());
                    continue;
                }
                    
                string d = o.ToString();
                if (string.IsNullOrEmpty(d))
                    d = "NULL";
                else
                {
                    d = $"'{d.Replace("'", "\\'")}'";
                }
                data.Add(d);
            }
            
            sb.AppendLine($"  ( {string.Join(", ", data)} ),");
        }

        private void WriteVariant1Rows(ISheet sheet, StringBuilder sb)
        {
            var rows = sheet.Cast<XivRow>();
            var cols = new List<string>();
            
            cols.Add("_key");

            foreach (var col in sheet.Header.Columns.Cast<RelationalColumn>())
            {
                string name = string.IsNullOrEmpty(col.Name) ? $"unk{col.Index}" : col.Name;
                
                cols.Add($"{FormatColumnName(name)}");
            }

            sb.AppendLine($"INSERT INTO {GetTableName(sheet)} ({string.Join(", ", cols)}) VALUES ");
            
            foreach (var row in rows)
            {
                var data = new List<string>();
                data.Add(row.Key.ToString());

                DoRowData(sheet, row, data, sb);
            }

            sb.Remove(sb.Length - 3, 3);
            sb.AppendLine(";");
        }

        private void WriteVairant2Rows(ISheet sheet, StringBuilder sb)
        {
            var rows = sheet.Cast<XivSubRow>();
            var cols = new List<string>();
            
            cols.Add("_key");
            cols.Add("_subkey");

            foreach (var col in sheet.Header.Columns.Cast<RelationalColumn>())
            {
                string name = string.IsNullOrEmpty(col.Name) ? $"unk{col.Index}" : col.Name;
                
                cols.Add($"{FormatColumnName(name)}");
            }

            sb.AppendLine($"INSERT INTO {GetTableName(sheet)} ({string.Join(", ", cols)}) VALUES ");

            foreach (var row in rows)
            {
                var data = new List<string>();
                data.Add(row.ParentRow.Key.ToString());
                data.Add(row.Key.ToString());

                DoRowData(sheet, row, data, sb);
            }

            sb.Remove(sb.Length - 3, 3);
            sb.AppendLine(";");
        }

        private void WriteRows(ISheet sheet, StringBuilder sb)
        {
            if (sheet.Header.Variant == 1)
                WriteVariant1Rows(sheet, sb);
            else
                WriteVairant2Rows(sheet, sb);
        }

        public override async Task<bool> InvokeAsync(string paramList)
        {
            var imports = new List<string>();
            
            // .Where(n => !n.Contains("quest/") && !n.Contains("custom/"))
            foreach (var name in _Realm.GameData.AvailableSheets)
            {
                var sheet = _Realm.GameData.GetSheet(name);
                var variant = sheet.Header.Variant;
                var sheet2 = sheet as XivSheet2<XivSubRow>;
                
                Console.WriteLine($"Sheet: {name}, variant: {variant}");

                if (sheet.Count == 0)
                    continue;

                var sb = new StringBuilder();
                sb.AppendLine($"create table if not exists {GetTableName(sheet)} (");
                
                // key meme
                if (sheet.Header.Variant == 1)
                {
                    sb.AppendLine($"  _key integer NOT NULL PRIMARY KEY,");
                }
                else
                {
                    sb.AppendLine($"  _key integer NOT NULL,");
                    sb.AppendLine($"  _subkey integer NOT NULL,");
                }
                
                // add cols
                foreach (var column in sheet.Header.Columns)
                {
                    var colName = column.Name;
                    if (string.IsNullOrEmpty(colName))
                        colName = $"unk{column.Index}";
                    
                    sb.AppendLine($"  {colName.ToLower()} {GetSqlType(column.Reader.Type)},");
                }

                // primary key, only for a hybrid key type, for the main variant 1, the pkey is built into the main structure.
                if (sheet.Header.Variant == 2)
                {
                    sb.AppendLine($"  PRIMARY KEY (_key, _subkey)");
                }
            
                sb.AppendLine(");");
                sb.AppendLine();
                
                WriteRows(sheet, sb);
                
                imports.Add(sb.ToString());
            }
            
            File.WriteAllText("psql-schema.sql", string.Join(Environment.NewLine, imports));

            return true;
        }
    }
}