using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Reflection;
using System.IO;

using ConsoleApplication1.ServiceReference1;

namespace MASCrawler
{
    /* Business class */
    public class Pub
    {
        private uint _id;
        private string _title;
        private uint _year;
        private string _abstract;

        [Column(Name = "ID", DataType = typeof(uint))]
        public uint ID
        {
            get { return _id; }
            set { _id = value; }
        }

        public string Title
        {
            get { return _title; }
            set { _title = value; }
        }

        [Column(Name = "Year", DataType = typeof(uint))]
        public uint Year
        {
            get { return _year; }
            set { _year = value; }
        }

        [Column(Name = "Abstract", DataType = typeof(string))]
        public string Abstract
        {
            get { return _abstract; }
            set { _abstract = value; }
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        private string _name;

        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }
        private Type _dataType;

        public Type DataType
        {
            get { return _dataType; }
            set { _dataType = value; }
        }
    }

    public class TextFileReader<T> : IEnumerable<T> where T : new()
    {
        private string _fileName = string.Empty;
        private string _delimiter = string.Empty;
        private Dictionary<String, PropertyInfo> _headerPropertyInfos =
            new Dictionary<string, PropertyInfo>();
        private Dictionary<String, Type> _headerDaytaTypes = new Dictionary<string, Type>();

        public TextFileReader(string fileName, string delimiter)
        {
            this._fileName = fileName;
            this._delimiter = delimiter;
        }

        #region IEnumerable<string[]> Members

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            using (StreamReader streamReader = new StreamReader(this._fileName))
            {
                string[] headers = streamReader.ReadLine().Split(new String[] { this._delimiter }, StringSplitOptions.None);
                this.ReadHeader(headers);

                while (!streamReader.EndOfStream)
                {
                    T item = new T();

                    string[] rowData = streamReader.ReadLine().Split(new String[] { this._delimiter }, StringSplitOptions.None);

                    for (int index = 0; index < headers.Length; index++)
                    {
                        string header = headers[index];
                        this._headerPropertyInfos[header].SetValue
                (item, Convert.ChangeType(rowData[index],
                this._headerDaytaTypes[header]), null);
                    }
                    yield return item;
                }
            }
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)((IEnumerator<T>)this)).GetEnumerator();
        }

        #endregion

        private void ReadHeader(string[] headers)
        {
            foreach (String header in headers)
            {
                foreach (PropertyInfo propertyInfo in (typeof(T)).GetProperties())
                {
                    foreach (object attribute in propertyInfo.GetCustomAttributes(true))
                    {
                        if (attribute is ColumnAttribute)
                        {
                            ColumnAttribute columnAttribute = attribute as ColumnAttribute;
                            if (columnAttribute.Name == header)
                            {
                                this._headerPropertyInfos[header] = propertyInfo;
                                this._headerDaytaTypes[header] = columnAttribute.DataType;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }


    public class CSV
    {
        private const string filename = "publications.csv";

        public static void csvInsert(uint id, string title, uint year, string abst)
        {
            const string header = "ID, Title, Year, Abstract";

            StringBuilder myString = new StringBuilder();

            StreamWriter sw = null;

            if (!File.Exists(filename))
            {
                sw = new StreamWriter(filename, true);
                sw.WriteLine(header);
            }
            else
            {
                sw = new StreamWriter(filename, true);
            }

            if (abst == null)
                abst = "x";
            if (abst.Equals(""))
                abst = "x";
            abst.Replace(",", "");
            abst.Replace("\n", "");
            abst.Replace("\r", "");

            if (title == null)
                title = "x";
            if (title.Equals(""))
                title = "x";
            title.Replace(",", "");
            title.Replace("\n", "");
            title.Replace("\r", "");

            string line = id + "," + title + "," + year + "," + abst;
            sw.WriteLine(line);
            sw.Close();

            GC.Collect();
        }

        public static void csvInsert(List<Publication> publications)
        {
            foreach (var p in publications)
            {
                csvInsert(p.ID, p.Title, p.Year, p.Abstract);
            }
        }



        /*    TextFileReader<Pub> reader2 = 
                new TextFileReader<Pub>(@"Sample.txt", ",");

            var query2 = from it2 in reader2
                         where it2.ID > 2
                         select it2;

            foreach (var x2 in query2)
            {
                Console.WriteLine(String.Format("ID={0} Title={1} Year={2} Abstract={3}", 
                    x2.ID, x2.Title, x2.Year, x2.Abstract));
            }
        */
    }

}
