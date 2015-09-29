using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SUBD
{
    class Program
    {
        static void Main(string[] args)
        {
            
            /*DateTime dt = new DateTime(2015, 8, 20, 20, 00, 25);
            long fdt = dt.ToBinary();
            byte[] bdt = BitConverter.GetBytes(fdt);
            fdt = BitConverter.ToInt64(bdt, 0);
            DateTime cdt = new DateTime(fdt);
            Console.WriteLine("size of float {0}", sizeof(long));*/
            FileSUBD FS = new FileSUBD();
            
            Console.ReadLine();

        }
    }
}
