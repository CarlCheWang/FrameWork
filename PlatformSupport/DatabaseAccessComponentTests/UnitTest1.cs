using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NUnit.Framework;
using Assert = Microsoft.VisualStudio.TestTools.UnitTesting.Assert;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    internal delegate bool D();

    internal delegate bool D2(int i);

    [TestClass]
    public class UnitTest1
    {
        D del;
        D2 del2;

        public void TestMethod(int input)
        {
            int j = 0;
            // Initialize the delegates with lambda expressions.  
            // Note access to 2 outer variables.  
            // del will be invoked within this method.  
            del = () =>
            {
                j = 10;
                return j > input;
            };

            // del2 will be invoked after TestMethod goes out of scope.  
            del2 = (x) => { return x == j; };

            // Demonstrate value of j:  
            // Output: j = 0   
            // The delegate has not been invoked yet.  
            Console.WriteLine("j = {0}", j); // Invoke the delegate.  
            bool boolResult = del();

            // Output: j = 10 b = True  
            Console.WriteLine("j = {0}. b = {1}", j, boolResult);
        }

        [TestMethod]
        public void TestMethod1()
        {
            UnitTest1 test = new UnitTest1();
            test.TestMethod(5);

            // Prove that del2 still has a copy of  
            // local variable j from TestMethod.  
            bool result = test.del2(10);

            // Output: True  
            //Console.WriteLine(result);

            //Console.ReadKey();
            Assert.IsTrue(result);
        }
    }

    [TestClass]
    public class UnitTest2
    {
        // Define two methods that have the same signature as CustomDel.
        static void Hello(string s)
        {
            System.Console.WriteLine("  Hello, {0}!", s);
        }

        static void Goodbye(string s)
        {
            System.Console.WriteLine("  Goodbye, {0}!", s);
        }

        private delegate void CustomDel(string s);

        [TestMethod]
        public void TestMethod1()
        {
            // Declare instances of the custom delegate.
            CustomDel hiDel, byeDel, multiDel, multiMinusHiDel;

            // In this example, you can omit the custom delegate if you 
            // want to and use Action<string> instead.
            //Action<string> hiDel, byeDel, multiDel, multiMinusHiDel;

            // Create the delegate object hiDel that references the
            // method Hello.
            hiDel = Hello;

            // Create the delegate object byeDel that references the
            // method Goodbye.
            byeDel = Goodbye;

            // The two delegates, hiDel and byeDel, are combined to 
            // form multiDel. 
            multiDel = hiDel + byeDel;

            // Remove hiDel from the multicast delegate, leaving byeDel,
            // which calls only the method Goodbye.
            multiMinusHiDel = multiDel - hiDel;

            Console.WriteLine("Invoking delegate hiDel:");
            hiDel("A");
            Console.WriteLine("Invoking delegate byeDel:");
            byeDel("B");
            Console.WriteLine("Invoking delegate multiDel:");
            multiDel("C");
            Console.WriteLine("Invoking delegate multiMinusHiDel:");
            multiMinusHiDel("D");
        }

        [TestMethod]
        [TestCase(-1)]
        [TestCase(0)]
        [TestCase(1)]
        public void ReturnFalseGivenValuesLessThan2(int value)
        {
            //var result = _primeService.IsPrime(value);

            //Assert.IsFalse(value.ToString() == $"{value} should not be prime");
        }
    }
}
