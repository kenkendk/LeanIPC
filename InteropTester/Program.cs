using System;

namespace InteropTester
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            // TODO: Write a README file

            var ut = new Unittest.IPCTypeTest();
            ut.TestEchoWithAllPrimitives();
            ut.TestEchoWithAllPrimitivePairs();
            ut.TestEchoWithAllPrimitiveKeyValuePairs();
            ut.TestEchoWithAllPrimitiveArraysAndLists();
            ut.TestEchoWithAllPrimitiveDictionary();
            ut.TestRunningShutdown();
            ut.TestMaxPendingLimit();

            var rt = new Unittest.RPCTester();
            rt.RemoteInvoke();
            rt.RemoteReferences();
            rt.RemoteTwoWayRemote();
            rt.TestAutomaticProxy();
            rt.TestPropertyIndices();
            rt.TestAsyncManualSupport();
            rt.TestAsyncAutomaticSupport();
        }
    }
}
