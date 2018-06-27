using NUnit.Framework;
using System;
using LeanIPC;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace Unittest
{
    [TestFixture()]
    public class IPCTypeTest
    {
        private static Task<RequestHandlerResponse> EchoMethod(ParsedMessage message)
        {
            if (message.Arguments == null || message.Arguments.Length == 0)
                return Task.FromResult(RequestHandlerResponse.FromResult((object)null));
            
            return Task.FromResult(new RequestHandlerResponse(message.Types, message.Arguments));
        }

        [Test()]
        public void TestEchoWithInts()
        {
            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {

                var sendIntTask = setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<int, int>(42));
                if (sendIntTask.Result != 42)
                    throw new Exception("Failed to transport int through server");

                sendIntTask = setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<int, int>(42));
                if (sendIntTask.Result != 42)
                    throw new Exception("Failed to transport int through client");
            }
        }

        public static async Task<object> GenericInvokeHelper<T>(ClientServerTester setup, InterProcessConnection con, T value)
        {
            return (object) await setup.GuardedOperationAsync(() => con.SendAndWaitAsync<T, T>(value));
        }

        public static async Task<RequestHandlerResponse> GenericInvokeHelper2<T1, T2>(ClientServerTester setup, InterProcessConnection con, T1 v1, T2 v2)
        {
            return await setup.GuardedOperationAsync(() => con.SendAndWaitAsync(new Type[] { typeof(T1), typeof(T2) }, new object[] { v1,v2 }));
        }

        public static bool CompareItems<T>(T a, T b)
        {
            // The exception types are lost on IPC calls
            if (typeof(Exception).IsAssignableFrom(typeof(T)))
                return ((Exception)(object)a).Message == ((Exception)(object)b).Message;

            return System.Collections.Generic.EqualityComparer<T>.Default.Equals(a, b);
        }

        public static bool AreEqual(Type t, object a, object b)
        {
            return
                (bool)
                typeof(IPCTypeTest)
                .GetMethod(nameof(CompareItems))
                .MakeGenericMethod(new Type[] { t })
                .Invoke(null, new object[] { a, b });
        }

        public static object CreateDefault(Type t)
        {
            if (t == typeof(Type))
                return typeof(string);
            else if (t == typeof(string))
                return "abc";
            else if (t == typeof(Exception))
                return new ArithmeticException();
            else if (t == typeof(void))
                return null;
            else
                return Activator.CreateInstance(t);
        }

        [Test()]
        public void TestEchoWithAllPrimitives()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestEchoWithAllPrimitives)}");
            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {
                foreach (var t in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                {
                    var v = CreateDefault(t);
                    if (v == null)
                        continue;

                    System.Diagnostics.Trace.WriteLine($"* Testing with {t}");

                    var method = typeof(IPCTypeTest).GetMethod(nameof(GenericInvokeHelper)).MakeGenericMethod(new Type[] { t });

                    var sendItemTask = (Task<object>)method.Invoke(null, new object[] { setup, setup.Server, v });
                    if (!AreEqual(t, sendItemTask.Result, v))
                        throw new Exception($"Failed to transport unboxed {t} through server");
                    
                    sendItemTask = (Task<object>)method.Invoke(null, new object[] { setup, setup.Client, v });
                    if (!AreEqual(t, sendItemTask.Result, v))
                        throw new Exception($"Failed to transport unboxed {t} through server");

                    sendItemTask = setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<object, object>(v));
                    if (!AreEqual(t, sendItemTask.Result, v))
                        throw new Exception($"Failed to transport boxed {t} through server");

                    sendItemTask = setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<object, object>(v));
                    if (!AreEqual(t, sendItemTask.Result, v))
                        throw new Exception($"Failed to transport boxed {t} through client");
                }

                System.Diagnostics.Trace.WriteLine($"* Shutting down");
            }
        }

        [Test()]
        public void TestEchoWithAllPrimitivePairs()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestEchoWithAllPrimitivePairs)}");
            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {
                foreach (var t1 in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                    foreach (var t2 in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                    {
                        var v1 = CreateDefault(t1);
                        var v2 = CreateDefault(t2);
                        if (v1 == null || v2 == null)
                            continue;

                        System.Diagnostics.Trace.WriteLine($"* Testing with [{t1},{t2}]");

                        var sendItemTask = setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync(new Type[] { t1, t2 }, new object[] { v1, v2 }));
                        if (!AreEqual(t1, sendItemTask.Result.Values[0], v1))
                            throw new Exception($"Failed to transport boxed {t1} through server");
                        if (!AreEqual(t2, sendItemTask.Result.Values[1], v2))
                            throw new Exception($"Failed to transport boxed {t2} through server");

                        sendItemTask = setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync(new Type[] { t1, t2 }, new object[] { v1, v2 }));
                        if (!AreEqual(t1, sendItemTask.Result.Values[0], v1))
                            throw new Exception($"Failed to transport boxed {t1} through client");
                        if (!AreEqual(t2, sendItemTask.Result.Values[1], v2))
                            throw new Exception($"Failed to transport boxed {t2} through client");
                    }

                System.Diagnostics.Trace.WriteLine($"* Shutting down");
            }
        }

        [Test()]
        public void TestEchoWithAllPrimitiveKeyValuePairs()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestEchoWithAllPrimitiveKeyValuePairs)}");
            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {
                foreach (var t1 in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                    foreach (var t2 in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                    {
                        var v1 = CreateDefault(t1);
                        var v2 = CreateDefault(t2);
                        if (v1 == null || v2 == null)
                            continue;

                        System.Diagnostics.Trace.WriteLine($"* Testing with KeyValuePair<{t1},{t2}>");
                        var kvpMethod = typeof(IPCTypeTest).GetMethod(nameof(RunWithKeyValuePair)).MakeGenericMethod(new Type[] { t1, t2 });
                        ((Task)kvpMethod.Invoke(null, new object[] { setup, v1, v2 })).Wait();
                    }

                System.Diagnostics.Trace.WriteLine($"* Shutting down");
            }
        }

        public static async Task RunWithKeyValuePair<TKey, TValue>(ClientServerTester setup, TKey key, TValue value)
        {
            var a = new KeyValuePair<TKey, TValue>(key, value);

            var result = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<KeyValuePair<TKey, TValue>, KeyValuePair<TKey, TValue>>(a));
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport unboxed {typeof(KeyValuePair<TKey, TValue>)} through server");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport unboxed {typeof(KeyValuePair<TKey, TValue>)} through server");

            result = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<KeyValuePair<TKey, TValue>, KeyValuePair<TKey, TValue>>(a));
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport unboxed {typeof(KeyValuePair<TKey, TValue>)} through client");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport unboxed {typeof(KeyValuePair<TKey, TValue>)} through client");

            var resultObject = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport boxed {typeof(KeyValuePair<TKey, TValue>)} through server");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport boxed {typeof(KeyValuePair<TKey, TValue>)} through server");

            resultObject = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport boxed {typeof(KeyValuePair<TKey, TValue>)} through client");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport boxed {typeof(KeyValuePair<TKey, TValue>)} through client");
        }

        public static async Task RunWithArray<T>(ClientServerTester setup, T value)
        {
            var a = new T[] { value };

            var result = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<T[], T[]>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport unboxed {typeof(T[])} through server");

            result = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<T[], T[]>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport unboxed {typeof(T[])} through client");

            var resultObject = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport boxed {typeof(T[])} through server");

            resultObject = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport boxed {typeof(T[])} through client");
        }

        public static async Task RunWithList<T>(ClientServerTester setup, T value)
        {
            var a = new List<T>(new T[] { value });

            var result = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<List<T>, List<T>>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport unboxed {typeof(T[])} through server");

            result = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<List<T>, List<T>>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport unboxed {typeof(T[])} through client");

            var resultObject = await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport boxed {typeof(T[])} through server");

            resultObject = await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<object, object>(a));
            if (!AreEqual(typeof(T), result[0], value))
                throw new Exception($"Failed to transport boxed {typeof(T[])} through client");
        }

        public static async Task RunWithDictionary<TKey, TValue>(ClientServerTester setup, TKey key, TValue value)
        {
            var a = new Dictionary<TKey, TValue>();
            a.Add(key, value);

            var result = (await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<Dictionary<TKey, TValue>, Dictionary<TKey, TValue>>(a))).First();
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport unboxed {typeof(Dictionary<TKey, TValue>)} through server");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport unboxed {typeof(Dictionary<TKey, TValue>)} through server");

            result = (await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<Dictionary<TKey, TValue>, Dictionary<TKey, TValue>>(a))).First();
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport unboxed {typeof(Dictionary<TKey, TValue>)} through client");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport unboxed {typeof(Dictionary<TKey, TValue>)} through client");

            result = ((Dictionary<TKey, TValue>)(await setup.GuardedOperationAsync(() => setup.Server.SendAndWaitAsync<object, object>(a)))).First();
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport boxed {typeof(Dictionary<TKey, TValue>)} through server");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport boxed {typeof(Dictionary<TKey, TValue>)} through server");

            result = ((Dictionary<TKey, TValue>)(await setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<object, object>(a)))).First();
            if (!AreEqual(typeof(TKey), result.Key, key))
                throw new Exception($"Failed to transport boxed {typeof(Dictionary<TKey, TValue>)} through client");
            if (!AreEqual(typeof(TValue), result.Value, value))
                throw new Exception($"Failed to transport boxed {typeof(Dictionary<TKey, TValue>)} through client");
        }

        [Test()]
        public void TestEchoWithAllPrimitiveArraysAndLists()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestEchoWithAllPrimitiveArraysAndLists)}");
            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {
                foreach (var t in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                {
                    var v = CreateDefault(t);
                    if (v == null)
                        continue;

                    System.Diagnostics.Trace.WriteLine($"* Testing with array {t}[]");
                    var arrayMethod = typeof(IPCTypeTest).GetMethod(nameof(RunWithArray)).MakeGenericMethod(new Type[] { t });
                    ((Task)arrayMethod.Invoke(null, new object[] { setup, v })).Wait();

                    System.Diagnostics.Trace.WriteLine($"* Testing with List<{t}>");
                    var listMethod = typeof(IPCTypeTest).GetMethod(nameof(RunWithList)).MakeGenericMethod(new Type[] { t });
                    ((Task)arrayMethod.Invoke(null, new object[] { setup, v })).Wait();

                }

                System.Diagnostics.Trace.WriteLine($"* Shutting down");
            }
        }

        [Test()]
        public void TestEchoWithAllPrimitiveDictionary()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestEchoWithAllPrimitiveDictionary)}");

            using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
            {
                foreach (var tvalue in LeanIPC.TypeSerializer.PRIMITIVE_TYPES)
                    foreach (var tkey in new Type[] { typeof(int), typeof(long), typeof(string) })
                    {
                        var k = CreateDefault(tkey);
                        var v = CreateDefault(tvalue);
                        if (v == null)
                            continue;

                        System.Diagnostics.Trace.WriteLine($"* Testing with Dictionary<{tkey}, {tvalue}>");
                        var dictMethod = typeof(IPCTypeTest).GetMethod(nameof(RunWithDictionary)).MakeGenericMethod(new Type[] { tkey, tvalue });
                        ((Task)dictMethod.Invoke(null, new object[] { setup, k, v })).Wait();

                    }

                System.Diagnostics.Trace.WriteLine($"* Shutting down");
            }
        }

        [Test()]
        public void TestRunningShutdown()
        {
            System.Diagnostics.Trace.WriteLine($"* Running {nameof(TestRunningShutdown)}");

            try
            {
                using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
                {
                    System.Diagnostics.Trace.WriteLine($"* Firing into server");

                    // Fire a bunch of data into the system
                    for (var i = 0; i < 1000; i++)
                        setup.Server.SendAndWaitAsync<int, int>(i);

                    // Then quit without waiting for the responses
                    System.Diagnostics.Trace.WriteLine($"* Shutting down");
                }
            }
            catch
            {
            }

            try
            {
                using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
                {
                    System.Diagnostics.Trace.WriteLine($"* Firing into client");

                    // Fire a bunch of data into the system
                    for (var i = 0; i < 1000; i++)
                        setup.Client.SendAndWaitAsync<int, int>(i);

                    // Then quit without waiting for the responses
                    System.Diagnostics.Trace.WriteLine($"* Shutting down");
                }
            }
            catch
            {
            }

            try
            {
                using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
                {
                    System.Diagnostics.Trace.WriteLine($"* Firing into server and client");

                    // Fire a bunch of data into the system
                    for (var i = 0; i < 1000; i++)
                    {
                        setup.Server.SendAndWaitAsync<int, int>(i);
                        setup.Client.SendAndWaitAsync<int, int>(i);
                    }

                    // Then quit without waiting for the responses
                    System.Diagnostics.Trace.WriteLine($"* Killing connection");
                }
            }
            catch
            {
            }

            try
            {
                using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
                {
                    System.Diagnostics.Trace.WriteLine($"* Firing into server and client");

                    // Fire a bunch of data into the system
                    for (var i = 0; i < 1000; i++)
                    {
                        setup.Server.SendAndWaitAsync<int, int>(i);
                        setup.Client.SendAndWaitAsync<int, int>(i);
                    }

                    // Then quit without waiting for the responses
                    System.Diagnostics.Trace.WriteLine($"* Killing connection");
                    setup.S1.Dispose();
                }
            }
            catch
            {
            }

            try
            {
                using (var setup = new ClientServerTester(EchoMethod, EchoMethod))
                {
                    System.Diagnostics.Trace.WriteLine($"* Firing into server and client");

                    // Fire a bunch of data into the system
                    for (var i = 0; i < 1000; i++)
                    {
                        setup.Server.SendAndWaitAsync<int, int>(i);
                        setup.Client.SendAndWaitAsync<int, int>(i);
                    }

                    // Then quit without waiting for the responses
                    System.Diagnostics.Trace.WriteLine($"* Killing connection");
                    setup.S2.Dispose();
                }
            }
            catch
            {
            }
        }



        [Test()]
        public void TestMaxPendingLimit()
        {
            TaskCompletionSource<bool> serverBlocker = new TaskCompletionSource<bool>();
            var serverMessages = 0;

            LeanIPC.RequestHandler serverHandler = async (message) =>
            {
                System.Threading.Interlocked.Increment(ref serverMessages);
                await serverBlocker.Task;

                if (message.Arguments == null || message.Arguments.Length == 0)
                    return RequestHandlerResponse.FromResult((object)null);

                return new RequestHandlerResponse(message.Types, message.Arguments);
            };

            using (var setup = new ClientServerTester(serverHandler, EchoMethod))
            {

                setup.Client.MaxPendingRequests = 10;
                var sendTasks = Enumerable
                    .Range(0, 20)
                    .Select(x =>
                        setup.GuardedOperationAsync(() => setup.Client.SendAndWaitAsync<int, int>(x))
                    )
                    .ToArray();

                // Make sure the requests have propagated
                System.Threading.Thread.Sleep(1000);
                if (serverMessages != setup.Client.MaxPendingRequests)
                    throw new Exception($"Server got {serverMessages} messages but the pending limit is {setup.Client.MaxPendingRequests}");

                // Trigger a resend
                setup.Client.MaxPendingRequests = 15;
                System.Threading.Thread.Sleep(1000);
                if (serverMessages != setup.Client.MaxPendingRequests)
                    throw new Exception($"Server got {serverMessages} messages but the pending limit is {setup.Client.MaxPendingRequests}");

                // Trigger a no-op resend
                setup.Client.MaxPendingRequests = 10;
                System.Threading.Thread.Sleep(1000);
                if (serverMessages != 15)
                    throw new Exception($"Server got {serverMessages} messages but the pending limit is {15}");

                serverBlocker.TrySetResult(true);

                Task.WhenAll(sendTasks).Wait();
                if (serverMessages != 20)
                    throw new Exception($"Server got {serverMessages} messages but should have {12}");
            }
        }

    }
}
