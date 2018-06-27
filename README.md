# LeanIPC - Library for interprocedure communications and remote procedure calls

This library offers an easy way to communicate between two processes even if they run on different machines, and only depends on the `System.Reflection.Emit` package. 

The core of the library is a serialization and deserialization implementation that is built for transfering types commonly used in the .Net languages, such as `int`, `List<>` and `Dictionary<...>` types. Value types (aka `struct` in C#) can be automatically transfered as well. Using a request/response mechanism, it is possible to send user defined values with inter-process communication (IPC).

On top of this system, it is also possible to use the remote procedure call (RPC) layer, which simplifies invoking methods across processes. The RPC layer has support for generating a remote proxy for an interface, making it trivial to create local proxies that invoke methods remotely. The library also has support for `Task` methods, using the `await/async` pattern, such that requests to not block a thread while waiting for the response.
