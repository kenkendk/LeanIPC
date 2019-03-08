# LeanIPC - Library for interprocedure communications and remote procedure calls

This library offers an easy way to communicate between two processes even if they run on different machines, and only depends on the `System.Reflection.Emit` package. If you find tutorials and other materials describing how to communicate, they often just send strings with newline delimiters. While this works for simple things, it is difficult to transmit more complex structures with such a basis. 

LeanIPC provides a more extensive way to communicate across any `Stream` boundary, while maintaining functionality and data structures.

The core of the library is a serialization and deserialization implementation that is built for transfering types commonly used in the .Net languages, such as `int`, `List<>` and `Dictionary<...>` types. Value types (aka `struct` in C#) can be automatically transfered as well. Using a request/response mechanism, it is also possible to send user defined values with inter-process communication (IPC).

On top of this system, it is also possible to use the remote procedure call (RPC) layer, which simplifies invoking methods across processes. The RPC layer has support for generating a remote proxy for an interface, making it trivial to create local proxies that invoke methods remotely. The library also has support for `Task` methods, using the `await/async` pattern, such that requests will not block a thread while waiting for the response.

## Type serializer
The type serializer is designed to describe and transfer the primitive types in .Net, such as `bool`, `int`, and `string`. It does this by defining a single byte typename for each of the commonly used types. Other more complicated types that are commonly used include arrays, `Dictionary<,>`, `Tuple<>`, `List<>`, `KeyValuePair<,>`, and `IEnumerable<>`.

The serializer works by prefixing each message with the types of the data being transmitted. For known simple types, this will be a short string, but for custom types this can be a bit longer (but can be overriden to produce smaller typenames). After the type header, the data is serialized to a binary representation. When deserializing, the type header provides all the information required to reconstruct the items.

The type serialization system is flexible and allows custom control, including mapping functions and user-supplied serializer/deserializers.

The serializer is fully accessible and can be used for other purposes, such as persisting data to files.

## Inter-process communication
When communicating with another process, it is often useful to have different message types being sent, where some also require a response. The IPC layer in LeanIP offers this functionality, allowing you to send a message (with arguments passed through the type serializer), and read back the response.

The IPC layer allows you to define custom messages and handlers, and is fully thread safe, allowing multiple threads on both sides to communicate without interference.

## Remote procedure calls
With an IPC system, it is also possible to define messages that, in turn, invoke methods remotely. The RPC layer in LeanIPC implements this functionality, allowing you to invoke a method in another process. To support cases where the invoked method returns an object, LeanIPC offers to create a proxy method, using just an interface definition. This autogenerated proxy will then implement the interface and redirect any methods or properties that are invoked to the remote process. This allows a somewhat transparent cross-process communication.

The default for RPC is to not allow any types, but requires that the setup code explictly enables different types and methods for remote invocation.

## Comparison to `System.Runtime.Remoting`
With .Net core, the remoting functionality in .Net was removed due to complexity. Much of this complexity arises when attempting to make it "enterprise grade", in the sense that it tries very hard to not allow misconfigurations and support many different scenarios.

LeanIPC is much smaller and puts slightly more work on the programmer, in that you manually need to declare which remote references are mapped to which interfaces. It also does not handle object lifetime cycles, so you need to make sure you `Dispose()` objects that you no longer need.

## Example
```csharp
interface ITarget {
	int Value { get; set; }
	string Print(int extra);
}

class Target {
	public int Value { get; set; }
	public string Print(int extra) => $"Value is {Value + extra}";
}

RPCPeer RunClient(Stream stream) {
    return
        new RPCPeer(stream)
        // We allow calls to all methods on Target (including constructors)
        // and only return handles (by-ref) for Target instances
        .RegisterLocallyServedType<Target>()
        .Start(true); // Connect as client
}

RPCPeer RunServer(Stream stream) {
    return
        new RPCPeer(stream)
        // When we get a handle to a Target method, 
        // wrap it inside an ITarget with an automatically
        // generated proxy to call the remote side
        .RegisterLocalProxyForRemote<ITarget, Target>()
        
        // We could allow the client to also call Target on the server
        // but for this example we do not allow it
        // .RegisterLocallyServedType<Target>()
        
        .Start(false); // Connect as server
}

var server = RunServer(...stream...);
// Assume the client is running in another process

// Create an instance of Target in the client process
// We do not need to have the actual type in this process, 
// we could just use the name of the type
var remotetarget = await server.CreateRemoteInstanceAsync<Target>();

// Invoke a property remotely
remotetarget.Value = 40;
// Invoke a function remotely
Console.WriteLine(remotetarget.Print(2));
```

## Security
The inter-process layer supports basic authentication, with support for manual verification of both the client and server. If LeanIPC is used for security sensitive transmissions, you should also ensure that the communication channel is encrypted (i.e. use SSL).

For the RPC part, it is obviously easy to abuse a system that allows calling methods remotely. For this reason, the RPCPeer starts out by not allowing any calls to go throug, and needs to be configured to explicitly allow calls to some methods. The easiest way to handle this, is to declare one or more types that are deemed "remoting safe" and then only allow calls to these methods.

## Type considerations
As mentioned above, the serializer supports basic .Net types, and will transfer these, both with the inter-process layer and the RPC layer. If you send custom defined types, beware that you need to have a matching type on both sides. This is easiest to solve by having a shared library with all types that will be used in a remote invocation. Beware that if you change the types, the two sides may be out-of-sync, and you will get errors when transfering the types.

For items that cannot be serialized into primitives, such as a class containing an open file handle, you need to pass it by reference. While there is some support for using a class, it is often better to use an interface. With a declared interface, both sides agree on what methods are supported, and the automatic proxy creation will return an instance that transparently invokes the methods and properties. LeanIPC can recognize derived interfaces, so it is possible to use existing interfaces.

It is also possible to send instances of a specific class by reference, but it is often complicated to get a remote proxy constructed on the other end.

## Custom type handling
For simple classes, meaning classes with only primitive fields, it is possible to register these for decomposition (like with a s`struct`), but it requires a default constructor. This can be done like shown here:
```csharp
class User {
	public string FirstName;
	public string LastName;
}

var peer = new RPCPeer(stream);
peer.RegisterByValType<User>();
```

Since interfaces cannot have fields, it is not possible to decompose interface types. If the interface only comprises a set of primitive properties, LeanIPC can instead send only the property values, and construct an instance that mimics the interface with the sent properties. In the example, note that each property is copied from the source, so any logic that would be invoked, is not active in the copy:

```csharp
interface IUser {
	string FirstName { get; set; }
	string LastName { get; set; }
	string FullName { get; }
}

var peer = new RPCPeer(stream);
peer.RegisterPropertyDecomposer<IUser>();
```

There is also a `RegisterByRefType<T>()` method, which will specify that a type is always passed by reference. However, if you register this, it will not enable the remote side to invoke any methods on it. If you want the remote side to invoke methods, you should use `RegisterLocallyServedType<T>()`. If you want to limit what methods the remote side can invoke, you can pass in a filter method that can reject or allow calls:
```csharp

var peer = new RPCPeer(stream);
peer.RegisterLocallyServedType<User>((MethodInfo method, object[] arguments) => method.Name != "LastName");
```

If you have items that you simply want to suppress, you can use `RegisterIgnoreType<T>()`, which will cause them to be missing from all transmissions.

For even more control, you can also provide the custom serialization and deserialization methods, for example:
```csharp
var peer = new RPCPeer(stream);
peer.RegisterCustomSerialization<User>(
	// Serializer, takes the type and instance,
	// and returns the sub-types and instances
	(Type t, object item) => new Tuple<Type[], object[]>(
		new Type[] { typeof(string), typeof(string) },
		new object[] { ((User)item).FirstName, ((User)item).LastName }
	)

	// Deserializer gets the type and instances, 
	// and creates the local instance
	// This can also be done with by-ref, where the
	// only argument is the remote handle
	(Type t, object[] arguments) => new User() { 
		FirstName = (string)arguments[0], 
		LastName = (string)arguments[1]
	}
);
```




