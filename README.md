# Pacaya
A generic asynchronous tracker for Java or Android (based on RxJava)

###What?

Pacaya is a lightweight yet powerful asynchronous event tracker. Just decide what kind of events you want to send, 
define a storage & flush policy and forget about it!

It's been developped for the official DeviantArt app & is currently tracking hundreds of thousands of events every day.

###How?

```
Pacaya<MyEvent> pacaya = new Pacaya.Builder<MyEvent>()
            .localStorageService(myStorageService)
            .flushService(myFlushService)
            .build();
```

Then use that instance to log whatever `MyEvent` you want, from anywhere.

```
pacaya.log(myEvent);
```


you'll need to provide at least : 

* `localStorageService` : will handle the events waiting to get flushed. Can be a database, a local file, memory, whatever suits your needs.
* `flushService`: will send the events to their final destination. Typically, a network call, but can be basically anything.

At this point you can just leave it as it and log as many things as you want without worrying. 


// TODO : advanced - explain all the features & how you can quickly tweak Pacaya to make it super powerful and preserve your resources
