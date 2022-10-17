# Loading Minimal Vendor


## Finding the process id 
```jps -l```


## Heap info
```jcmd pid GC.heap_info ```


## Resident set size
``ps -ef -o rss | grep pid ``