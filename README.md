# Erlang Simple Queue

The library provides very simple (tiny) priority queue. 
It uses B-tree indexes to sort message.


## Usage

```erlang
   esq:start(). 
   esq:start_link(my_queue).
   
   ok = esq:enq(my_queue, <<"my message">>).
   
   [<<"my message">>] = esq:deq(my_queue).
```

## Performance

   MacBook Pro, Intel Core i5, 2.5GHz, 8GB 1600 MHz DDR3, 256 SSD

### Heap queue

   ![Heap queue performance][heap.png]


 


