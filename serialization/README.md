Banyan - Serialization Code base
=====

Contains 

TDataTable - A datastructure to store a graph representation.

TDataEnum - A convinience enum used for serialization and schema representation

ArrayMap - Synchronized map which is thread safe for inserts and updates. Slightly slower than HashMap (speed not messurable) but very efficient for multi threaded apps where you dont have to synchronize the whole map.

ObjectInput/OutputUtil - Used for converting int, long, String, Double to bytes and vice versa

Test data is not checked in yet if you want test and data follow the blogs @ http://vallur.girhub.io