# ensemblekv and extentkv
A multi-file store with many optional backends

## Motivation and Design

I wanted a key-value store that could hold 10 Tb of data, and I couldn't find anything that could come close.  The best projects seemed to top out at around 300Gb.  There were a lot of projects with great README files showing off their great design, but when I put them to the test, half didn't even work.  Of the rest, none were really functional above ~300Gb, with some failing completely and others just slowing down to the point they were unusable.

I didn't want to write my own key-value store, so instead I wrote a small program to open 100 key-value stores, and wrote the data evenly across all of them.  This worked well for a while, but even then, the best backends just chewed up so much disk space and slowed down so badly during import that I gave up and wrote my own key-value store, extentkv.

## Details

EKV creates 3 directories, and then puts an existing kv database in each one.  Options include boltdb, barreldb, etc.

EKV then uses a hash function to evenly spread data across all the individual databases.  Once the component databases reach a certain size, EKV creates 6 directories, creates databases in each new directory, and shifts the existing data into the new databases, and then deletes the old dbs.  This process continues until you have so many databases that you run out of filehandles and crash.
