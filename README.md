# Test for GORA-419

Demonstrates that other columns are dropped when a MAP field is updated.

This test uses the Gora 0.5 because I'm trying to use gora-accumulo in 
Nutch 2.3 and 0.6 is not supported. However, `AccumuloStore` is similar
between 0.5 and 0.6, so this should still occur.

Versions:
- Gora 0.5
- Accumulo 1.5.1
- Zookeeper 3.4.6
- Hadoop 1.2.1