# Design of ART

## Background

The membuffer stores the temporary data of transaction, which is an in-mem index module with rich features.

- Set/Get
- Iterate
- Staging/Release/Cleanup
- Snapshot Get/Iterate
- Memory Tracking

[ART][ART_Paper] is an in-mem index data structure, which has better performance when the keys have long common perfix. In our test, it's a fitting solution for TiDB's usage.

## Design

### Index

The index implementation follows the paper, which has 4 types of node.

![index](./img/index.png)

#### Lookup

The node of tree always tracks the longest common prefix of all children nodes or leaves, in the above figure, the depth of leaf `t0001i0001z` is 4, which means we need lookup 4 times when searching it.

#### Set

The insert case is much more complex, we may meet one of the following cases:

- `set(t000, ...)` will be written an inplace leaf of the yellow node4, the node4 can still have other 4 children.
- `set(t0003, ...)` will create a new leaf node under the yellow node4, if the node4 already have 4 children, it will grow to node16.
- `set(t0001i0001z0001, ...)` will replace the green leaf with a new created node4, the green leaf and the new inserted leaf will be inplace leaf or children of the new created node4.

### Node


### Memory Arena

### Value Log

[ART_Paper]: https://db.in.tum.de/~leis/papers/ART.pdf
